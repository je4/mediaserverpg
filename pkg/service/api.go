package service

import (
	"context"
	"emperror.dev/errors"
	"fmt"
	"github.com/bluele/gcache"
	"github.com/jackc/pgtype/zeronull"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/je4/mediaserverdb/v2/pkg/mediaserverdbproto"
	"github.com/je4/utils/v2/pkg/zLogger"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"sync"
	"time"
)

func NewMediaserverPG(conn *pgx.Conn, logger zLogger.ZLogger) *mediaserverPG {

	return &mediaserverPG{
		conn:            conn,
		logger:          logger,
		storageCache:    gcache.New(100).Expiration(time.Minute * 10).LRU().LoaderFunc(getStorageLoader(conn, logger)).Build(),
		collectionCache: gcache.New(200).Expiration(time.Minute * 10).LRU().LoaderFunc(getCollectionLoader(conn, logger)).Build(),
	}
}

type mediaserverPG struct {
	mediaserverdbproto.UnimplementedDBControllerServer
	logger          zLogger.ZLogger
	conn            *pgx.Conn
	storageCache    gcache.Cache
	collectionCache gcache.Cache
}

func (d *mediaserverPG) getStorage(id string) (*storage, error) {
	storageAny, err := d.storageCache.Get(id)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot get storage %s from cache", id)
	}
	s, ok := storageAny.(*storage)
	if !ok {
		return nil, errors.Errorf("cannot cast storage %v to *storage", storageAny)
	}
	return s, nil
}

func (d *mediaserverPG) GetStorage(ctx context.Context, id *mediaserverdbproto.StorageIdentifier) (*mediaserverdbproto.Storage, error) {
	s, err := d.getStorage(id.GetId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "cannot get storage %s: %v", id.GetId(), err)
	}

	return &mediaserverdbproto.Storage{
		Id:         s.Id,
		Name:       s.Name,
		Filebase:   s.Filebase,
		Datadir:    s.Datadir,
		Subitemdir: s.Subitemdir,
		Tempdir:    s.Tempdir,
	}, nil
}

func (d *mediaserverPG) getCollection(id string) (*collection, error) {
	collectionAny, err := d.collectionCache.Get(id)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot get collection %s from cache", id)
	}
	c, ok := collectionAny.(*collection)
	if !ok {
		return nil, errors.Errorf("cannot cast collection %v to *collection", collectionAny)
	}
	return c, nil
}

func (d *mediaserverPG) GetCollection(ctx context.Context, id *mediaserverdbproto.CollectionIdentifier) (*mediaserverdbproto.Collection, error) {
	c, err := d.getCollection(id.GetCollection())
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, status.Errorf(codes.NotFound, "collection %s not found", id.GetCollection())
		}
		return nil, status.Errorf(codes.Internal, "cannot get collection %s: %v", id.GetCollection(), err)
	}
	return &mediaserverdbproto.Collection{
		Id:          c.Id,
		Identifier:  &mediaserverdbproto.CollectionIdentifier{Collection: c.Name},
		Description: string(c.Description),
		Secret:      string(c.Secret),
		Public:      string(c.Public),
		Jwtkey:      string(c.Jwtkey),
		Storageid:   c.Storageid,
	}, nil
}

func (d *mediaserverPG) CreateItem(ctx context.Context, item *mediaserverdbproto.NewItem) (*mediaserverdbproto.DefaultResponse, error) {
	if item == nil {
		return nil, status.Errorf(codes.InvalidArgument, "item is nil")
	}
	if item.GetIdentifier() == nil {
		return nil, status.Errorf(codes.InvalidArgument, "item identifier is nil")
	}
	c, err := d.getCollection(item.GetIdentifier().GetCollection())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "cannot get collection %s: %v", item.GetIdentifier().GetCollection(), err)
	}
	sqlStr := "INSERT INTO item (collectionid, signature, urn, public, status, creation_date, last_modified) VALUES ($1, $2, $3, $4, 'new', now(), now())"
	params := []any{
		c.Id, item.GetIdentifier().GetSignature(), item.GetUrn(), zeronull.Text(item.GetPublic()),
	}
	tag, err := d.conn.Exec(context.Background(),
		sqlStr,
		params...)
	if err != nil {
		var pgError *pgconn.PgError
		if errors.As(err, &pgError) {
			if pgError.Code == "23505" {
				return nil, status.Errorf(codes.AlreadyExists, "item %s/%s already exists", c.Name, item.GetIdentifier().GetSignature())
			}
		}
		return nil, status.Errorf(codes.Internal, "cannot insert item %s [%v]: %v", sqlStr, params, err)
	}
	if tag.RowsAffected() != 1 {
		return nil, status.Errorf(codes.Internal, "inserted %d rows instead of 1", tag.RowsAffected())
	}
	return &mediaserverdbproto.DefaultResponse{
		Status:  mediaserverdbproto.ResultStatus_OK,
		Message: fmt.Sprintf("item %s/%s inserted", c.Name, item.GetIdentifier().GetSignature()),
		Data:    nil,
	}, nil
}

func (d *mediaserverPG) DeleteItem(ctx context.Context, id *mediaserverdbproto.ItemIdentifier) (*mediaserverdbproto.DefaultResponse, error) {
	if id == nil {
		return nil, status.Errorf(codes.InvalidArgument, "item identifier is nil")
	}
	c, err := d.getCollection(id.GetCollection())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "cannot get collection %s: %v", id.GetCollection(), err)
	}
	sqlStr := "DELETE FROM item WHERE collectionid = $1 AND signature = $2"
	params := []any{
		c.Id, id.GetSignature(),
	}
	tag, err := d.conn.Exec(context.Background(),
		sqlStr,
		params...)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "cannot delete item %s [%v]: %v", sqlStr, params, err)
	}
	if tag.RowsAffected() != 1 {
		return nil, status.Errorf(codes.Internal, "deleted %d rows instead of 1", tag.RowsAffected())
	}
	return &mediaserverdbproto.DefaultResponse{
		Status:  mediaserverdbproto.ResultStatus_OK,
		Message: fmt.Sprintf("item %s/%s deleted", c.Name, id.GetSignature()),
		Data:    nil,
	}, nil
}

func (d *mediaserverPG) ExistsItem(ctx context.Context, id *mediaserverdbproto.ItemIdentifier) (*mediaserverdbproto.DefaultResponse, error) {
	if id == nil {
		return nil, status.Errorf(codes.InvalidArgument, "item identifier is nil")
	}
	c, err := d.getCollection(id.GetCollection())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "cannot get collection %s: %v", id.GetCollection(), err)
	}
	sqlStr := "SELECT count(*) FROM item WHERE collectionid = $1 AND signature = $2"
	params := []any{
		c.Id, id.GetSignature(),
	}
	var count int
	if err := d.conn.QueryRow(context.Background(),
		sqlStr,
		params...).Scan(&count); err != nil {
		return nil, status.Errorf(codes.Internal, "cannot delete item %s [%v]: %v", sqlStr, params, err)
	}
	if count == 0 {
		return &mediaserverdbproto.DefaultResponse{
			Status:  mediaserverdbproto.ResultStatus_NotFound,
			Message: fmt.Sprintf("item %s/%s not found", c.Name, id.GetSignature()),
			Data:    nil,
		}, nil
	}
	return &mediaserverdbproto.DefaultResponse{
		Status:  mediaserverdbproto.ResultStatus_OK,
		Message: fmt.Sprintf("item %s/%s exists", c.Name, id.GetSignature()),
		Data:    nil,
	}, nil
}

func (d *mediaserverPG) Ping(context.Context, *emptypb.Empty) (*mediaserverdbproto.DefaultResponse, error) {
	return &mediaserverdbproto.DefaultResponse{
		Status:  mediaserverdbproto.ResultStatus_OK,
		Message: "pong",
		Data:    nil,
	}, nil
}

// overlapping of GetIngestItem calls must be prevented
var getIngestItemMutex = &sync.Mutex{}

func (d *mediaserverPG) GetIngestItem(context.Context, *emptypb.Empty) (*mediaserverdbproto.IngestItem, error) {
	var result = &mediaserverdbproto.IngestItem{
		Identifier: &mediaserverdbproto.ItemIdentifier{},
	}
	getIngestItemMutex.Lock()
	defer getIngestItemMutex.Unlock()
	sqlStr := "SELECT id, collectionid, signature, urn FROM item WHERE status = 'new' OR (status = 'indexing' and last_modified < now() - interval '1 hour') ORDER BY last_modified ASC LIMIT 1"
	var collectionid, itemid string
	if err := d.conn.QueryRow(context.Background(), sqlStr).Scan(&itemid, &collectionid, &result.Identifier.Signature, &result.Urn); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, status.Errorf(codes.NotFound, "no ingest item found")
		}
		return nil, status.Errorf(codes.Internal, "cannot get ingest item: %v", err)
	}
	c, err := d.getCollection(collectionid)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "cannot get collection %s: %v", collectionid, err)
	}
	result.Identifier.Collection = c.Name
	sqlStr2 := "UPDATE item SET status = 'indexing', last_modified = now() WHERE id = $1"
	if _, err := d.conn.Exec(context.Background(), sqlStr2, itemid); err != nil {
		return nil, status.Errorf(codes.Internal, "cannot update item %s to indexing: %v", itemid, err)
	}
	return result, nil
}

var _ mediaserverdbproto.DBControllerServer = (*mediaserverPG)(nil)

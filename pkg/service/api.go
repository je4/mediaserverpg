package service

import (
	"context"
	"emperror.dev/errors"
	"github.com/bluele/gcache"
	"github.com/jackc/pgx/v5"
	"github.com/je4/mediaserverdb/v2/pkg/mediaserverdbproto"
	"github.com/je4/utils/v2/pkg/zLogger"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"time"
)

func NewMediaserverPG(conn *pgx.Conn, logger zLogger.ZLogger) *mediaserverPG {

	return &mediaserverPG{
		conn:            conn,
		logger:          logger,
		storageCache:    gcache.New(100).Expiration(time.Minute * 10).LRU().LoaderFunc(GetStorageLoader(conn, logger)).Build(),
		collectionCache: gcache.New(200).Expiration(time.Minute * 10).LRU().LoaderFunc(GetCollectionLoader(conn, logger)).Build(),
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
		return nil, status.Errorf(codes.Internal, "cannot get collection %s: %v", id.GetCollection(), err)
	}
	return &mediaserverdbproto.Collection{
		Id:          c.Id,
		Identifier:  &mediaserverdbproto.CollectionIdentifier{Collection: c.Name},
		Description: string(c.Description),
		Secret:      string(c.Secret),
		Public:      c.Public,
		Jwtkey:      string(c.Jwtkey),
		Storageid:   c.Storageid,
	}, nil
}

func (d *mediaserverPG) CreateItem(ctx context.Context, item *mediaserverdbproto.NewItem) (*mediaserverdbproto.DefaultResponse, error) {
	return &mediaserverdbproto.DefaultResponse{
		Status:  mediaserverdbproto.ResultStatus_OK,
		Message: "all fine",
		Data:    nil,
	}, nil
}

func (d *mediaserverPG) DeleteItem(context.Context, *mediaserverdbproto.ItemIdentifier) (*mediaserverdbproto.DefaultResponse, error) {
	panic("implement me")
}

var _ mediaserverdbproto.DBControllerServer = (*mediaserverPG)(nil)

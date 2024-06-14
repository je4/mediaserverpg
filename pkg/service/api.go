package service

import (
	"context"
	"emperror.dev/errors"
	"fmt"
	"github.com/bluele/gcache"
	"github.com/jackc/pgtype/zeronull"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	pbgeneric "github.com/je4/genericproto/v2/pkg/generic/proto"
	pb "github.com/je4/mediaserverproto/v2/pkg/mediaserver/proto"
	"github.com/je4/utils/v2/pkg/zLogger"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
	wrapperspb "google.golang.org/protobuf/types/known/wrapperspb"
	"slices"
	"strings"
	"sync"
	"time"
)

var customTypes = []string{"item_type", "item_objecttype", "item_status"}
var preparedStatements = map[string]string{
	"getChildItemsByCollectionSignature": `
SELECT 
    i.id, i.collectionid, i.signature, i.urn, i.type, i.subtype, i.objecttype, 
    i.mimetype, i.error, i.sha512, i.creation_date, i.last_modified, i.disabled, 
    i.public, i.public_actions, i.status, i.parentid,
    COUNT(*) OVER () AS total_count
FROM item i
 		JOIN 
    collection c ON i.collectionid = c.id 
		JOIN
    	item p ON i.parentid = p.id 
WHERE c.name = $1 
  AND p.signature = $2
LIMIT $3 OFFSET $4`,
	"getChildItemsByCollectionSignatureSuffix": `
SELECT 
    c.name AS collection, i.signature AS signature
FROM item i
 		JOIN 
    collection c ON i.collectionid = c.id 
		JOIN
    	item p ON i.parentid = p.id 
WHERE c.name = $1 
  AND p.signature = $2
  AND i.signature SIMILAR TO $3`,
	"getItemByCollectionSignature":           "SELECT i.id, i.collectionid, i.signature, i.urn, i.type, i.subtype, i.objecttype, i.mimetype, i.error, i.sha512, i.creation_date, i.last_modified, i.disabled, i.public, i.public_actions, i.status, i.parentid FROM item i, collection c WHERE c.name = $1 AND i.signature = $2 AND c.id=i.collectionid",
	"getItemMetadataByCollectionSignature":   "SELECT i.metadata::text FROM item i, collection c WHERE c.name = $1 AND i.signature = $2 AND c.id=i.collectionid",
	"getItemBySignature":                     "SELECT id, collectionid, signature, urn, type, subtype, objecttype, mimetype, error, sha512, creation_date, last_modified, disabled, public, public_actions, status, parentid FROM item WHERE collectionid = $1 AND signature = $2",
	"getStorageByID":                         "SELECT id, name, filebase, datadir, subitemdir, tempdir FROM storage WHERE id = $1",
	"getStorageByName":                       "SELECT id, name, filebase, datadir, subitemdir, tempdir FROM storage WHERE name = $1",
	"getCollectionByID":                      "SELECT c.id, c.name, c.description, c.signature_prefix, c.secret, c.public, c.jwtkey, s.name AS storagename, s.filebase AS storageFilebase, s.datadir AS storageDatadir, s.subitemdir AS storageSubitemdir, s.tempdir AS storageTempdir, e.name AS estatename FROM collection c, storage s, estate e WHERE c.id = $1 AND c.storageid = s.id AND c.estateid = e.id",
	"getCollectionByName":                    "SELECT c.id, c.name, c.description, c.signature_prefix, c.secret, c.public, c.jwtkey, s.name AS storagename, s.filebase AS storagefilebase, s.datadir AS storagedatadir, s.subitemdir AS storagesubitemdir, s.tempdir AS storagetempdir, e.name AS estatename FROM collection c, storage s, estate e WHERE c.name = $1 AND c.storageid = s.id AND c.estateid = e.id",
	"getCacheByCollectionSignature":          "SELECT c.id, i.collectionid, i.id AS itemid, c.action, c.params, c.width, c.height, c.duration, c.mimetype, c.filesize, c.path, c.storageid FROM cache c, item i, collection col WHERE col.name = $1  AND i.signature = $2 AND c.action = $3 AND c.params = $4 AND i.collectionid=col.id AND c.itemid = i.id",
	"getCacheByCollectionSignatureNullParam": "SELECT c.id, i.collectionid, i.id AS itemid, c.action, c.params, c.width, c.height, c.duration, c.mimetype, c.filesize, c.path, c.storageid FROM cache c, item i, collection col WHERE col.name = $1  AND i.signature = $2 AND c.action = $3 AND c.params is null AND i.collectionid=col.id AND c.itemid = i.id",
	"getCachesByCollectionSignature": `
SELECT
    c.id, i.collectionid, i.id AS itemid, c.action, c.params, c.width, c.height, c.duration, c.mimetype, c.filesize, c.path, c.storageid,
    COUNT(*) OVER () AS total_count
FROM
    cache c
        JOIN
    item i ON c.itemid = i.id
        JOIN
    collection col ON i.collectionid = col.id
WHERE
    col.name = $1
  AND i.signature = $2
LIMIT $3 OFFSET $4`,
	"getIngestItem":          "SELECT id, collectionid, signature, urn, status FROM item WHERE status IN ('new','newcopy','newmove') OR (status IN ('indexing','indexingcopy','indexingmove') and last_modified < now() - interval '1 hour') ORDER BY last_modified ASC LIMIT 1",
	"getDerivateIngestItem1": "SELECT c.name AS collection, i.signature AS signature FROM collection c, item i LEFT JOIN item child ON child.parentid = i.id AND child.signature NOT SIMILAR TO $2 WHERE i.type = $1 AND i.collectionid = c.id AND child.id IS NULL",
	"getDerivateIngestItem2": "SELECT c.name AS collection, i.signature AS signature  FROM collection c, item i LEFT JOIN item child ON child.parentid = i.id AND child.signature NOT SIMILAR TO $3 WHERE i.type = $1 AND i.subtype = $2 AND i.collectionid = c.id AND child.id IS NULL",
}

func AfterConnectFunc(ctx context.Context, conn *pgx.Conn, logger zLogger.ZLogger) error {
	for _, typeName := range customTypes {
		t, err := conn.LoadType(ctx, typeName)
		if err != nil {
			return errors.Wrapf(err, "cannot load type '%s'", typeName)
		}
		conn.TypeMap().RegisterType(t)
	}
	for name, sql := range preparedStatements {
		if _, err := conn.Prepare(ctx, name, sql); err != nil {
			return errors.Wrapf(err, "cannot prepare statement '%s' - '%s'", name, sql)
		}
	}

	return nil
}

func NewMediaserverDatabasePG(conn *pgxpool.Pool, logger zLogger.ZLogger) (*mediaserverPG, error) {
	_logger := logger.With().Str("rpcService", "mediaserverPG").Logger()
	return &mediaserverPG{
		conn:            conn,
		logger:          &_logger,
		storageCache:    gcache.New(100).Expiration(time.Minute * 10).LRU().LoaderFunc(getStorageLoader(conn, logger)).Build(),
		collectionCache: gcache.New(200).Expiration(time.Minute * 10).LRU().LoaderFunc(getCollectionLoader(conn, logger)).Build(),
		itemCache:       gcache.New(500).Expiration(time.Minute * 10).LRU().LoaderFunc(getItemLoader(conn, logger)).Build(),
		cacheCache:      gcache.New(800).Expiration(time.Minute * 10).LRU().LoaderFunc(getCacheLoader(conn, logger)).Build(),
		derivateIngest:  gcache.New(1000).Expiration(time.Hour * 12).LRU().Build(),
	}, nil
}

type mediaserverPG struct {
	pb.UnimplementedDatabaseServer
	logger          zLogger.ZLogger
	conn            *pgxpool.Pool
	storageCache    gcache.Cache
	collectionCache gcache.Cache
	itemCache       gcache.Cache
	cacheCache      gcache.Cache
	derivateIngest  gcache.Cache
}

func (d *mediaserverPG) getItemMetadata(collection, signature string) (string, error) {
	var metadata zeronull.Text
	if err := d.conn.QueryRow(context.Background(), "getItemMetadataByCollectionSignature", collection, signature).Scan(&metadata); err != nil {
		return "", errors.Wrapf(err, "error getting metadata for %s/%s", collection, signature)
	}
	return string(metadata), nil
}

func (d *mediaserverPG) getItem(collection, signature string) (*item, error) {
	id := &ItemIdentifier{
		Collection: collection,
		Signature:  signature,
	}
	itemAny, err := d.itemCache.Get(id)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot get item %s/%s from cache", collection, signature)
	}
	i, ok := itemAny.(*item)
	if !ok {
		return nil, errors.Errorf("cannot cast type %T to *item", itemAny)
	}
	return i, nil
}

func (d *mediaserverPG) getStorage(id string) (*storage, error) {
	storageAny, err := d.storageCache.Get(id)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot get storage %s from cache", id)
	}
	s, ok := storageAny.(*storage)
	if !ok {
		return nil, errors.Errorf("cannot cast storage %T to *storage", storageAny)
	}
	return s, nil
}

func (d *mediaserverPG) DeleteCache(_ context.Context, req *pb.CacheRequest) (*pbgeneric.DefaultResponse, error) {
	itemId := req.GetIdentifier()
	cacheId := &CacheIdentifier{
		Collection: itemId.GetCollection(),
		Signature:  itemId.GetSignature(),
		Action:     req.GetAction(),
		Params:     req.GetParams(),
	}
	cacheAny, err := d.cacheCache.Get(cacheId)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, status.Errorf(codes.NotFound, "cache %s/%s/%s/%s not found", itemId.GetCollection(), itemId.GetSignature(), req.GetAction(), req.GetParams())
		}
		return nil, status.Errorf(codes.Internal, "cannot get cache %s/%s/%s/%s from cache: %v", itemId.GetCollection(), itemId.GetSignature(), req.GetAction(), req.GetParams(), err)
	}
	c, ok := cacheAny.(*cache)
	if !ok {
		return nil, errors.Errorf("cannot cast cache %T to *cache", cacheAny)
	}
	sqlStr := "DELETE FROM cache WHERE id = $1"
	params := []any{
		c.Id,
	}
	tag, err := d.conn.Exec(context.Background(),
		sqlStr,
		params...)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "cannot delete cache %s [%v]: %v", sqlStr, params, err)
	}
	if tag.RowsAffected() != 1 {
		return nil, status.Errorf(codes.Internal, "deleted %d rows instead of 1", tag.RowsAffected())
	}
	d.cacheCache.Remove(cacheId)
	return &pbgeneric.DefaultResponse{
		Status:  pbgeneric.ResultStatus_OK,
		Message: fmt.Sprintf("cache %s/%s/%s/%s deleted", itemId.GetCollection(), itemId.GetSignature(), req.GetAction(), req.GetParams()),
		Data:    nil,
	}, nil
}

func (d *mediaserverPG) GetCaches(_ context.Context, req *pb.CachesRequest) (*pb.CachesResult, error) {
	var limit int64 = 100
	var offset int64 = 0
	pr := req.GetPageRequest()
	if page := pr.GetPage(); page != nil {
		limit = page.GetPageSize()
		offset = limit * page.GetPageNo()
	}

	itemIdentifier := req.GetIdentifier()
	sqlStr := "getCachesByCollectionSignature"
	sqlParams := []any{
		itemIdentifier.GetCollection(), itemIdentifier.GetSignature(), limit, offset,
	}
	rows, err := d.conn.Query(context.Background(), sqlStr, sqlParams...)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "cannot get caches for %s/%s: %v", itemIdentifier.GetCollection(), itemIdentifier.GetSignature(), err)
	}
	defer rows.Close()
	res := &pb.CachesResult{
		Caches: []*pb.Cache{},
	}
	var path zeronull.Text
	var params zeronull.Text
	var storageid zeronull.Text
	var width zeronull.Int8
	var height zeronull.Int8
	var duration zeronull.Int8
	var totalCount int64
	var c = &cache{}
	for rows.Next() {
		if err := rows.Scan(
			&c.Id,
			&c.CollectionId,
			&c.ItemId,
			&c.Action,
			&params,
			&width,
			&height,
			&duration,
			&c.Mimetype,
			&c.Filesize,
			&path,
			&storageid,
			&totalCount,
		); err != nil {
			return nil, errors.Wrapf(err, "cannot get caches %s/%s - %s", itemIdentifier.GetCollection(), itemIdentifier.GetSignature(), "getCachesByCollectionSignature")
		}
		c.Params = string(params)
		c.Width = int(width)
		c.Height = int(height)
		c.Duration = int(duration)
		c.Path = string(path)
		c.StorageId = string(storageid)
		sendCache := &pb.Cache{
			Identifier: &pb.ItemIdentifier{
				Collection: itemIdentifier.GetCollection(),
				Signature:  itemIdentifier.GetSignature(),
			},
			Metadata: &pb.CacheMetadata{
				Action:   c.Action,
				Params:   c.Params,
				Width:    int64(c.Width),
				Height:   int64(c.Height),
				Duration: int64(c.Duration),
				Size:     int64(c.Filesize),
				MimeType: c.Mimetype,
				Path:     c.Path,
			},
		}
		if c.StorageId != "" {
			storAny, err := d.storageCache.Get(c.StorageId)
			if err != nil {
				return nil, errors.Wrapf(err, "cannot get storage %s", c.StorageId)
			}
			stor, ok := storAny.(*storage)
			if !ok {
				return nil, errors.Errorf("cannot cast storage %T to *storage", storAny)
			}
			sendCache.Metadata.Storage = &pb.Storage{
				Name:       stor.Name,
				Filebase:   stor.Filebase,
				Datadir:    stor.Datadir,
				Subitemdir: stor.Subitemdir,
				Tempdir:    stor.Tempdir,
			}
		}
		d.cacheCache.Set(
			&CacheIdentifier{
				Collection: itemIdentifier.GetCollection(),
				Signature:  itemIdentifier.GetSignature(),
				Action:     c.Action,
				Params:     c.Params,
			},
			c)

		res.Caches = append(res.Caches, sendCache)
	}
	if totalCount > offset+limit {
		mod := totalCount % limit
		pages := (totalCount - mod) / limit
		if mod > 0 {
			pages++
		}
		res.PageResponse = &pbgeneric.PageResponse{
			PageResponse: &pbgeneric.PageResponse_PageResult{
				PageResult: &pbgeneric.PageResult{
					Total:    pages,
					PageNo:   offset / limit,
					PageSize: limit,
				},
			},
		}
	}
	return res, nil
}

func (d *mediaserverPG) GetCache(_ context.Context, req *pb.CacheRequest) (*pb.Cache, error) {
	itemId := req.GetIdentifier()
	cacheAny, err := d.cacheCache.Get(&CacheIdentifier{
		Collection: itemId.GetCollection(),
		Signature:  itemId.GetSignature(),
		Action:     req.GetAction(),
		Params:     req.GetParams(),
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, status.Errorf(codes.NotFound, "cache %s/%s/%s/%s not found", itemId.GetCollection(), itemId.GetSignature(), req.GetAction(), req.GetParams())
		}
		return nil, status.Errorf(codes.Internal, "cannot get cache %s/%s/%s/%s from cache: %v", itemId.GetCollection(), itemId.GetSignature(), req.GetAction(), req.GetParams(), err)
	}
	c, ok := cacheAny.(*cache)
	if !ok {
		return nil, errors.Errorf("cannot cast cache %T to *cache", cacheAny)
	}
	storageName := ""
	if c.StorageId != "" {
		stor, err := d.getStorage(c.StorageId)
		if err != nil {
			return nil, errors.Wrapf(err, "cannot get storage %s", c.StorageId)
		}
		storageName = stor.Name
	}
	res := &pb.Cache{
		Identifier: &pb.ItemIdentifier{
			Collection: itemId.GetCollection(),
			Signature:  itemId.GetSignature(),
		},
		Metadata: &pb.CacheMetadata{
			Action:   c.Action,
			Params:   c.Params,
			Width:    int64(c.Width),
			Height:   int64(c.Height),
			Duration: int64(c.Duration),
			Size:     int64(c.Filesize),
			MimeType: c.Mimetype,
			Path:     c.Path,
		},
	}
	if storageName != "" {
		storAny, err := d.storageCache.Get(storageName)
		if err != nil {
			return nil, errors.Wrapf(err, "cannot get storage %s", storageName)
		}
		stor, ok := storAny.(*storage)
		if !ok {
			return nil, errors.Errorf("cannot cast storage %T to *storage", storAny)
		}
		res.Metadata.Storage = &pb.Storage{
			Name:       stor.Name,
			Filebase:   stor.Filebase,
			Datadir:    stor.Datadir,
			Subitemdir: stor.Subitemdir,
			Tempdir:    stor.Tempdir,
		}
	}
	return res, nil

}

func (d *mediaserverPG) GetStorage(_ context.Context, id *pb.StorageIdentifier) (*pb.Storage, error) {
	s, err := d.getStorage(id.GetName())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "cannot get storage %s: %v", id.GetName(), err)
	}

	return &pb.Storage{
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

func (d *mediaserverPG) GetCollection(_ context.Context, id *pb.CollectionIdentifier) (*pb.Collection, error) {
	c, err := d.getCollection(id.GetCollection())
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, status.Errorf(codes.NotFound, "collection %s not found", id.GetCollection())
		}
		return nil, status.Errorf(codes.Internal, "cannot get collection %s: %v", id.GetCollection(), err)
	}
	s := &pb.Storage{
		Name:       c.Storage.Name,
		Filebase:   c.Storage.Filebase,
		Datadir:    c.Storage.Datadir,
		Subitemdir: c.Storage.Subitemdir,
		Tempdir:    c.Storage.Tempdir,
	}
	return &pb.Collection{
		Name:        c.Name,
		Description: string(c.Description),
		Secret:      string(c.Secret),
		Public:      string(c.Public),
		Jwtkey:      string(c.Jwtkey),
		Storage:     s,
	}, nil
}

func (d *mediaserverPG) GetCollections(_ *emptypb.Empty, result pb.Database_GetCollectionsServer) error {
	collections, err := getCollections(d.conn, d.logger)
	if err != nil {
		return status.Errorf(codes.Internal, "cannot get collections: %v", err)
	}
	for _, c := range collections {
		s := &pb.Storage{
			Name:       c.Storage.Name,
			Filebase:   c.Storage.Filebase,
			Datadir:    c.Storage.Datadir,
			Subitemdir: c.Storage.Subitemdir,
			Tempdir:    c.Storage.Tempdir,
		}
		result.Send(&pb.Collection{
			Name:        c.Name,
			Description: string(c.Description),
			Secret:      string(c.Secret),
			Public:      string(c.Public),
			Jwtkey:      string(c.Jwtkey),
			Storage:     s,
		})
	}
	return nil
}

func (d *mediaserverPG) CreateItem(_ context.Context, item *pb.NewItem) (*pbgeneric.DefaultResponse, error) {
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
	sqlStr := "INSERT INTO item (collectionid, signature, urn, public, status, creation_date, last_modified) VALUES ($1, $2, $3, $4, $5, now(), now())"
	var newstatus string
	switch item.GetIngestType() {
	case pb.IngestType_KEEP:
		newstatus = "new"
	case pb.IngestType_COPY:
		newstatus = "newcopy"
	case pb.IngestType_MOVE:
		newstatus = "newmove"
	default:
		return nil, status.Errorf(codes.InvalidArgument, "invalid ingest type %s", item.GetIngestType())
	}
	params := []any{
		c.Id, item.GetIdentifier().GetSignature(), item.GetUrn(), item.GetPublic(), newstatus,
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
	return &pbgeneric.DefaultResponse{
		Status:  pbgeneric.ResultStatus_OK,
		Message: fmt.Sprintf("item %s/%s inserted", c.Name, item.GetIdentifier().GetSignature()),
		Data:    nil,
	}, nil
}

func (d *mediaserverPG) DeleteItem(_ context.Context, id *pb.ItemIdentifier) (*pbgeneric.DefaultResponse, error) {
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
	return &pbgeneric.DefaultResponse{
		Status:  pbgeneric.ResultStatus_OK,
		Message: fmt.Sprintf("item %s/%s deleted", c.Name, id.GetSignature()),
		Data:    nil,
	}, nil
}

func (d *mediaserverPG) GetItemMetadata(_ context.Context, id *pb.ItemIdentifier) (*wrapperspb.StringValue, error) {
	if id == nil {
		return nil, status.Errorf(codes.InvalidArgument, "item identifier is nil")
	}
	metadata, err := d.getItemMetadata(id.GetCollection(), id.GetSignature())
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, status.Errorf(codes.NotFound, "item %s/%s not found", id.GetCollection(), id.GetSignature())
		}
		return nil, status.Errorf(codes.Internal, "cannot get metadata for %s/%s: %v", id.GetCollection(), id.GetSignature(), err)
	}
	return &wrapperspb.StringValue{
		Value: metadata,
	}, nil
}

func (d *mediaserverPG) GetItem(_ context.Context, id *pb.ItemIdentifier) (*pb.Item, error) {
	if id == nil {
		return nil, status.Errorf(codes.InvalidArgument, "item identifier is nil")
	}

	_it, err := d.getItem(id.GetCollection(), id.GetSignature())
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, status.Errorf(codes.NotFound, "item %s/%s not found", id.GetCollection(), id.GetSignature())
		}
		return nil, status.Errorf(codes.Internal, "cannot get item %s/%s: %v", id.GetCollection(), id.GetSignature(), err)
	}

	var it = &pb.Item{
		Identifier: &pb.ItemIdentifier{
			Collection: id.GetCollection(),
			Signature:  id.GetSignature(),
		},
		Metadata: &pb.ItemMetadata{
			Type:       &_it.Type,
			Subtype:    &_it.Subtype,
			Mimetype:   &_it.Mimetype,
			Objecttype: &_it.Objecttype,
			Sha512:     &_it.Sha512,
		},
		Status:        _it.Status,
		Urn:           _it.Urn,
		PublicActions: _it.PublicActions,
		Disabled:      _it.Disabled,
		Public:        _it.Public,
	}
	if _it.Error != "" {
		it.Error = &_it.Error
	}
	if !_it.CreationDate.IsZero() {
		it.Created = timestamppb.New(_it.CreationDate)
	}
	if !_it.LastModified.IsZero() {
		it.Updated = timestamppb.New(_it.LastModified)
	}
	if _it.PartentId != "" {
		sqlStr := `SELECT collectionid, signature FROM item WHERE id = $1`
		if err := d.conn.QueryRow(context.Background(), sqlStr, _it.PartentId).Scan(&it.Parent.Collection, &it.Parent.Signature); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil, status.Errorf(codes.NotFound, "parent item %s not found", _it.PartentId)
			}
			return nil, status.Errorf(codes.Internal, "cannot get parent item %s [%s]: %v", sqlStr, _it.PartentId, err)
		}
	}
	return it, nil
}

func (d *mediaserverPG) GetChildItems(_ context.Context, req *pb.ItemsRequest) (*pb.ItemsResult, error) {
	var limit int64 = 100
	var offset int64 = 0
	pr := req.GetPageRequest()
	if page := pr.GetPage(); page != nil {
		limit = page.GetPageSize()
		offset = limit * page.GetPageNo()
	}

	itemIdentifier := req.GetIdentifier()
	sqlStr := "getChildItemsByCollectionSignature"
	sqlParams := []any{
		itemIdentifier.GetCollection(), itemIdentifier.GetSignature(), limit, offset,
	}
	rows, err := d.conn.Query(context.Background(), sqlStr, sqlParams...)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "cannot get caches for %s/%s: %v", itemIdentifier.GetCollection(), itemIdentifier.GetSignature(), err)
	}
	defer rows.Close()
	res := &pb.ItemsResult{
		Items: []*pb.Item{},
	}
	var totalCount int64
	var parentCache = map[string]*pb.ItemIdentifier{}
	for rows.Next() {
		var _it = item{}
		var parentID zeronull.Text
		var _type zeronull.Text
		var subtype zeronull.Text
		var objecttype zeronull.Text
		var mimetype zeronull.Text
		var errorStr zeronull.Text
		var sha512 zeronull.Text
		if err := rows.Scan(
			&_it.Id,
			&_it.Collectionid,
			&_it.Signature,
			&_it.Urn,
			&_type,
			&subtype,
			&objecttype,
			&mimetype,
			&errorStr,
			&sha512,
			&_it.CreationDate,
			&_it.LastModified,
			&_it.Disabled,
			&_it.Public,
			&_it.PublicActions,
			&_it.Status,
			&parentID,
			&totalCount,
		); err != nil {
			return nil, errors.Wrapf(err, "cannot get children of item %s/%s - %s", itemIdentifier.GetCollection(), itemIdentifier.GetSignature(), "getChildItemsByCollectionSignature")
		}
		_it.PartentId = string(parentID)
		_it.Type = string(_type)
		_it.Subtype = string(subtype)
		_it.Objecttype = string(objecttype)
		_it.Mimetype = string(mimetype)
		_it.Error = string(errorStr)
		_it.Sha512 = string(sha512)
		var it = &pb.Item{
			Identifier: &pb.ItemIdentifier{
				Collection: itemIdentifier.GetCollection(),
				Signature:  itemIdentifier.GetSignature(),
			},
			Metadata: &pb.ItemMetadata{
				Type:       &_it.Type,
				Subtype:    &_it.Subtype,
				Mimetype:   &_it.Mimetype,
				Objecttype: &_it.Objecttype,
				Sha512:     &_it.Sha512,
			},
			Status:        _it.Status,
			PublicActions: _it.PublicActions,
			Disabled:      _it.Disabled,
			Public:        _it.Public,
		}
		if _it.Error != "" {
			it.Error = &_it.Error
		}
		if !_it.CreationDate.IsZero() {
			it.Created = timestamppb.New(_it.CreationDate)
		}
		if !_it.LastModified.IsZero() {
			it.Updated = timestamppb.New(_it.LastModified)
		}
		if _it.PartentId != "" {
			pIdent, ok := parentCache[_it.PartentId]
			if ok {
				it.Parent = pIdent
			} else {
				it.Parent = &pb.ItemIdentifier{}
				sqlStr := `SELECT collectionid, signature FROM item WHERE id = $1`
				if err := d.conn.QueryRow(context.Background(), sqlStr, _it.PartentId).Scan(&it.Parent.Collection, &it.Parent.Signature); err != nil {
					if errors.Is(err, pgx.ErrNoRows) {
						return nil, status.Errorf(codes.NotFound, "parent item %s not found", _it.PartentId)
					}
					return nil, status.Errorf(codes.Internal, "cannot get parent item %s [%s]: %v", sqlStr, _it.PartentId, err)
				}
				parentCache[_it.PartentId] = it.Parent
			}
		}
		res.Items = append(res.Items, it)
	}
	if totalCount > offset+limit {
		mod := totalCount % limit
		pages := (totalCount - mod) / limit
		if mod > 0 {
			pages++
		}
		res.PageResponse = &pbgeneric.PageResponse{
			PageResponse: &pbgeneric.PageResponse_PageResult{
				PageResult: &pbgeneric.PageResult{
					Total:    pages,
					PageNo:   offset / limit,
					PageSize: limit,
				},
			},
		}
	}
	return res, nil
}

func (d *mediaserverPG) ExistsItem(_ context.Context, id *pb.ItemIdentifier) (*pbgeneric.DefaultResponse, error) {
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
		return &pbgeneric.DefaultResponse{
			Status:  pbgeneric.ResultStatus_NotFound,
			Message: fmt.Sprintf("item %s/%s not found", c.Name, id.GetSignature()),
			Data:    nil,
		}, nil
	}
	return &pbgeneric.DefaultResponse{
		Status:  pbgeneric.ResultStatus_OK,
		Message: fmt.Sprintf("item %s/%s exists", c.Name, id.GetSignature()),
		Data:    nil,
	}, nil
}

func (d *mediaserverPG) Ping(context.Context, *emptypb.Empty) (*pbgeneric.DefaultResponse, error) {
	return &pbgeneric.DefaultResponse{
		Status:  pbgeneric.ResultStatus_OK,
		Message: "pong",
		Data:    nil,
	}, nil
}

var getDerivateIngestItemMutex = map[string]*sync.Mutex{}

func (d *mediaserverPG) GetDerivateIngestItem(_ context.Context, req *pb.DerivatIngestRequest) (*pb.DerivatIngestResponse, error) {
	id := strings.Join(req.GetSuffix(), "/")

	// todo: correct locking including map operations
	mutex, ok := getDerivateIngestItemMutex[id]
	if !ok {
		mutex = &sync.Mutex{}
		getDerivateIngestItemMutex[id] = mutex
	}
	mutex.Lock()
	defer mutex.Unlock()
	params := []any{req.GetType()}
	sqlStr := "getDerivateIngestItem1"
	if req.GetSubtype() != "" {
		sqlStr = "getDerivateIngestItem2"
		params = append(params, req.GetSubtype())
	}
	suffixesParam := fmt.Sprintf("%%(%s)", strings.Join(req.GetSuffix(), "|"))
	params = append(params, suffixesParam)

	rows, err := d.conn.Query(context.Background(), sqlStr, params...)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "cannot get derivate ingest item %s [%v]: %v", sqlStr, params, err)
	}
	defer rows.Close()
	for rows.Next() {
		var col, signature string
		if err := rows.Scan(&col, &signature); err != nil {
			return nil, status.Errorf(codes.Internal, "cannot scan derivate ingest item %s [%v]: %v", sqlStr, params, err)
		}
		it, err := d.getItem(col, signature)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "cannot get item %s/%s: %v", col, signature, err)
		}
		findSuffixes := func() ([]string, error) {
			var suffixesFound = []string{}
			sqlStr2 := "getChildItemsByCollectionSignatureSuffix"
			params2 := []any{col, signature, fmt.Sprintf("%%(%s)", strings.Join(req.GetSuffix(), "|"))}
			rows2, err := d.conn.Query(context.Background(), sqlStr2, params2...)
			if err != nil {
				return nil, status.Errorf(codes.Internal, "cannot get children for %s/%s: %v", col, signature, err)
			}
			defer rows2.Close()
			for rows2.Next() {
				var col2, signature2 string
				if err := rows2.Scan(&col2, &signature2); err != nil {
					return nil, status.Errorf(codes.Internal, "cannot scan child item for %s/%s: %v", col2, signature2, err)
				}
				for _, suffix := range req.GetSuffix() {
					if strings.HasSuffix(signature2, suffix) {
						suffixesFound = append(suffixesFound, suffix)
					}
				}
			}
			return suffixesFound, nil
		}
		suffixes, err := findSuffixes()
		if err != nil {
			return nil, err
		}
		if len(suffixes) == len(req.GetSuffix()) {
			continue
		}
		id2 := fmt.Sprintf("%s/%s(%s)", col, signature, strings.Join(req.GetSuffix(), "|"))
		if d.derivateIngest.Has(id2) {
			continue
		}
		d.derivateIngest.Set(id2, true)
		var missingSuffixes = []string{}
		for _, suffix := range req.GetSuffix() {
			if !slices.Contains(suffixes, suffix) {
				missingSuffixes = append(missingSuffixes, suffix)
			}
		}
		return &pb.DerivatIngestResponse{
			Item: &pb.Item{
				Identifier: &pb.ItemIdentifier{
					Collection: col,
					Signature:  signature,
				},
				Metadata: &pb.ItemMetadata{
					Type:       &it.Type,
					Subtype:    &it.Subtype,
					Mimetype:   &it.Mimetype,
					Objecttype: &it.Objecttype,
					Sha512:     &it.Sha512,
				},
				Status:        it.Status,
				Urn:           it.Urn,
				PublicActions: it.PublicActions,
				Disabled:      it.Disabled,
				Public:        it.Public,
			},
			Missing: missingSuffixes,
		}, nil
	}
	return nil, status.Errorf(codes.NotFound, "no ingest item found")
}

// overlapping of GetIngestItem calls must be prevented
var getIngestItemMutex = &sync.Mutex{}

func (d *mediaserverPG) GetIngestItem(context.Context, *emptypb.Empty) (*pb.IngestItem, error) {
	var result = &pb.IngestItem{
		Identifier: &pb.ItemIdentifier{},
	}
	getIngestItemMutex.Lock()
	defer getIngestItemMutex.Unlock()
	sqlStr := "getIngestItem"
	var collectionid, itemid, statusStr string
	if err := d.conn.QueryRow(context.Background(), sqlStr).Scan(&itemid, &collectionid, &result.Identifier.Signature, &result.Urn, &statusStr); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, status.Errorf(codes.NotFound, "no ingest item found")
		}
		d.logger.Error().Err(err).Msgf("cannot get ingest item - %s", sqlStr)
		return nil, status.Errorf(codes.Internal, "cannot get ingest item: %v", err)
	}
	c, err := d.getCollection(collectionid)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "cannot get collection %s: %v", collectionid, err)
	}
	result.Identifier.Collection = c.Name
	result.Collection = &pb.Collection{
		Name:        c.Name,
		Description: string(c.Description),
		Secret:      string(c.Secret),
		Public:      string(c.Public),
		Jwtkey:      string(c.Jwtkey),
		Storage: &pb.Storage{
			Name:       c.Storage.Name,
			Filebase:   c.Storage.Filebase,
			Datadir:    c.Storage.Datadir,
			Subitemdir: c.Storage.Subitemdir,
			Tempdir:    c.Storage.Tempdir,
		},
	}
	result.IngestType = pb.IngestType_KEEP
	var newstatus string
	switch statusStr {
	case "newcopy":
		result.IngestType = pb.IngestType_COPY
		newstatus = "indexingcopy"
	case "newmove":
		result.IngestType = pb.IngestType_MOVE
		newstatus = "indexingmove"
	case "new":
		result.IngestType = pb.IngestType_KEEP
		newstatus = "indexing"
	case "indexingcopy":
		result.IngestType = pb.IngestType_COPY
		newstatus = "indexingcopy"
	case "indexingmove":
		result.IngestType = pb.IngestType_MOVE
		newstatus = "indexingmove"
	case "indexing":
		result.IngestType = pb.IngestType_KEEP
		newstatus = "indexing"
	default:
		return nil, status.Errorf(codes.Internal, "invalid status %s", statusStr)
	}

	sqlStr2 := "UPDATE item SET status = $1, last_modified = now() WHERE id = $2"
	if _, err := d.conn.Exec(context.Background(), sqlStr2, newstatus, itemid); err != nil {
		return nil, status.Errorf(codes.Internal, "cannot update item %s to indexing: %v", itemid, err)
	}
	return result, nil
}

func (d *mediaserverPG) SetIngestItem(ctx context.Context, metadata *pb.IngestMetadata) (*pbgeneric.DefaultResponse, error) {
	if metadata == nil {
		return nil, status.Errorf(codes.InvalidArgument, "metadata is nil")
	}
	metaItemMetadata := metadata.GetItemMetadata()
	if metaItemMetadata == nil {
		return nil, status.Errorf(codes.InvalidArgument, "item metadata is nil")
	}
	metaCacheMetadata := metadata.GetCacheMetadata()
	if metaCacheMetadata == nil {
		return nil, status.Errorf(codes.InvalidArgument, "cache metadata is nil")
	}
	coll, err := d.getCollection(metadata.GetItem().GetCollection())
	if err != nil {
		d.logger.Error().Err(err).Msg("cannot get collection")
		return nil, status.Errorf(codes.Internal, "cannot get collection: %v", err)
	}
	tx, err := d.conn.Begin(ctx)
	if err != nil {
		d.logger.Error().Err(err).Msg("cannot start transaction")
		return nil, status.Errorf(codes.Internal, "cannot start transaction: %v", err)
	}
	defer tx.Rollback(ctx)
	sqlStr := "UPDATE item SET type = $1, subtype = $2, objecttype = $3, mimetype = $4, error = $5, sha512 = $6, metadata = $7, last_modified = now(), status = $8 WHERE collectionid = $9 AND signature = $10"
	params := []any{
		zeronull.Text(metaItemMetadata.GetType()),
		zeronull.Text(metaItemMetadata.GetSubtype()),
		metaItemMetadata.GetObjecttype(),
		zeronull.Text(metaItemMetadata.GetMimetype()),
		zeronull.Text(metadata.GetError()),
		zeronull.Text(metaItemMetadata.GetSha512()),
		zeronull.Text(metadata.GetFullMetadata()),
		metadata.GetStatus(),
		coll.Id,
		metadata.GetItem().GetSignature(),
	}
	if _, err := tx.Exec(ctx, sqlStr, params...); err != nil {
		d.logger.Error().Err(err).Msgf("cannot update item - '%s' - %v", sqlStr, params)
		return nil, status.Errorf(codes.Internal, "cannot update item - '%s' - %v: %v", sqlStr, params, err)
	}
	if metadata.GetStatus() == "ok" {
		it, err := d.getItem(metadata.GetItem().GetCollection(), metadata.GetItem().GetSignature())
		if err != nil {
			d.logger.Error().Err(err).Msgf("cannot get item %s/%s", metadata.GetItem().GetCollection(), metadata.GetItem().GetSignature())
			return nil, status.Errorf(codes.Internal, "cannot get item %s/%s: %v", metadata.GetItem().GetCollection(), metadata.GetItem().GetSignature(), err)
		}
		var storageID zeronull.Text
		if metaCacheMetadata.GetStorage() != nil {
			stor := metaCacheMetadata.GetStorage()
			storInt, err := d.getStorage(stor.GetName())
			if err != nil {
				d.logger.Error().Err(err).Msgf("cannot get storage %s", stor.GetName())
				return nil, status.Errorf(codes.Internal, "cannot get storage %s: %v", stor.GetName(), err)
			}
			storageID = zeronull.Text(storInt.Id)
		}
		sqlStr = "INSERT INTO cache (collectionid, itemid, action, width, height, duration, mimetype, filesize, path, storageid) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)"
		params = []any{
			coll.Id,
			it.Id,
			"item",
			zeronull.Int4(metaCacheMetadata.GetWidth()),
			zeronull.Int4(metaCacheMetadata.GetHeight()),
			zeronull.Int4(metaCacheMetadata.GetDuration()),
			zeronull.Text(metaCacheMetadata.GetMimeType()),
			metaCacheMetadata.GetSize(),
			zeronull.Text(metaCacheMetadata.GetPath()),
			storageID,
		}
		if _, err := tx.Exec(ctx, sqlStr, params...); err != nil {
			d.logger.Error().Err(err).Msgf("cannot insert cache - '%s' - %v", sqlStr, params)
			return nil, status.Errorf(codes.Internal, "cannot insert cache - '%s' - %v: %v", sqlStr, params, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		d.logger.Error().Err(err).Msg("cannot commit transaction")
		return nil, status.Errorf(codes.Internal, "cannot commit transaction: %v", err)
	}
	return &pbgeneric.DefaultResponse{
		Status:  pbgeneric.ResultStatus_OK,
		Message: "item updated",
		Data:    nil,
	}, nil
}

func (d *mediaserverPG) InsertCache(ctx context.Context, cache *pb.Cache) (*pbgeneric.DefaultResponse, error) {
	if cache == nil {
		return nil, status.Errorf(codes.InvalidArgument, "cache is nil")
	}
	identifier := cache.GetIdentifier()
	coll, err := d.getCollection(identifier.GetCollection())
	if err != nil {
		d.logger.Error().Err(err).Msgf("cannot get collection %s", identifier.GetCollection())
		return nil, status.Errorf(codes.Internal, "cannot get collection %s: %v", identifier.GetCollection(), err)
	}
	it, err := d.getItem(identifier.GetCollection(), identifier.GetSignature())
	if err != nil {
		d.logger.Error().Err(err).Msgf("cannot get item %s/%s", identifier.GetCollection(), identifier.GetSignature())
		return nil, status.Errorf(codes.Internal, "cannot get item %s/%s: %v", identifier.GetCollection(), identifier.GetSignature(), err)
	}
	cacheMetadata := cache.GetMetadata()
	var storageid zeronull.Text
	stor := cacheMetadata.GetStorage()
	if stor != nil {
		st, err := d.getStorage(stor.GetName())
		if err != nil {
			d.logger.Error().Err(err).Msgf("cannot get storage %s", stor.GetName())
			return nil, status.Errorf(codes.Internal, "cannot get storage %s: %v", stor.GetName(), err)
		}
		storageid = zeronull.Text(st.Id)
	}
	var action zeronull.Text = zeronull.Text(cacheMetadata.GetAction())
	var paramStr zeronull.Text = zeronull.Text(cacheMetadata.GetParams())
	var width zeronull.Int4 = zeronull.Int4(cacheMetadata.GetWidth())
	var height zeronull.Int4 = zeronull.Int4(cacheMetadata.GetHeight())
	var duration zeronull.Int4 = zeronull.Int4(cacheMetadata.GetDuration())
	var mimetype zeronull.Text = zeronull.Text(cacheMetadata.GetMimeType())
	var path zeronull.Text = zeronull.Text(cacheMetadata.GetPath())
	sqlStr := "INSERT INTO cache (collectionid, itemid, action, params, width, height, duration, mimetype, filesize, path, storageid) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)"
	params := []any{
		coll.Id,
		it.Id,
		action,
		paramStr,
		width,
		height,
		duration,
		mimetype,
		cacheMetadata.GetSize(), // not null
		path,
		storageid,
	}
	tag, err := d.conn.Exec(ctx, sqlStr, params...)
	if err != nil {
		d.logger.Error().Err(err).Msgf("cannot insert cache - '%s' - %v", sqlStr, params)
		return nil, status.Errorf(codes.Internal, "cannot insert cache - '%s' - %v: %v", sqlStr, params, err)
	}
	if tag.RowsAffected() != 1 {
		d.logger.Error().Msgf("inserted %d rows instead of 1", tag.RowsAffected())
		return nil, status.Errorf(codes.Internal, "inserted %d rows instead of 1", tag.RowsAffected())
	}
	return &pbgeneric.DefaultResponse{
		Status:  pbgeneric.ResultStatus_OK,
		Message: "cache inserted",
		Data:    nil,
	}, nil

}

var _ pb.DatabaseServer = (*mediaserverPG)(nil)

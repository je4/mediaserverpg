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
	pb "github.com/je4/mediaserverproto/v2/pkg/mediaserverdb/proto"
	"github.com/je4/utils/v2/pkg/zLogger"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"sync"
	"time"
)

var customTypes = []string{"item_type", "item_objecttype", "item_status"}
var preparedStatements = map[string]string{
	"getItemByCollectionSignature":  "SELECT i.id, i.collectionid, i.signature, i.urn, i.type, i.subtype, i.objecttype, i.mimetype, i.error, i.sha512, i.metadata, i.creation_date, i.last_modified, i.disabled, i.public, i.public_actions, i.status, i.parentid FROM item i, collection c WHERE c.name = $1 AND i.signature = $2 AND c.id=i.collectionid",
	"getItemBySignature":            "SELECT id, collectionid, signature, urn, type, subtype, objecttype, mimetype, error, sha512, metadata, creation_date, last_modified, disabled, public, public_actions, status, parentid FROM item WHERE collectionid = $1 AND signature = $2",
	"getStorageByID":                "SELECT id, name, filebase, datadir, subitemdir, tempdir FROM storage WHERE id = $1",
	"getStorageByName":              "SELECT id, name, filebase, datadir, subitemdir, tempdir FROM storage WHERE name = $1",
	"getCollectionByID":             "SELECT c.id, c.name, c.description, c.signature_prefix, c.secret, c.public, c.jwtkey, s.name AS storagename, s.filebase AS storageFilebase, s.datadir AS storageDatadir, s.subitemdir AS storageSubitemdir, s.tempdir AS storageTempdir, e.name AS estatename FROM collection c, storage s, estate e WHERE c.id = $1 AND c.storageid = s.id AND c.estateid = e.id",
	"getCollectionByName":           "SELECT c.id, c.name, c.description, c.signature_prefix, c.secret, c.public, c.jwtkey, s.name AS storagename, s.filebase AS storagefilebase, s.datadir AS storagedatadir, s.subitemdir AS storagesubitemdir, s.tempdir AS storagetempdir, e.name AS estatename FROM collection c, storage s, estate e WHERE c.name = $1 AND c.storageid = s.id AND c.estateid = e.id",
	"getCacheByCollectionSignature": "SELECT c.id, i.collectionid, i.id AS itemid, c.action, c.params, c.width, c.height, c.duration, c.mimetype, c.filesize, c.path, c.storageid FROM cache c, item i, collection col WHERE col.name = $1  AND i.signature = $2 AND c.action = $3 AND c.params = $4 AND i.collectionid=col.id AND c.itemid = i.id",
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

func NewMediaserverPG(conn *pgxpool.Pool, logger zLogger.ZLogger) (*mediaserverPG, error) {
	return &mediaserverPG{
		conn:            conn,
		logger:          logger,
		storageCache:    gcache.New(100).Expiration(time.Minute * 10).LRU().LoaderFunc(getStorageLoader(conn, logger)).Build(),
		collectionCache: gcache.New(200).Expiration(time.Minute * 10).LRU().LoaderFunc(getCollectionLoader(conn, logger)).Build(),
		itemCache:       gcache.New(500).Expiration(time.Minute * 10).LRU().LoaderFunc(getItemLoader(conn, logger)).Build(),
		cacheCache:      gcache.New(800).Expiration(time.Minute * 10).LRU().LoaderFunc(getCacheLoader(conn, logger)).Build(),
	}, nil
}

type mediaserverPG struct {
	pb.UnimplementedDBControllerServer
	logger          zLogger.ZLogger
	conn            *pgxpool.Pool
	storageCache    gcache.Cache
	collectionCache gcache.Cache
	itemCache       gcache.Cache
	cacheCache      gcache.Cache
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

func (d *mediaserverPG) GetCache(ctx context.Context, req *pb.CacheRequest) (*pb.Cache, error) {
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
		res.Metadata.StorageName = &storageName
	}
	return res, nil

}

func (d *mediaserverPG) GetStorage(ctx context.Context, id *pb.StorageIdentifier) (*pb.Storage, error) {
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

func (d *mediaserverPG) GetCollection(ctx context.Context, id *pb.CollectionIdentifier) (*pb.Collection, error) {
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

func (d *mediaserverPG) GetCollections(empty *emptypb.Empty, result pb.DBController_GetCollectionsServer) error {
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

func (d *mediaserverPG) CreateItem(ctx context.Context, item *pb.NewItem) (*pbgeneric.DefaultResponse, error) {
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

func (d *mediaserverPG) DeleteItem(ctx context.Context, id *pb.ItemIdentifier) (*pbgeneric.DefaultResponse, error) {
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

func (d *mediaserverPG) GetItem(ctx context.Context, id *pb.ItemIdentifier) (*pb.Item, error) {
	if id == nil {
		return nil, status.Errorf(codes.InvalidArgument, "item identifier is nil")
	}
	c, err := d.getCollection(id.GetCollection())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "cannot get collection %s: %v", id.GetCollection(), err)
	}
	var _type zeronull.Text
	var subtype zeronull.Text
	var mimetype zeronull.Text
	var errorstr zeronull.Text
	var sha512 zeronull.Text
	var metadata zeronull.Text
	var creation_date zeronull.Timestamp
	var last_modified zeronull.Timestamp
	var disabled bool
	var public bool
	var public_actions zeronull.Text
	var statusStr string
	var parentid zeronull.UUID
	sqlStr := `SELECT 
       urn,
       type,
       subtype,
       objecttype,
       mimetype,
       error,
       sha512,
       metadata,
       creation_date,
       last_modified,
       disabled,
       public,
       public_actions,
       status,
       parentid
FROM item 
WHERE collectionid = $1 AND signature = $2`
	params := []any{
		c.Id, id.GetSignature(),
	}
	var it = &pb.Item{
		Identifier: &pb.ItemIdentifier{
			Collection: id.GetCollection(),
			Signature:  id.GetSignature(),
		},
		Metadata: &pb.ItemMetadata{},
	}
	if err := d.conn.QueryRow(context.Background(),
		sqlStr,
		params...).Scan(
		&it.Urn,
		&_type,
		&subtype,
		&it.Metadata.Objecttype,
		&mimetype,
		&errorstr,
		&sha512,
		&metadata,
		&creation_date,
		&last_modified,
		&disabled,
		&public,
		&public_actions,
		&statusStr,
		&parentid,
	); err != nil {
		return nil, status.Errorf(codes.Internal, "cannot get item %s [%v]: %v", sqlStr, params, err)
	}
	if _type != "" {
		it.Metadata.Type = (*string)(&_type)
	}
	if subtype != "" {
		it.Metadata.Subtype = (*string)(&subtype)
	}
	if mimetype != "" {
		it.Metadata.Mimetype = (*string)(&mimetype)
	}
	if errorstr != "" {
		it.Metadata.Error = (*string)(&errorstr)
	}
	if sha512 != "" {
		it.Metadata.Sha512 = (*string)(&sha512)
	}
	if metadata != "" {
		it.Metadata.Metadata = ([]byte)(metadata)
	}
	if !time.Time(creation_date).IsZero() {
		it.Created = timestamppb.New(time.Time(creation_date))
	}
	if !time.Time(last_modified).IsZero() {
		it.Updated = timestamppb.New(time.Time(last_modified))
	}
	it.Disabled = disabled
	it.Public = public
	if public_actions != "" {
		it.PublicActions = ([]byte)(public_actions)
	}
	it.Status = statusStr
	if parentid[0] != 0 {
		sqlStr = `SELECT collectionid, signature FROM item WHERE id = $1`
		params = []any{
			parentid,
		}
		if err := d.conn.QueryRow(context.Background(), sqlStr, params...).Scan(&it.Parent.Collection, &it.Parent.Signature); err != nil {
			return nil, status.Errorf(codes.Internal, "cannot get parent item %s [%v]: %v", sqlStr, params, err)
		}
	}
	return it, nil
}

func (d *mediaserverPG) ExistsItem(ctx context.Context, id *pb.ItemIdentifier) (*pbgeneric.DefaultResponse, error) {
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

// overlapping of GetIngestItem calls must be prevented
var getIngestItemMutex = &sync.Mutex{}

func (d *mediaserverPG) GetIngestItem(context.Context, *emptypb.Empty) (*pb.IngestItem, error) {
	var result = &pb.IngestItem{
		Identifier: &pb.ItemIdentifier{},
	}
	getIngestItemMutex.Lock()
	defer getIngestItemMutex.Unlock()
	sqlStr := "SELECT id, collectionid, signature, urn, status FROM item WHERE status IN ('new','newcopy','newmove') OR (status IN ('indexing','indexingcopy','indexingmove') and last_modified < now() - interval '1 hour') ORDER BY last_modified ASC LIMIT 1"
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
		zeronull.Text(metaItemMetadata.GetError()),
		zeronull.Text(metaItemMetadata.GetSha512()),
		zeronull.Text(metaItemMetadata.GetMetadata()),
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
		if metaCacheMetadata.GetStorageName() != "" {
			stor, err := d.getStorage(metaCacheMetadata.GetStorageName())
			if err != nil {
				d.logger.Error().Err(err).Msgf("cannot get storage %s", coll.Storage.Name)
				return nil, status.Errorf(codes.Internal, "cannot get storage %s: %v", coll.Storage.Name, err)
			}
			storageID = zeronull.Text(stor.Id)
		}
		sqlStr = "INSERT INTO cache (collectionid, itemid, action, width, height, duration, mimetype, filesize, path, storageid) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)"
		params = []any{
			coll.Id,
			it.Id,
			"item",
			metaCacheMetadata.GetWidth(),
			metaCacheMetadata.GetHeight(),
			metaCacheMetadata.GetDuration(),
			metaCacheMetadata.GetMimeType(),
			metaCacheMetadata.GetSize(),
			metaCacheMetadata.GetPath(),
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
	st, err := d.getStorage(cacheMetadata.GetStorageName())
	if err != nil {
		d.logger.Error().Err(err).Msgf("cannot get storage %s", cacheMetadata.GetStorageName())
		return nil, status.Errorf(codes.Internal, "cannot get storage %s: %v", cacheMetadata.GetStorageName(), err)
	}
	sqlStr := "INSERT INTO cache (collectionid, itemid, action, params, width, height, duration, mimetype, filesize, path, storageid) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)"
	params := []any{
		coll.Id,
		it.Id,
		cacheMetadata.GetAction(),
		cacheMetadata.GetParams(),
		cacheMetadata.GetWidth(),
		cacheMetadata.GetHeight(),
		cacheMetadata.GetDuration(),
		cacheMetadata.GetMimeType(),
		cacheMetadata.GetSize(),
		cacheMetadata.GetPath(),
		st.Id,
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

var _ pb.DBControllerServer = (*mediaserverPG)(nil)

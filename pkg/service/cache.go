package service

import (
	"context"
	"emperror.dev/errors"
	"github.com/bluele/gcache"
	"github.com/jackc/pgx/v5/pgtype/zeronull"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/je4/utils/v2/pkg/zLogger"
)

type cache struct {
	Id           string
	CollectionId string
	ItemId       string
	Action       string
	Params       string
	Width        int
	Height       int
	Duration     int
	Mimetype     string
	Filesize     int
	Path         string
	StorageId    string
}

type CacheIdentifier struct {
	Collection string
	Signature  string
	Action     string
	Params     string
}

func getCacheLoader(conn *pgxpool.Pool, logger zLogger.ZLogger) gcache.LoaderFunc {
	return func(key interface{}) (interface{}, error) {
		id, ok := key.(*CacheIdentifier)
		if !ok {
			return nil, errors.Errorf("key %T of wrong type", key)
		}
		c := &cache{
			Action: id.Action,
			Params: id.Params,
		}
		var path zeronull.Text
		var storageid zeronull.Text
		var width zeronull.Int8
		var height zeronull.Int8
		var duration zeronull.Int8
		if err := conn.QueryRow(context.Background(), "getCacheByCollectionSignature", id.Collection, id.Signature, id.Action, id.Params).Scan(
			&c.Id,
			&c.CollectionId,
			&c.ItemId,
			&c.Action,
			&c.Params,
			&width,
			&height,
			&duration,
			&c.Mimetype,
			&c.Filesize,
			&path,
			&storageid,
		); err != nil {
			return nil, errors.Wrapf(err, "cannot get cache %s/%s/%s/%s - %s", id.Collection, id.Signature, id.Action, id.Params, "getCacheByCollectionSignature")
		}
		c.Width = int(width)
		c.Height = int(height)
		c.Duration = int(duration)
		c.Path = string(path)
		c.StorageId = string(storageid)

		return c, nil

	}
}

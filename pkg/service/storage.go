package service

import (
	"context"
	"emperror.dev/errors"
	"github.com/bluele/gcache"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/je4/utils/v2/pkg/zLogger"
)

type storage struct {
	Id         string `json:"id,omitempty"`
	Name       string `json:"name,omitempty"`
	Filebase   string `json:"filebase,omitempty"`
	Datadir    string `json:"datadir,omitempty"`
	Subitemdir string `json:"subitemdir,omitempty"`
	Tempdir    string `json:"tempdir,omitempty"`
}

func getStorageLoader(conn *pgxpool.Pool, logger zLogger.ZLogger) gcache.LoaderFunc {
	return func(key interface{}) (interface{}, error) {
		id, ok := key.(string)
		if !ok {
			return nil, errors.Errorf("key %v is not a string", key)
		}
		var sql string
		if IsValidUUID(id) {
			sql = "getStorageByID"
			logger.Debug().Msgf("%s, [%s]", sql, id)
		} else {
			sql = "getStorageByName"
			logger.Debug().Msgf("%s, [%s]", sql, id)
		}
		s := &storage{}
		if err := conn.QueryRow(
			context.Background(),
			sql,
			id,
		).Scan(&s.Id, &s.Name, &s.Filebase, &s.Datadir, &s.Subitemdir, &s.Tempdir); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil, errors.Wrapf(errors.Combine(err, gcache.KeyNotFoundError), "storage %s not found", id)
			}
			return nil, errors.Wrapf(err, "cannot get storage %s from database", id)
		}
		return s, nil
	}
}

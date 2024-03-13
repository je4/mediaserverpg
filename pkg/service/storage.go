package service

import (
	"context"
	"emperror.dev/errors"
	"github.com/bluele/gcache"
	"github.com/jackc/pgx/v5"
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

func GetStorageLoader(conn *pgx.Conn, logger zLogger.ZLogger) gcache.LoaderFunc {
	getStorageSQL := "SELECT id, name, filebase, datadir, subitemdir, tempdir FROM storage WHERE id = $1"
	if _, err := conn.Prepare(context.Background(), "getStorage", getStorageSQL); err != nil {
		logger.Panic().Err(err).Msg("cannot prepare statement")
	}
	return func(key interface{}) (interface{}, error) {
		id, ok := key.(string)
		if !ok {
			return nil, errors.Errorf("key %v is not a string", key)
		}
		logger.Debug().Msgf("%s, [%s]", getStorageSQL, id)
		s := &storage{}
		if err := conn.QueryRow(
			context.Background(),
			"getStorage",
			id,
		).Scan(&s.Id, &s.Name, &s.Filebase, &s.Datadir, &s.Subitemdir, &s.Tempdir); err != nil {
			return nil, errors.Wrapf(err, "cannot get storage %s from database", id)
		}
		return s, nil
	}
}

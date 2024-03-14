package service

import (
	"context"
	"emperror.dev/errors"
	"github.com/bluele/gcache"
	"github.com/jackc/pgtype/zeronull"
	"github.com/jackc/pgx/v5"
	"github.com/je4/utils/v2/pkg/zLogger"
)

type collection struct {
	Id              string        `json:"id,omitempty"`
	Name            string        `json:"name,omitempty"`
	Description     zeronull.Text `json:"description,omitempty"`
	SignaturePrefix zeronull.Text `json:"signature_prefix,omitempty"`
	Secret          zeronull.Text `json:"secret,omitempty"`
	Public          zeronull.Text `json:"public,omitempty"`
	Jwtkey          zeronull.Text `json:"jwtkey,omitempty"`
	Estateid        string        `json:"estateid,omitempty"`
	Storageid       string        `json:"storageid,omitempty"`
}

func GetCollectionLoader(conn *pgx.Conn, logger zLogger.ZLogger) gcache.LoaderFunc {
	getCollectionByIDSQL := "SELECT id, name, description, signature_prefix, secret, public, jwtkey, storageid, estateid FROM collection WHERE id = $1"
	if _, err := conn.Prepare(context.Background(), "getCollectionByID", getCollectionByIDSQL); err != nil {
		logger.Panic().Err(err).Msg("cannot prepare statement")
	}
	getCollectionByNameSQL := "SELECT id, name, description, signature_prefix, secret, public, jwtkey, storageid, estateid FROM collection WHERE name = $1"
	if _, err := conn.Prepare(context.Background(), "getCollectionByName", getCollectionByNameSQL); err != nil {
		logger.Panic().Err(err).Msg("cannot prepare statement")
	}
	return func(key interface{}) (interface{}, error) {
		id, ok := key.(string)
		if !ok {
			return nil, errors.Errorf("key %v is not a string", key)
		}
		coll := &collection{}
		var sql string
		if IsValidUUID(id) {
			sql = "getCollectionByID"
			logger.Debug().Msgf("%s, [%s]", getCollectionByIDSQL, id)
		} else {
			sql = "getCollectionByName"
			logger.Debug().Msgf("%s, [%s]", getCollectionByNameSQL, id)
		}
		if err := conn.QueryRow(
			context.Background(),
			sql,
			id,
		).Scan(&coll.Id, &coll.Name, &coll.Description, &coll.SignaturePrefix, &coll.Secret, &coll.Public, &coll.Jwtkey, &coll.Storageid, &coll.Estateid); err != nil {
			return nil, errors.Wrapf(err, "cannot get collection %s from database", id)
		}
		return coll, nil
	}
}

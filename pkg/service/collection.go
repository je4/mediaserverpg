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
	SignaturePrefix zeronull.Text `json:"signaturePrefix,omitempty"`
	Secret          zeronull.Text `json:"secret,omitempty"`
	Public          zeronull.Text `json:"public,omitempty"`
	Jwtkey          zeronull.Text `json:"jwtkey,omitempty"`
	EstateName      string        `json:"estateName,omitempty"`
	Storage         *storage      `json:"storageName,omitempty"`
}

func getCollections(conn *pgx.Conn, logger zLogger.ZLogger) ([]*collection, error) {
	getCollectionsSQL := "SELECT c.id, c.name, c.description, c.signature_prefix, c.secret, c.public, c.jwtkey, s.name AS storagename, s.filebase AS storageFilebase, s.datadir AS storageDatadir, s.subitemdir AS storageSubitemdir, s.tempdir AS storageTempdir, e.name AS estatename FROM collection c, storage s, estate e WHERE c.storageid = s.id AND c.estateid = e.id"
	rows, err := conn.Query(context.Background(), getCollectionsSQL)
	if err != nil {
		return nil, errors.Wrap(err, "cannot get collections from database")
	}
	defer rows.Close()
	var colls []*collection
	for rows.Next() {
		coll := &collection{}
		stor := &storage{}
		if err := rows.Scan(&coll.Id, &coll.Name, &coll.Description, &coll.SignaturePrefix, &coll.Secret, &coll.Public, &coll.Jwtkey, &stor.Name, &stor.Filebase, &stor.Datadir, &stor.Subitemdir, &stor.Tempdir, &coll.EstateName); err != nil {
			return nil, errors.Wrap(err, "cannot scan collection")
		}
		coll.Storage = stor
		colls = append(colls, coll)
	}
	return colls, nil
}

func getCollectionLoader(conn *pgx.Conn, logger zLogger.ZLogger) gcache.LoaderFunc {
	getCollectionByIDSQL := "SELECT c.id, c.name, c.description, c.signature_prefix, c.secret, c.public, c.jwtkey, s.name AS storagename, s.filebase AS storageFilebase, s.datadir AS storageDatadir, s.subitemdir AS storageSubitemdir, s.tempdir AS storageTempdir, e.name AS estatename FROM collection c, storage s, estate e WHERE c.id = $1 AND c.storageid = s.id AND c.estateid = e.id"
	if _, err := conn.Prepare(context.Background(), "getCollectionByID", getCollectionByIDSQL); err != nil {
		logger.Panic().Err(err).Msg("cannot prepare statement")
	}
	getCollectionByNameSQL := "SELECT c.id, c.name, c.description, c.signature_prefix, c.secret, c.public, c.jwtkey, s.name AS storagename , e.name AS estatename FROM collection c, storage s, estate e WHERE c.name = $1 AND c.storageid = s.id AND c.estateid = e.id"
	if _, err := conn.Prepare(context.Background(), "getCollectionByName", getCollectionByNameSQL); err != nil {
		logger.Panic().Err(err).Msg("cannot prepare statement")
	}
	return func(key interface{}) (interface{}, error) {
		id, ok := key.(string)
		if !ok {
			return nil, errors.Errorf("key %v is not a string", key)
		}
		coll := &collection{}
		stor := &storage{}
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
		).Scan(&coll.Id, &coll.Name, &coll.Description, &coll.SignaturePrefix, &coll.Secret, &coll.Public, &coll.Jwtkey, &stor.Name, &stor.Filebase, &stor.Datadir, &stor.Subitemdir, &stor.Tempdir, &coll.EstateName); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil, errors.Wrapf(errors.Combine(err, gcache.KeyNotFoundError), "collection %s not found", id)
			}
			return nil, errors.Wrapf(err, "cannot get collection %s from database", id)
		}
		coll.Storage = stor
		return coll, nil
	}
}

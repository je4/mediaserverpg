package service

import (
	"context"
	"emperror.dev/errors"
	"github.com/bluele/gcache"
	"github.com/jackc/pgx/v5/pgtype/zeronull"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/je4/utils/v2/pkg/zLogger"
	"time"
)

type item struct {
	Id            string
	Signature     string
	Collectionid  string
	Urn           string
	Type          string
	Subtype       string
	Mimetype      string
	Error         string
	Sha512        string
	CreationDate  time.Time
	LastModified  time.Time
	Disabled      bool
	Public        bool
	PublicActions string
	Status        string
	PartentId     string
	Objecttype    string
}

type ItemIdentifier struct {
	Collection string
	Signature  string
}

func getItemLoader(conn *pgxpool.Pool, logger zLogger.ZLogger) gcache.LoaderFunc {
	return func(key interface{}) (interface{}, error) {
		id, ok := key.(*ItemIdentifier)
		if !ok {
			return nil, errors.Errorf("key %T of wrong type", key)
		}
		i := &item{}
		var parentID zeronull.Text
		var _type zeronull.Text
		var subtype zeronull.Text
		var objecttype zeronull.Text
		var mimetype zeronull.Text
		var errorStr zeronull.Text
		var sha512 zeronull.Text
		var publicActions zeronull.Text
		if err := conn.QueryRow(context.Background(), "getItemByCollectionSignature", id.Collection, id.Signature).Scan(
			&i.Id,
			&i.Collectionid,
			&i.Signature,
			&i.Urn,
			&_type,
			&subtype,
			&objecttype,
			&mimetype,
			&errorStr,
			&sha512,
			&i.CreationDate,
			&i.LastModified,
			&i.Disabled,
			&i.Public,
			&publicActions,
			&i.Status,
			&parentID,
		); err != nil {
			return nil, errors.Wrapf(err, "cannot get item %s/%s - %s", id.Collection, id.Signature, "getItemByCollectionSignature")
		}
		i.PartentId = string(parentID)
		i.Type = string(_type)
		i.Subtype = string(subtype)
		i.Objecttype = string(objecttype)
		i.Mimetype = string(mimetype)
		i.Error = string(errorStr)
		i.Sha512 = string(sha512)
		i.PublicActions = string(publicActions)
		return i, nil

	}
}

package service

import (
	"context"
	"github.com/jackc/pgx/v5"
	"github.com/je4/mediaserverdb/v2/pkg/mediaserverdbproto"
	"github.com/je4/utils/v2/pkg/zLogger"
)

func NewMediaserverPG(conn *pgx.Conn, logger zLogger.ZLogger) *mediaserverPG {
	conn.Prepare(
		context.Background(),
		"createItem",
		"INSERT INTO items (id, name, description, type, data) VALUES ($1, $2, $3, $4, $5)",
	)

	return &mediaserverPG{
		conn:   conn,
		logger: logger,
	}
}

type mediaserverPG struct {
	mediaserverdbproto.UnimplementedDBControllerServer
	logger zLogger.ZLogger
	conn   *pgx.Conn
}

func (d *mediaserverPG) GetStorage(context.Context, *mediaserverdbproto.StorageIdentifier) (*mediaserverdbproto.Storage, error) {
	panic("implement me")
}
func (d *mediaserverPG) GetCollection(context.Context, *mediaserverdbproto.CollectionIdentifier) (*mediaserverdbproto.Collection, error) {
	panic("implement me")
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

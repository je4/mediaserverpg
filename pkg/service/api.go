package service

import (
	"context"
	"github.com/jackc/pgx/v5"
	"github.com/je4/mediaserverdb/v2/pkg/mediaserverdbproto"
	"github.com/je4/utils/v2/pkg/zLogger"
)

func NewMediaserverPG(conn *pgx.Conn, logger zLogger.ZLogger) *mediaserverPG {
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

func (d *mediaserverPG) CreateItem(ctx context.Context, item *mediaserverdbproto.NewItem) (*mediaserverdbproto.DefaultResponse, error) {
	return &mediaserverdbproto.DefaultResponse{
		Status:  mediaserverdbproto.ResultStatus_OK,
		Message: "all fine",
		Data:    nil,
	}, nil
}

var _ mediaserverdbproto.DBControllerServer = (*mediaserverPG)(nil)

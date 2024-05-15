package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/je4/mediaserverpg/v2/configs"
	"github.com/je4/mediaserverpg/v2/pkg/service"
	pb "github.com/je4/mediaserverproto/v2/pkg/mediaserverdb/proto"
	resolverclient "github.com/je4/miniresolver/v2/pkg/client"
	"github.com/je4/miniresolver/v2/pkg/grpchelper"
	"github.com/je4/trustutil/v2/pkg/certutil"
	"github.com/je4/trustutil/v2/pkg/loader"
	"github.com/je4/utils/v2/pkg/zLogger"
	"github.com/rs/zerolog"
	"io"
	"io/fs"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"
)

var cfg = flag.String("config", "", "location of toml configuration file")

func main() {
	flag.Parse()
	var cfgFS fs.FS
	var cfgFile string
	if *cfg != "" {
		cfgFS = os.DirFS(filepath.Dir(*cfg))
		cfgFile = filepath.Base(*cfg)
	} else {
		cfgFS = configs.ConfigFS
		cfgFile = "mediaserverpg.toml"
	}
	conf := &MediaserverPGConfig{
		LocalAddr: "localhost:8443",
		LogLevel:  "DEBUG",
	}
	if err := LoadMediaserverPGConfig(cfgFS, cfgFile, conf); err != nil {
		log.Fatalf("cannot load toml from [%v] %s: %v", cfgFS, cfgFile, err)
	}
	// create logger instance
	var out io.Writer = os.Stdout
	if conf.LogFile != "" {
		fp, err := os.OpenFile(conf.LogFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			log.Fatalf("cannot open logfile %s: %v", conf.LogFile, err)
		}
		defer fp.Close()
		out = fp
	}

	output := zerolog.ConsoleWriter{Out: out, TimeFormat: time.RFC3339}
	_logger := zerolog.New(output).With().Timestamp().Logger()
	_logger.Level(zLogger.LogLevel(conf.LogLevel))
	var logger zLogger.ZLogger = &_logger
	//	var dbLogger = zerologadapter.NewLogger(_logger)

	//logger.Info().Msgf("connecting to database: %s", conf.DBConn)
	pgxConf, err := pgxpool.ParseConfig(string(conf.DBConn))
	if err != nil {
		logger.Fatal().Err(err).Msg("cannot parse db connection string")
	}
	//	pgxConf.TLSConfig = &tls.Config{InsecureSkipVerify: true, ServerName: "dd-pdb3.ub.unibas.ch"}
	// create prepared queries on each connection
	pgxConf.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		return service.AfterConnectFunc(ctx, conn, logger)
	}
	var conn *pgxpool.Pool
	connCount := 0
	for {
		connCount++
		logger.Info().Msgf("connecting to database: %s (try %d)", conf.DBConn, connCount)
		conn, err = pgxpool.NewWithConfig(context.Background(), pgxConf)
		//conn, err = pgx.ConnectConfig(context.Background(), pgxConf)
		if err != nil {
			logger.Error().Err(err).Msgf("cannot connect to database: %s", conf.DBConn)
		}
		if err == nil {
			break
		}
		if connCount > 12 {
			logger.Fatal().Err(err).Msgf("cannot connect to database: %s", conf.DBConn)
		}
		logger.Info().Msgf("retrying in 5 seconds")
		time.Sleep(5 * time.Second)
	}
	defer conn.Close()

	srv, err := service.NewMediaserverPG(conn, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("cannot create service")
	}

	// create TLS Certificate.
	// the certificate MUST contain <package>.<service> as DNS name
	certutil.AddDefaultDNSNames(grpchelper.GetService(pb.DBController_Ping_FullMethodName))
	serverTLSConfig, serverLoader, err := loader.CreateServerLoader(true, &conf.ServerTLS, nil, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("cannot create server loader")
	}
	defer serverLoader.Close()

	// create client TLS certificate
	// the certificate MUST contain "grpc:miniresolverproto.MiniResolver" or "*" in URIs
	clientTLSConfig, clientLoader, err := loader.CreateClientLoader(&conf.ClientTLS, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("cannot create client loader")
	}
	defer clientLoader.Close()

	// create resolver client
	resolver, resolverCloser, err := resolverclient.CreateClient(conf.ResolverAddr, clientTLSConfig)
	if err != nil {
		logger.Fatal().Err(err).Msg("cannot create resolver client")
	}
	defer resolverCloser.Close()
	grpchelper.RegisterResolver(resolver, time.Duration(conf.ResolverTimeout), time.Duration(conf.ResolverNotFoundTimeout), logger)

	// create grpc server with resolver for name resolution
	grpcServer, err := grpchelper.NewServer(conf.LocalAddr, serverTLSConfig, resolver, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("cannot create server")
	}
	// register the server
	pb.RegisterDBControllerServer(grpcServer, srv)

	grpcServer.Startup()

	done := make(chan os.Signal, 1)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
	fmt.Println("press ctrl+c to stop server")
	s := <-done
	fmt.Println("got signal:", s)

	defer grpcServer.Shutdown()

}

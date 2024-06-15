package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/je4/mediaserverpg/v2/configs"
	"github.com/je4/mediaserverpg/v2/pkg/service"
	mediaserverproto "github.com/je4/mediaserverproto/v2/pkg/mediaserver/proto"
	"github.com/je4/miniresolver/v2/pkg/resolver"
	"github.com/je4/trustutil/v2/pkg/certutil"
	"github.com/je4/trustutil/v2/pkg/loader"
	"github.com/je4/utils/v2/pkg/zLogger"
	"github.com/rs/zerolog"
	ublogger "gitlab.switch.ch/ub-unibas/go-ublogger"
	"io"
	"io/fs"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"
)

var cfg = flag.String("config", "", "location of toml configuration file")

type queryTracer struct {
	log zLogger.ZLogger
}

func (tracer *queryTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	tracer.log.Debug().Msgf("postgreSQL command start '%s' - %v", data.SQL, data.Args)
	return ctx
}

func (tracer *queryTracer) TraceQueryEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryEndData) {
	if data.Err != nil {
		tracer.log.Error().Err(data.Err).Msgf("postgreSQL command error")
		return
	}
	tracer.log.Debug().Msgf("postgreSQL command end: %s (%d)", data.CommandTag.String(), data.CommandTag.RowsAffected())
}

type testlogger struct {
	*zerolog.Logger
}

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
	}
	if err := LoadMediaserverPGConfig(cfgFS, cfgFile, conf); err != nil {
		log.Fatalf("cannot load toml from [%v] %s: %v", cfgFS, cfgFile, err)
	}
	// create logger instance
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("cannot get hostname: %v", err)
	}

	var loggerTLSConfig *tls.Config
	var loggerLoader io.Closer
	if conf.Log.Stash.TLS != nil {
		loggerTLSConfig, loggerLoader, err = loader.CreateClientLoader(conf.Log.Stash.TLS, nil)
		if err != nil {
			log.Fatalf("cannot create client loader: %v", err)
		}
		defer loggerLoader.Close()
	}

	_logger, _logstash, _logfile := ublogger.CreateUbMultiLoggerTLS(conf.Log.Level, conf.Log.File,
		ublogger.SetDataset(conf.Log.Stash.Dataset),
		ublogger.SetLogStash(conf.Log.Stash.LogstashHost, conf.Log.Stash.LogstashPort, conf.Log.Stash.Namespace, conf.Log.Stash.LogstashTraceLevel),
		ublogger.SetTLS(conf.Log.Stash.TLS != nil),
		ublogger.SetTLSConfig(loggerTLSConfig),
	)
	if _logstash != nil {
		defer _logstash.Close()
	}
	if _logfile != nil {
		defer _logfile.Close()
	}

	l2 := _logger.With().Timestamp().Str("host", hostname).Logger() //.Output(output)
	var logger zLogger.ZLogger = &l2

	pgxConf, err := pgxpool.ParseConfig(string(conf.DBConn))
	if err != nil {
		logger.Fatal().Err(err).Msg("cannot parse db connection string")
	}
	//	pgxConf.TLSConfig = &tls.Config{InsecureSkipVerify: true, ServerName: "dd-pdb3.ub.unibas.ch"}
	// create prepared queries on each connection
	pgxConf.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		return service.AfterConnectFunc(ctx, conn, logger)
	}
	pgxConf.BeforeConnect = func(ctx context.Context, cfg *pgx.ConnConfig) error {
		cfg.Tracer = &queryTracer{log: logger}
		return nil
	}
	var conn *pgxpool.Pool
	var dbstrRegexp = regexp.MustCompile(`^postgres://postgres:([^@]+)@.+$`)
	pws := dbstrRegexp.FindStringSubmatch(string(conf.DBConn))
	if len(pws) == 2 {
		logger.Info().Msgf("connecting to database: %s", strings.Replace(string(conf.DBConn), pws[1], "xxxxxxxx", -1))
	} else {
		logger.Info().Msgf("connecting to database")
	}
	conn, err = pgxpool.NewWithConfig(context.Background(), pgxConf)
	//conn, err = pgx.ConnectConfig(context.Background(), pgxConf)
	if err != nil {
		logger.Fatal().Err(err).Msgf("cannot connect to database: %s", conf.DBConn)
	}
	defer conn.Close()

	// create TLS Certificate.
	// the certificate MUST contain <package>.<service> as DNS name
	for _, domain := range conf.ServerDomains {
		var domainPrefix string
		if domain != "" {
			domainPrefix = domain + "."
		}
		certutil.AddDefaultDNSNames(domainPrefix + mediaserverproto.Database_ServiceDesc.ServiceName)
	}

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
	resolverClient, err := resolver.NewMiniresolverClient(conf.ResolverAddr, conf.GRPCClient, clientTLSConfig, serverTLSConfig, time.Duration(conf.ResolverTimeout), time.Duration(conf.ResolverNotFoundTimeout), logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("cannot create resolver client")
	}
	defer resolverClient.Close()

	// create grpc server with resolver for name resolution
	grpcServer, err := resolverClient.NewServer(conf.LocalAddr, conf.ServerDomains)
	if err != nil {
		logger.Fatal().Err(err).Msg("cannot create server")
	}
	addr := grpcServer.GetAddr()
	l2 = _logger.With().Timestamp().Str("addr", addr).Logger() //.Output(output)
	logger = &l2

	srv, err := service.NewMediaserverDatabasePG(conn, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("cannot create service")
	}
	// register the server
	mediaserverproto.RegisterDatabaseServer(grpcServer, srv)

	grpcServer.Startup()

	done := make(chan os.Signal, 1)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
	fmt.Println("press ctrl+c to stop server")
	s := <-done
	fmt.Println("got signal:", s)

	defer grpcServer.Shutdown()

}

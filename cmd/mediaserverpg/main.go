package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/jackc/pgx/v5"
	pb "github.com/je4/mediaserverdb/v2/pkg/mediaserverdbproto"
	"github.com/je4/mediaserverpg/v2/configs"
	"github.com/je4/mediaserverpg/v2/pkg/service"
	"github.com/je4/trustutil/v2/pkg/grpchelper"
	"github.com/je4/utils/v2/pkg/zLogger"
	_ "github.com/lib/pq"
	"github.com/rs/zerolog"
	"io"
	"io/fs"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
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
		LocalAddr:    "localhost:8443",
		ExternalAddr: "https://localhost:8443",
		LogLevel:     "DEBUG",
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

	//	output := zerolog.ConsoleWriter{Out: out, TimeFormat: time.RFC3339}
	_logger := zerolog.New(out).With().Timestamp().Logger()
	_logger.Level(zLogger.LogLevel(conf.LogLevel))
	var logger zLogger.ZLogger = &_logger

	pgxConf, err := pgx.ParseConfig(string(conf.DBConn))
	if err != nil {
		logger.Fatal().Err(err).Msg("cannot parse db connection string")
	}
	//	pgxConf.TLSConfig = &tls.Config{InsecureSkipVerify: true, ServerName: "dd-pdb3.ub.unibas.ch"}
	conn, err := pgx.ConnectConfig(context.Background(), pgxConf)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close(context.Background())

	srv := service.NewMediaserverPG(conn, logger)
	grpcServer, err := grpchelper.NewServer(conf.LocalAddr, nil, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("cannot create server")
	}
	pb.RegisterDBControllerServer(grpcServer, srv)

	grpcServer.Startup()

	done := make(chan os.Signal, 1)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
	fmt.Println("press ctrl+c to stop server")
	s := <-done
	fmt.Println("got signal:", s)

	defer grpcServer.Shutdown()

}

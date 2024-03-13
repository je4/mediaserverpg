package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"github.com/jackc/pgx/v5"
	pb "github.com/je4/mediaserverdb/v2/pkg/mediaserverdbproto"
	"github.com/je4/mediaserverpg/v2/configs"
	"github.com/je4/mediaserverpg/v2/pkg/service"
	"github.com/je4/trustutil/v2/pkg/grpchelper"
	"github.com/je4/trustutil/v2/pkg/tlsutil"
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

	//logger.Info().Msgf("connecting to database: %s", conf.DBConn)
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

	certChannel := make(chan *tls.Certificate)
	defer close(certChannel)

	var tlsConfig *tls.Config
	switch conf.TLS.Type {
	case "ENV":
		certPEM := os.Getenv(conf.TLS.Cert)
		keyPEM := os.Getenv(conf.TLS.Key)
		caPEM := os.Getenv(conf.TLS.CA)
		cert, err := tls.X509KeyPair([]byte(certPEM), []byte(keyPEM))
		if err != nil {
			logger.Fatal().Err(err).Msg("cannot create x509 key pair")
		}
		tlsConfig, err = tlsutil.CreateServerTLSConfig(cert, true, nil, [][]byte{[]byte(caPEM)})
		if err != nil {
			logger.Fatal().Err(err).Msg("cannot create server tls config")
		}
		tlsutil.UpgradeTLSConfigServerExchanger(tlsConfig, certChannel)
		go func() {
			for {
				time.Sleep(time.Minute * 5)
				certPEM0 := os.Getenv(conf.TLS.Cert)
				keyPEM0 := os.Getenv(conf.TLS.Key)
				caPEM0 := os.Getenv(conf.TLS.CA)
				if certPEM != certPEM0 || keyPEM != keyPEM0 || caPEM != caPEM0 {
					cert0, err := tls.X509KeyPair([]byte(certPEM), []byte(keyPEM))
					if err != nil {
						logger.Error().Err(err).Msg("cannot create x509 key pair")
					}
					certChannel <- &cert0
				}
				//todo: end this loop
			}
		}()
	case "FILE":
		certPEM, err := os.ReadFile(conf.TLS.Cert)
		if err != nil {
			logger.Fatal().Err(err).Msgf("cannot read certificate file %s", conf.TLS.Cert)
		}
		keyPEM, err := os.ReadFile(conf.TLS.Key)
		if err != nil {
			logger.Fatal().Err(err).Msgf("cannot read key file %s", conf.TLS.Key)
		}
		caPEM, err := os.ReadFile(conf.TLS.CA)
		if err != nil {
			logger.Fatal().Err(err).Msgf("cannot read ca file %s", conf.TLS.CA)
		}
		cert, err := tls.X509KeyPair(certPEM, keyPEM)
		if err != nil {
			logger.Fatal().Err(err).Msg("cannot create x509 key pair")
		}
		tlsConfig, err = tlsutil.CreateServerTLSConfig(cert, true, nil, [][]byte{caPEM})
		if err != nil {
			logger.Fatal().Err(err).Msg("cannot create server tls config")
		}
		tlsutil.UpgradeTLSConfigServerExchanger(tlsConfig, certChannel)
	case "SERVICE":
		panic("tls location from service not implemented")
	case "SELF":
		tlsConfig, err = tlsutil.CreateDefaultServerTLSConfig("mediaserverdb")
	}

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

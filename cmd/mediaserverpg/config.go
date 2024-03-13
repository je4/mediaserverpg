package main

import (
	"emperror.dev/errors"
	"github.com/BurntSushi/toml"
	"github.com/je4/utils/v2/pkg/config"
	"io/fs"
	"os"
)

type TLSConfig struct {
	Type string `toml:"type"` // "ENV", "FILE", "SERVICE" OR "SELF"
	Cert string `toml:"cert"`
	Key  string `toml:"key"`
	CA   string `toml:"ca"`
}

type MediaserverPGConfig struct {
	LocalAddr string           `toml:"localaddr"`
	TLS       TLSConfig        `toml:"tls"`
	LogFile   string           `toml:"logfile"`
	LogLevel  string           `toml:"loglevel"`
	DBConn    config.EnvString `toml:"dbconn"`
}

func LoadMediaserverPGConfig(fSys fs.FS, fp string, conf *MediaserverPGConfig) error {
	if _, err := fs.Stat(fSys, fp); err != nil {
		path, err := os.Getwd()
		if err != nil {
			return errors.Wrap(err, "cannot get current working directory")
		}
		fSys = os.DirFS(path)
		fp = "mediaserverpg.toml"
	}
	data, err := fs.ReadFile(fSys, fp)
	if err != nil {
		return errors.Wrapf(err, "cannot read file [%v] %s", fSys, fp)
	}
	_, err = toml.Decode(string(data), conf)
	if err != nil {
		return errors.Wrapf(err, "error loading config file %v", fp)
	}
	return nil
}

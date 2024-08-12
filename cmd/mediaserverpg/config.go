package main

import (
	"emperror.dev/errors"
	"github.com/BurntSushi/toml"
	"github.com/je4/certloader/v2/pkg/loader"
	"github.com/je4/utils/v2/pkg/config"
	"github.com/je4/utils/v2/pkg/stashconfig"
	"io/fs"
	"os"
)

type MediaserverPGConfig struct {
	LocalAddr               string             `toml:"localaddr"`
	ServerDomains           []string           `toml:"serverdomains"`
	ResolverAddr            string             `toml:"resolveraddr"`
	ResolverTimeout         config.Duration    `toml:"resolvertimeout"`
	ResolverNotFoundTimeout config.Duration    `toml:"resolvernotfoundtimeout"`
	GRPCClient              map[string]string  `toml:"grpcclient"`
	ServerTLS               loader.Config      `toml:"servertls"`
	MiniresolverClientTLS   loader.Config      `toml:"miniresolverclienttls"`
	DBConn                  config.EnvString   `toml:"dbconn"`
	Log                     stashconfig.Config `toml:"log"`
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

module github.com/je4/mediaserverpg/v2

go 1.22.2

replace github.com/telkomdev/go-stash => github.com/png-ub/go-stash v0.0.0-20230831094646-7daebd817e31

require (
	emperror.dev/errors v0.8.1
	github.com/BurntSushi/toml v1.4.0
	github.com/bluele/gcache v0.0.2
	github.com/google/uuid v1.6.0
	github.com/jackc/pgtype v1.14.3
	github.com/jackc/pgx/v5 v5.6.0
	github.com/je4/genericproto/v2 v2.0.3
	github.com/je4/mediaserverproto/v2 v2.0.27
	github.com/je4/miniresolver/v2 v2.0.8
	github.com/je4/trustutil/v2 v2.0.12
	github.com/je4/utils/v2 v2.0.38
	github.com/rs/zerolog v1.33.0
	gitlab.switch.ch/ub-unibas/go-ublogger v0.0.0-20240529135102-38bc77a4bfdf
	google.golang.org/grpc v1.64.0
	google.golang.org/protobuf v1.34.1
)

require (
	filippo.io/edwards25519 v1.1.0 // indirect
	github.com/jackc/pgio v1.0.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20231201235250-de7065d80cb9 // indirect
	github.com/jackc/puddle/v2 v2.2.1 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/telkomdev/go-stash v1.0.4 // indirect
	go.step.sm/crypto v0.46.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/crypto v0.23.0 // indirect
	golang.org/x/net v0.25.0 // indirect
	golang.org/x/sync v0.7.0 // indirect
	golang.org/x/sys v0.20.0 // indirect
	golang.org/x/text v0.15.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240528184218-531527333157 // indirect
)

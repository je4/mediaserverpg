## Generate GRPC code for pgcontroller
```bash
protoc --proto_path=pkg --go_out=pkg --go_opt=paths=source_relative --go-grpc_out=pkg --go-grpc_opt=paths=source_relative pkg/pgcontroller/pgcontroller.proto
```
### Generate GRPC code for mediaserver database services
```bash
protoc --proto_path=. --proto_path=../../../mediaserverdb/pkg/mediaserverdbproto/ --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative *.proto
```

### Generate Documentation for mediaserver database services
```bash
protoc --proto_path=. --doc_out="template=doc/gen_doc_tpl/tmpl.html:doc/"  *.proto
```

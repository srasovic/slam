Initial version of the socket listener code.
Build with modules (requires >1.11 golang):

go build -o slam -i init.go config_reader.go config_parser.go printer.go socket_processor.go

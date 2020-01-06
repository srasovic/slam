Initial version of the socket listener code.
Build with modules (requires >1.11 golang):

`go build -o slam -i init.go config_reader.go config_parser.go printer.go socket_processor.go`

test.config included shows the inital structure of the config file that needs to be present in the same directory.
Next phase will turn this into web socket.

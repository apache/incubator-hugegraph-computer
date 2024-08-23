
#!/bin/bash
go test -c vermeer_test.go -tags vermeer_test
./main.test -mode=function
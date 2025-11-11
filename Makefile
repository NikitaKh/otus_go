.PHONY: proto build run test clean install-deps

# Генерация protobuf кода
proto:
	protoc --go_out=. --go_opt=paths=source_relative appsinstalled.proto

# Установка зависимостей
install-deps:
	go mod download
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest

# Сборка проекта
build: proto
	go build -o memc_load main.go appsinstalled.pb.go

# Запуск тестов
test:
	go run . -test

# Запуск в dry-run режиме
run-dry:
	go run . -dry -pattern="./testdata/*.tsv.gz"

# Очистка сгенерированных файлов
clean:
	rm -f memc_load appsinstalled.pb.go

# Форматирование кода
fmt:
	go fmt ./...


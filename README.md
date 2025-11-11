# Memcached Loader - Go

Утилита для загрузки данных об установленных приложениях в memcached. 

## Описание

Программа читает gzip-сжатые TSV файлы с информацией об установленных приложениях на устройствах и загружает их в memcached. Поддерживает параллельную обработку с использованием goroutines и постоянные соединения с memcached серверами.

## Требования

- Go 1.21 или выше
- Protocol Buffers компилятор (protoc)
- Memcached сервера (опционально, для реальной работы)

## Установка

### 1. Установка Protocol Buffers компилятора

**macOS:**
```bash
brew install protobuf
```

**Linux:**
```bash
sudo apt-get install protobuf-compiler
# или
sudo yum install protobuf-compiler
```

### 2. Установка зависимостей и генерация protobuf кода

```bash
make install-deps
make proto
```

Или вручную:
```bash
go mod download
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
protoc --go_out=. --go_opt=paths=source_relative appsinstalled.proto
```

## Сборка

```bash
make build
```

Или вручную:
```bash
go build -o memc_load main.go appsinstalled.pb.go
```

## Использование

### Базовый запуск

```bash
./memc_load -pattern="/data/appsinstalled/*.tsv.gz"
```

### Параметры командной строки

- `-pattern` - паттерн для поиска файлов (по умолчанию: `/data/appsinstalled/*.tsv.gz`)
- `-idfa` - адрес memcached сервера для устройств idfa (по умолчанию: `127.0.0.1:33013`)
- `-gaid` - адрес memcached сервера для устройств gaid (по умолчанию: `127.0.0.1:33014`)
- `-adid` - адрес memcached сервера для устройств adid (по умолчанию: `127.0.0.1:33015`)
- `-dvid` - адрес memcached сервера для устройств dvid (по умолчанию: `127.0.0.1:33016`)
- `-dry` - режим dry-run (без записи в memcached)
- `-log` - путь к лог-файлу (по умолчанию: вывод в stdout)
- `-test` - запустить тест protobuf сериализации

### Примеры

**Тестирование protobuf:**
```bash
./memc_load -test
# или
make test
```

**Dry-run режим (без записи в memcached):**
```bash
./memc_load -dry -pattern="./testdata/*.tsv.gz"
```

**Запуск с логированием в файл:**
```bash
./memc_load -pattern="/data/appsinstalled/*.tsv.gz" -log="memc_load.log"
```

**Указание своих адресов memcached:**
```bash
./memc_load -pattern="/data/*.tsv.gz" \
  -idfa="10.0.0.1:11211" \
  -gaid="10.0.0.2:11211" \
  -adid="10.0.0.3:11211" \
  -dvid="10.0.0.4:11211"
```

## Формат входных данных

Программа ожидает gzip-сжатые TSV файлы со следующими колонками:
```
dev_type	dev_id	latitude	longitude	app_ids
```

Пример строки:
```
idfa	1rfw452y52g2gq4g	55.55	42.42	1423,43,567,3,7,23
```

Где:
- `dev_type` - тип устройства (idfa, gaid, adid, dvid)
- `dev_id` - идентификатор устройства
- `latitude` - широта
- `longitude` - долгота
- `app_ids` - список ID установленных приложений через запятую

## Архитектура

### Основные компоненты

- **parseAppsInstalled** - парсинг строк из TSV файла
- **insertAppsInstalled** - сериализация в protobuf и запись в memcached
- **processFile** - обработка одного файла с использованием goroutines
- **main** - инициализация и координация обработки файлов

### Особенности реализации

1. **Постоянные соединения**: Используются persistent connections к memcached серверам
2. **Параллелизм**: Goroutines с семафором (по умолчанию 100 параллельных операций)
3. **Обработка ошибок**: Подсчет ошибок с проверкой допустимого порога (1%)
4. **Хронологический порядок**: Файлы обрабатываются по времени модификации
5. **Переименование файлов**: После обработки файлы переименовываются с добавлением точки в начало

## Отличия от Python версии

- Использование goroutines вместо asyncio
- Постоянные соединения к memcached из коробки
- Более простая сортировка файлов (bubble sort вместо использования библиотечных функций)
- Типизированная структура конфигурации
- Семафор для контроля параллелизма реализован через buffered channel

## Разработка

### Форматирование кода

```bash
make fmt
```

### Очистка

```bash
make clean
```

## Производительность

Программа использует:
- Buffered channels для результатов
- WaitGroup для синхронизации goroutines
- Семафор для ограничения параллельных операций (100 по умолчанию)
- Connection pooling в memcached клиенте (10 idle connections)
- Timeout 3 секунды для операций с memcached

## Логирование

Логи включают:
- Информацию о начале обработки каждого файла
- Ошибки парсинга и записи
- Статистику по ошибкам для каждого файла
- Неизвестные типы устройств

## Лицензия

Образовательный проект для курса OTUS Go


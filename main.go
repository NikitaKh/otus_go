package main

import (
	"bufio"
	"compress/gzip"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"google.golang.org/protobuf/proto"
)

const (
	normalErrRate = 0.01
)

// AppsInstalled представляет распарсенную запись об установленных приложениях
type AppsInstalled struct {
	DevType string
	DevID   string
	Lat     float64
	Lon     float64
	Apps    []uint32
}

// Config содержит конфигурацию приложения
type Config struct {
	Pattern string
	Idfa    string
	Gaid    string
	Adid    string
	Dvid    string
	Dry     bool
	LogFile string
	Test    bool
}

// MemcClient обертка над memcache клиентом для переиспользования соединений
type MemcClient struct {
	client *memcache.Client
	addr   string
}

// dotRename переименовывает файл, добавляя точку в начало имени
func dotRename(path string) error {
	dir := filepath.Dir(path)
	base := filepath.Base(path)
	newPath := filepath.Join(dir, "."+base)
	return os.Rename(path, newPath)
}

// parseAppsInstalled парсит строку из файла в структуру AppsInstalled
func parseAppsInstalled(line string) (*AppsInstalled, error) {
	parts := strings.Split(strings.TrimSpace(line), "\t")
	if len(parts) < 5 {
		return nil, fmt.Errorf("недостаточно полей в строке")
	}

	devType := parts[0]
	devID := parts[1]
	latStr := parts[2]
	lonStr := parts[3]
	rawApps := parts[4]

	if devType == "" || devID == "" {
		return nil, fmt.Errorf("пустой dev_type или dev_id")
	}

	lat, err := strconv.ParseFloat(latStr, 64)
	if err != nil {
		return nil, fmt.Errorf("неверные координаты lat: %v", err)
	}

	lon, err := strconv.ParseFloat(lonStr, 64)
	if err != nil {
		return nil, fmt.Errorf("неверные координаты lon: %v", err)
	}

	// Парсим список приложений
	var apps []uint32
	appStrs := strings.Split(rawApps, ",")
	for _, appStr := range appStrs {
		appStr = strings.TrimSpace(appStr)
		if appStr == "" {
			continue
		}
		app, err := strconv.ParseUint(appStr, 10, 32)
		if err != nil {
			log.Printf("Не все app id являются числами: %s", line)
			continue
		}
		apps = append(apps, uint32(app))
	}

	return &AppsInstalled{
		DevType: devType,
		DevID:   devID,
		Lat:     lat,
		Lon:     lon,
		Apps:    apps,
	}, nil
}

// insertAppsInstalled записывает данные в memcached
func insertAppsInstalled(client *MemcClient, appsInstalled *AppsInstalled, dry bool) error {
	ua := &UserApps{
		Lat:  proto.Float64(appsInstalled.Lat),
		Lon:  proto.Float64(appsInstalled.Lon),
		Apps: appsInstalled.Apps,
	}

	key := fmt.Sprintf("%s:%s", appsInstalled.DevType, appsInstalled.DevID)
	packed, err := proto.Marshal(ua)
	if err != nil {
		return fmt.Errorf("ошибка сериализации protobuf: %v", err)
	}

	if dry {
		log.Printf("%s - %s -> lat=%f, lon=%f, apps=%v",
			client.addr, key, appsInstalled.Lat, appsInstalled.Lon, appsInstalled.Apps)
		return nil
	}

	err = client.client.Set(&memcache.Item{
		Key:   key,
		Value: packed,
	})
	if err != nil {
		return fmt.Errorf("ошибка записи в memcache %s: %v", client.addr, err)
	}

	return nil
}

// processFile обрабатывает один файл
func processFile(filename string, deviceMemc map[string]*MemcClient, dry bool, semaphore chan struct{}) {
	log.Printf("Обработка файла %s", filename)

	file, err := os.Open(filename)
	if err != nil {
		log.Printf("Ошибка открытия файла %s: %v", filename, err)
		return
	}
	defer file.Close()

	gzipReader, err := gzip.NewReader(file)
	if err != nil {
		log.Printf("Ошибка создания gzip reader для %s: %v", filename, err)
		return
	}
	defer gzipReader.Close()

	processed := 0
	errors := 0
	scanner := bufio.NewScanner(gzipReader)

	var wg sync.WaitGroup
	resultChan := make(chan bool, 1000)

	// Горутина для подсчета результатов
	go func() {
		for ok := range resultChan {
			if ok {
				processed++
			} else {
				errors++
			}
		}
	}()

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		appsInstalled, err := parseAppsInstalled(line)
		if err != nil {
			errors++
			continue
		}

		memcClient, ok := deviceMemc[appsInstalled.DevType]
		if !ok {
			errors++
			log.Printf("Неизвестный тип устройства: %s", appsInstalled.DevType)
			continue
		}

		wg.Add(1)
		// Ограничиваем количество параллельных операций через семафор
		semaphore <- struct{}{}
		go func(client *MemcClient, apps *AppsInstalled) {
			defer wg.Done()
			defer func() { <-semaphore }()

			err := insertAppsInstalled(client, apps, dry)
			resultChan <- (err == nil)
			if err != nil {
				log.Printf("Ошибка вставки: %v", err)
			}
		}(memcClient, appsInstalled)
	}

	// Ждем завершения всех горутин
	wg.Wait()
	close(resultChan)

	// Небольшая задержка чтобы горутина подсчета успела обработать все результаты
	time.Sleep(100 * time.Millisecond)

	if err := scanner.Err(); err != nil {
		log.Printf("Ошибка чтения файла %s: %v", filename, err)
	}

	if processed == 0 {
		dotRename(filename)
		return
	}

	errRate := float64(errors) / float64(processed)
	if errRate < normalErrRate {
		log.Printf("Допустимый уровень ошибок (%f). Успешная загрузка", errRate)
	} else {
		log.Printf("Высокий уровень ошибок (%f > %f). Неудачная загрузка", errRate, normalErrRate)
	}

	dotRename(filename)
}

// protoTest выполняет тестирование protobuf сериализации/десериализации
func protoTest() {
	sample := `idfa	1rfw452y52g2gq4g	55.55	42.42	1423,43,567,3,7,23
gaid	7rfw452y52g2gq4g	55.55	42.42	7423,424`

	for _, line := range strings.Split(sample, "\n") {
		parts := strings.Split(strings.TrimSpace(line), "\t")
		if len(parts) < 5 {
			continue
		}

		lat, _ := strconv.ParseFloat(parts[2], 64)
		lon, _ := strconv.ParseFloat(parts[3], 64)

		var apps []uint32
		for _, appStr := range strings.Split(parts[4], ",") {
			app, err := strconv.ParseUint(strings.TrimSpace(appStr), 10, 32)
			if err == nil {
				apps = append(apps, uint32(app))
			}
		}

		ua := &UserApps{
			Lat:  proto.Float64(lat),
			Lon:  proto.Float64(lon),
			Apps: apps,
		}

		packed, err := proto.Marshal(ua)
		if err != nil {
			log.Fatalf("Ошибка сериализации: %v", err)
		}

		unpacked := &UserApps{}
		err = proto.Unmarshal(packed, unpacked)
		if err != nil {
			log.Fatalf("Ошибка десериализации: %v", err)
		}

		if !proto.Equal(ua, unpacked) {
			log.Fatalf("Protobuf не совпадают")
		}
	}
	fmt.Println("Protobuf тест пройден успешно")
}

func main() {
	config := &Config{}

	flag.BoolVar(&config.Test, "test", false, "Запустить тест protobuf")
	flag.StringVar(&config.LogFile, "log", "", "Путь к лог-файлу")
	flag.BoolVar(&config.Dry, "dry", false, "Dry run режим (без записи в memcache)")
	flag.StringVar(&config.Pattern, "pattern", "/data/appsinstalled/*.tsv.gz", "Паттерн для поиска файлов")
	flag.StringVar(&config.Idfa, "idfa", "127.0.0.1:33013", "Адрес memcached для idfa")
	flag.StringVar(&config.Gaid, "gaid", "127.0.0.1:33014", "Адрес memcached для gaid")
	flag.StringVar(&config.Adid, "adid", "127.0.0.1:33015", "Адрес memcached для adid")
	flag.StringVar(&config.Dvid, "dvid", "127.0.0.1:33016", "Адрес memcached для dvid")

	flag.Parse()

	// Настройка логирования
	if config.LogFile != "" {
		logFile, err := os.OpenFile(config.LogFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			log.Fatalf("Ошибка открытия лог-файла: %v", err)
		}
		defer logFile.Close()
		log.SetOutput(logFile)
	}

	log.SetFlags(log.Ldate | log.Ltime)

	if config.Test {
		protoTest()
		return
	}

	// Создаем клиенты memcached с постоянными соединениями
	deviceMemc := map[string]*MemcClient{
		"idfa": {client: memcache.New(config.Idfa), addr: config.Idfa},
		"gaid": {client: memcache.New(config.Gaid), addr: config.Gaid},
		"adid": {client: memcache.New(config.Adid), addr: config.Adid},
		"dvid": {client: memcache.New(config.Dvid), addr: config.Dvid},
	}

	// Устанавливаем таймауты для клиентов
	for _, client := range deviceMemc {
		client.client.Timeout = 3 * time.Second
		client.client.MaxIdleConns = 10
	}

	log.Printf("Memc loader запущен с параметрами: pattern=%s, dry=%v", config.Pattern, config.Dry)

	// Находим файлы по паттерну
	files, err := filepath.Glob(config.Pattern)
	if err != nil {
		log.Fatalf("Ошибка поиска файлов: %v", err)
	}

	// Сортируем файлы по времени модификации (хронологический порядок)
	type fileInfo struct {
		path    string
		modTime time.Time
	}
	var fileInfos []fileInfo
	for _, file := range files {
		info, err := os.Stat(file)
		if err != nil {
			log.Printf("Ошибка получения информации о файле %s: %v", file, err)
			continue
		}
		fileInfos = append(fileInfos, fileInfo{path: file, modTime: info.ModTime()})
	}

	// Простая сортировка по времени
	for i := 0; i < len(fileInfos); i++ {
		for j := i + 1; j < len(fileInfos); j++ {
			if fileInfos[i].modTime.After(fileInfos[j].modTime) {
				fileInfos[i], fileInfos[j] = fileInfos[j], fileInfos[i]
			}
		}
	}

	// Семафор для ограничения параллельных операций записи в memcached
	semaphore := make(chan struct{}, 100)

	// Обрабатываем файлы последовательно в хронологическом порядке
	for _, fileInfo := range fileInfos {
		processFile(fileInfo.path, deviceMemc, config.Dry, semaphore)
	}

	log.Println("Обработка завершена")
}


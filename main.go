// необходимо ограничение на максимальное количество блоков для записи
package main

import (
	// "errors"
	"flag"
	"io"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"database/sql"
	"fmt"
	"io/ioutil"
	"sync"
	"time"

	"github.com/hjson/hjson-go"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"

	_ "github.com/go-sql-driver/mysql"
)

//карта для отслеживания открытых файлов и блокировки удаления/закрытия файлов, т.к. обращение к функции удаления происходит раньше, чем завершается его закрытие и обнуление флагов
type filesFlagsStruct struct {
	reading bool
	writing bool
	backup  bool
	pipes   uint
}

var filesOpenMutex sync.Mutex
var filesOpen map[uint64]*filesFlagsStruct

// карта для списка файлов на чтение с кассет
// var filesToRead map[uint64]bool
var filesToRead []uint64
var filesToReadMutex sync.Mutex

// var filesys *FS
var filesReleaseDeleteLock sync.Mutex

// структура ответа запроса из БД
type folderContentStruct struct {
	name     string
	nameType bool
	uid      uint64
	size     uint64
	mtime    string // время модификации
	atime    string // время последнего доступа
	ctime    string // время изменения атрибутов
	// lastread     uint64
	// needtobackup bool
	// version uint16
}

type Node struct {
	inode uint64
	name  string
}

var inode uint64
var Usage = func() {
	log.Printf("Usage of %s:\n", os.Args[0])
	log.Printf("  %s MOUNTPOINT\n", os.Args[0])
	log.Printf("use flag -t after mountpoint for running in test mode")
	flag.PrintDefaults()
}

var db *sql.DB // делаем подключение к базе глобальным
func main() {
	// инициализация работы с log файлом
	logfilePurge("./kts.log", 256*1024)                                                // стирает лог файл, если его размер превышает 256 Кб
	logFile, errL := os.OpenFile("./kts.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666) // файл создается в корне пользователя
	checkerr(errL)
	defer logFile.Close() // don't forget to close it
	// log.SetOutput(logFile) // assign it to the standard logger
	mw := io.MultiWriter(os.Stdout, logFile)
	log.SetOutput(mw)
	// log.SetFlags(log.LstdFlags | log.Lshortfile)

	log.Println("Start working")

	chanShutdownSchedulerInit := make(chan bool, 1) // канал инициирования выключения планировщика
	chanShutdownSchedulerConfirm := make(chan bool) // канал подтверждения выключения планировщика

	filesOpen = make(map[uint64]*filesFlagsStruct) // инициализация map для отслеживания открытых файлов
	// filesToRead = make(map[uint64]bool)            // инициализация map для файлов на чтение
	// filesToRead = make([]uint64, 0)
	filesToRead = []uint64{}

	flag.Usage = Usage
	flag.Parse()

	if flag.NArg() < 1 {
		Usage()
		os.Exit(2)
	}

	// отработка работы в тестовом режиме
	if flag.Arg(1) == "-t" {
		setConfigVariable(true) // установка настроек для тестового режима
	} else {
		setConfigVariable(false) // в обычном режиме работы с кассетным приводом
	}
	mountpoint := flag.Arg(0)

	// open sql base
	var err error
	db, err = sql.Open("mysql", config.mysql)
	checkerr(err)
	defer db.Close()

	// mount wth set read buff size
	c, err := fuse.Mount(mountpoint, fuse.MaxReadahead(128*1024), fuse.AsyncRead(), fuse.WritebackCache(), fuse.AllowOther())
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	// размонтирование при закрытии по нажатию Ctrl-C
	breakSignal := make(chan os.Signal, 1)
	signal.Notify(breakSignal, os.Interrupt)
	go func() {
		for sig := range breakSignal {
			if sig == syscall.SIGINT {
				log.Printf("[main] got break signal")
				chanShutdownSchedulerInit <- true // отправка в планировщик сигнала на прекращение работы
				select {
				case schedulerReady := <-chanShutdownSchedulerConfirm: // получение сигнала от планировщика
					log.Printf("[main] scheduler closed")
					if schedulerReady { // подтверждение от планировщика о прекращении работы
						fuse.Unmount(mountpoint)
						log.Println(mountpoint, " unmounted")
						os.Exit(0)
					}
				}

			}
		}
	}()

	// запуск рутины с тестом формирования пакета
	go goMakeBackup(chanShutdownSchedulerInit, chanShutdownSchedulerConfirm)

	if p := c.Protocol(); !p.HasInvalidate() {
		log.Panicln("kernel FUSE support is too old to have invalidations: version %v", p)
	}
	srv := fs.New(c, nil)
	filesys := &FS{
		&Dir{Node: Node{name: "root", inode: 0}},
	}

	log.Println("About to serve fs")
	if err := srv.Serve(filesys); err != nil {
		log.Panicln(err)
	}
	// Check if the mount process has an error to report.
	<-c.Ready
	if err := c.MountError; err != nil {
		log.Panicln(err)
	}
}

// ##### SUBS #####

// возврат содержимого папки по его fatherid, возвращаем слайс
func SqlGetFolderContent(inode uint64) []folderContentStruct {
	// log.Printf("[SqlGetFolderContent] requested %d", inode)
	var content []folderContentStruct // определение массива структуры
	var rows *sql.Rows
	var err error

	rows, err = db.Query("SELECT `name`, `type`, `uid`, `size`, mtime, atime, ctime FROM root_index WHERE fatherid = ? AND deleted = 0", inode)
	checkerr(err)
	defer rows.Close()

	for rows.Next() { // перебор результатов запроса и складывание его в слайс
		var folderContent folderContentStruct
		if err := rows.Scan(&folderContent.name, &folderContent.nameType, &folderContent.uid, &folderContent.size, &folderContent.mtime, &folderContent.atime, &folderContent.ctime); err != nil {
			log.Printf("here1")
			// log.Fatal(err)
			log.Printf("%v", err)
			log.Printf("here2")
		}
		content = append(content, folderContentStruct{folderContent.name, folderContent.nameType, folderContent.uid, folderContent.size, folderContent.mtime, folderContent.atime, folderContent.ctime})
	}
	return content
}

// возврат свойств файла по fatherid и имени
func SqlGetFileProperties(name string, inode uint64) folderContentStruct {
	var content folderContentStruct
	// log.Println("[SqlGetFileProperties] name: ", name)

	err := db.QueryRow("SELECT `name`, `type`, uid, size, mtime, atime, ctime FROM root_index WHERE fatherid = ? AND `name` = ? AND deleted = 0", inode, name).Scan(&content.name, &content.nameType, &content.uid, &content.size, &content.mtime, &content.atime, &content.ctime)
	if err != nil { // обработчик ошибок
		if err == sql.ErrNoRows { // обработчик ошибок пустых строк, срабатывает при переходе в папку, которой нет
			content.name = ""
			content.nameType = true
			return content
		} else {
			log.Fatal(err)
		}
	}
	return content
}

// возврат свойств файла по inode
func SqlGetFilePropertiesByINode(inode uint64) folderContentStruct {
	var content folderContentStruct
	// log.Println("[SqlGetFilePropertiesByINode] uid: ", inode)

	err := db.QueryRow("SELECT `name`, `type`, uid, size, mtime, atime, ctime FROM root_index WHERE uid = ? AND deleted = 0", inode).Scan(&content.name, &content.nameType, &content.uid, &content.size, &content.mtime, &content.atime, &content.ctime)
	if err != nil { // обработчик ошибок
		if err == sql.ErrNoRows { // обработчик ошибок пустых строк, срабатывает при переходе в папку, которой нет
			content.name = ""
			content.nameType = true
			return content
		} else {
			log.Fatal(err)
		}
	}
	return content
}

// обновление размера файла по его uid и size
func SqlUpdateFileSize(inode uint64, size uint64) {
	_, err := db.Exec("UPDATE root_index SET `size` = ? WHERE uid =?", size, inode)
	checkerr(err)
}

// простой обработчик ошибок
func checkerr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

// обработчик ошибок с выводом комментария
func checkErrWithComment(err error, text string) {
	if err != nil {
		log.Printf("%s", text)
		log.Fatal(err)
	}
}

func uint2string(val uint64) string {
	str := strconv.Itoa(int(val))
	return str
}

func uint2string10(val uint64) string {
	str := strconv.Itoa(int(val))
	return fmt.Sprintf("%010s", str)
}

func string2uint(val string) uint {
	// log.Printf("[string2uint] %s", val)
	i, err := strconv.Atoi(val)
	checkerr(err)
	return uint(i)
}

func string2uint64(val string) uint64 {
	// log.Printf("[string2uint64] in %v %s", val, string(val))
	i, err := strconv.ParseInt(val, 10, 64) //  Atoi(val)
	checkerr(err)
	return uint64(i)
}

// mysql unix timestamp to time
func timestamp2time(timestamp string) time.Time {
	layout := "2006-01-02 15:04:05"
	t, err := time.Parse(layout, timestamp)
	checkerr(err)
	// log.Printf("[timestamp2time] in: %s, out: %v", timestamp, t)
	return t
}

// проверка существует ли файл
func fileExists(path string) bool {
	_, err := os.Stat(path)
	if err == nil {
		return true
	}
	if os.IsNotExist(err) {
		return false
	}
	return true
}

// проверка существует ли в кэше uid и формирование запроса на чтение
func checkInCache(uid uint64, runFromOpen bool) bool {
	inCache := fileExists(config.cachePath + uint2string(uid))
	if !inCache { // запрос файла
		if runFromOpen { // если файл открывается для чтения, то формировать запрос
			// if _, ok := filesToRead[uid]; !ok {
			if !checkInSlice(filesToRead, uid) {
				filesToReadMutex.Lock()
				// filesToRead[uid] = true
				filesToRead = append(filesToRead, uid)
				// log.Printf("[checkInCache] %v", filesToRead)
				filesToReadMutex.Unlock()
			}
		}
	}
	return inCache
}

// проверка наличия uint в slice
func checkInSlice(slice []uint64, uid uint64) bool {
	// save the items of slice in map
	m := make(map[uint64]bool)
	for i := 0; i < len(slice); i++ {
		m[slice[i]] = true
	}

	if _, ok := m[uid]; ok {
		return true
	} else {
		return false
	}
}

// удаление из слайса определенного значения
func removeFromSliceByContent(slice []uint64, source uint64) []uint64 {
	var index int
	var value uint64
	for index, value = range slice {
		if value == source {
			break
		}
	}
	return append(slice[:index], slice[index+1:]...)
}

// // пометить кассету как ошибочную для исключения из цикла до разбирательства и отметка содержащихся на ней файлов для создания их дополнительной копии
func updateTapeSetToDuplicate(tapeNo uint64) {
	tx, err := db.Begin()
	checkerr(err)
	defer tx.Rollback()

	_, err = tx.Exec("UPDATE tapes SET broken = 1 WHERE tape_no = ?", tapeNo)
	checkerr(err)

	_, err = tx.Exec("UPDATE root_index SET needtobackup = 1 WHERE uid IN (SELECT file_uid FROM files_location WHERE tape_no = ? AND deleted = 0 AND todelete = 0)", tapeNo)
	checkerr(err)

	_, err = tx.Exec("UPDATE files_location SET broken = 1 WHERE deleted = 0 AND todelete = 0 AND tape_no = ?", tapeNo)
	checkerr(err)

	tx.Commit()
}

// возвращаем кассету в строй
func sqlResumeTheTape(tapeNo uint64) {
	tx, err := db.Begin()
	checkerr(err)
	defer tx.Rollback()

	_, err = tx.Exec("UPDATE tapes SET broken = 0 WHERE tape_no = ?", tapeNo)
	checkerr(err)

	_, err = tx.Exec("UPDATE files_location SET deleted = 1 WHERE broken = 1 AND tape_no = ?", tapeNo)
	checkerr(err)

	tx.Commit()
}

// исключение уже прочитанных в кэш файлов, отмечаем файлы с ошибкой чтения флагом needtobackup
func updateFilesOpen(readResult map[uint64]bool, readResultTape map[uint64]uint64) {
	// log.Printf("[updateFilesOpen] readResult %v", readResult)
	// log.Printf("[updateFilesOpen] readResultTape %v", readResultTape)
	for uid, status := range readResult {
		if status {
			log.Printf("[updateFilesOpen] file uid %d on tape %d updated is OK", uid, readResultTape[uid])
			filesToReadMutex.Lock()
			filesToRead = removeFromSlice(filesToRead, findInSlice(filesToRead, uid))
			filesToReadMutex.Unlock()
			_, err := db.Exec("UPDATE root_index SET atime = CURRENT_TIMESTAMP() WHERE uid = ?", uid)
			checkerr(err)
			// log.Printf("[updateFilesOpen] file %d loaded", uid)
		} else {
			// log.Printf("[updateFilesOpen] file uid %d on tape %d updated as broken", uid, readResultTape[uid])
			// обновляем статус файла, т.к. не смогли его прочитать с этой кассеты
			// узнаем номер кассеты и номер блока по ошибочному файлу
			// всем файлам блока даем статус на обновление
			tx, err := db.Begin()
			checkerr(err)
			defer tx.Rollback()

			_, err = tx.Exec("UPDATE root_index SET needtobackup = 1 WHERE uid IN (SELECT file_uid FROM files_location WHERE tape_no = ? AND tape_record_no IN (SELECT tape_record_no FROM files_location WHERE deleted = 0 AND todelete = 0 AND file_uid = ? AND tape_no = ?))", readResultTape[uid], uid, readResultTape[uid])
			//
			// _, err := db.Exec("UPDATE kts.index SET needtobackup = 1 WHERE uid = ?", uid)
			log.Printf("[updateFilesOpen] file %d did not load, set to rewrite it's block on tape %d", uid, readResultTape[uid])
			checkerr(err)
			_, err = tx.Exec(`UPDATE
  files_location
SET
  broken = 1
WHERE tape_no = ? AND file_uid IN
  (SELECT * FROM (SELECT
    file_uid
  FROM
    files_location
  WHERE tape_no = ?
    AND tape_record_no IN
    (SELECT
      tape_record_no
    FROM
      files_location
    WHERE deleted = 0
      AND todelete = 0
      AND file_uid = ?
      AND tape_no = ?)) AS p)`, readResultTape[uid], readResultTape[uid], uid, readResultTape[uid]) // помечаем данные файлы на кассете сбойными
			checkerr(err)
			tx.Commit()
		}
	}
	// os.Exit(0)
}

// поиск позиции в слайсе
func findInSlice(slice []uint64, element uint64) uint {
	for k, v := range slice {
		if v == element {
			return uint(k)
		}
	}
	return 0
}

// удаление из слайса по позиции
func removeFromSlice(slice []uint64, s uint) []uint64 {
	return append(slice[:s], slice[s+1:]...)
}

// установка конфигурационной переменной
func setConfigVariable(testmode bool) {
	mainConfig := readCfgFile("./config.json") // чтение конфигурации
	if testmode {
		config.mysql = mainConfig["TestMysql"].(string)
		config.cachePath = mainConfig["TestCachePath"].(string)
		config.testMode = true
		config.timeoutQueueTurnover = uint(mainConfig["TestTimeoutQueueTurnover"].(float64))
		config.tapeBlockSizeLimit = uint64(mainConfig["TestTapeBlockSizeLimit"].(float64))

		// config.mysql = "kts:194432@tcp(localhost:3306)/kts_test" // user:password@tcp(host:port)/base
		// config.cachePath = "/mnt/storage_test/"
		// config.timeoutQueueTurnover = 2              // задержка между повторным сканированием файлов на бэкап в секундах
		// config.tapeBlockSizeLimit = 10 * 1024 * 1024 // максимальный размер блока в байтах
	} else {
		config.mysql = mainConfig["Mysql"].(string)
		config.cachePath = mainConfig["CachePath"].(string)
		config.testMode = false
		config.timeoutQueueTurnover = uint(mainConfig["TimeoutQueueTurnover"].(float64))
		config.tapeBlockSizeLimit = uint64(mainConfig["TapeBlockSizeLimit"].(float64))
		// config.mysql = "kts:194432@tcp(localhost:3306)/kts" // user:password@tcp(host:port)/base
		// config.cachePath = "/mnt/sdc/storage/"
		// config.timeoutQueueTurnover = 60 * 2               // задержка между повторным сканированием файлов на бэкап в секундах
		// config.tapeBlockSizeLimit = 1 * 1024 * 1024 * 1024 // максимальный размер блока в байтах
	}
	config.timeBeforeBackup = uint(mainConfig["TimeBeforeBackup"].(float64))
	config.tempTapeStoragePath = mainConfig["TempTapeStoragePath"].(string)
	config.numberOfCopies = uint(mainConfig["NumberOfCopies"].(float64))
	config.cacheTimeLive = uint(mainConfig["CacheTimeLive"].(float64))
	config.driveName = mainConfig["DriveName"].(string)
	config.bufioSize = 4 * 1024 * 1024

	// config.timeBeforeBackup = 10               // минимальное время в секундах после обновления/создания файла для отправки его на кассету
	// config.tempTapeStoragePath = "/mnt/tapes/" // временное (на время тестов) хранилище кассет
	// config.numberOfCopies = 1 // количество копий файла
	// config.timeoutQueueTurnover = 60 * 2               // задержка между повторным сканированием файлов на бэкап в секундах
	// config.cacheTimeLive = 60 * 2  // время жизни файлов в кэше в секундах
	// config.driveName = "/dev/nst0" // системное имя привода, неперематываемое
	log.Printf("[setConfigVariable] %v", config)
	// log.Printf("TimeBeforeBackup %d", config.timeBeforeBackup)
	// log.Printf("TimeoutQueueTurnover %d", config.timeoutQueueTurnover)
}

var config Config

type Config struct {
	cachePath            string
	mysql                string
	tapeBlockSizeLimit   uint64
	timeBeforeBackup     uint
	timeoutQueueTurnover uint
	tempTapeStoragePath  string
	numberOfCopies       uint
	cacheTimeLive        uint
	testMode             bool
	driveName            string
	bufioSize            int
}

// удаляет файл, если его размер превышает указанный
func logfilePurge(logFileName string, fileSize int64) {
	f, err := os.Open(logFileName)
	if err != nil {
		log.Println(err)
		return
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		log.Println(err)
	}

	if fi.Size() > fileSize {
		err := os.Remove(logFileName)

		if err != nil {
			log.Println(err)
			return
		}
	}
}

// чтение конфиг файла
func readCfgFile(filename string) map[string]interface{} {
	configFileText, err := ioutil.ReadFile(filename)
	if err != nil {
		panic(err)
	}

	var dat map[string]interface{}
	// Decode and a check for errors.
	if err := hjson.Unmarshal(configFileText, &dat); err != nil {
		panic(err)
	}

	return dat
}

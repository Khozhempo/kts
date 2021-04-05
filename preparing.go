// TO DO
//
// preparing
// здесь по запросу готовим блок, содержащий номер кассеты и uid файлов для записи в группировке по блокам
package main

import (
	// "fmt"
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"

	"os"
	"strconv"
	"strings"
	"time"
)

// структура для пакета на запись
type sqlPackageToTapeStruct struct {
	uid        uint64
	size       uint64
	total_summ uint64
	name       string
	mtime      string
}

func goMakeBackup(chanShutdownSchedulerInit chan bool, chanShutdownSchedulerConfirm chan bool) {
	toshutdown := false // команда внутри функции на ее закрытие

	for { // цикл постоянно мониторинга за наличием файлов для бэкапа

		// цикл ожидания перед сканированием
		beginTime := time.Now()
		// time.Sleep(time.Duration(config.timeoutQueueTurnover) * time.Second)
		for {
			time.Sleep(10 * time.Second)
			if time.Now().Sub(beginTime) > (time.Duration(config.timeoutQueueTurnover) * time.Second) {
				break
			}

			// проверка получения сигнала на прекращение работы
			toshutdown = chanWatcher4goMakeBackup(chanShutdownSchedulerInit)
			// select {
			// case shutdownSignal := <-chanShutdownSchedulerInit:
			// if shutdownSignal { // выход, если получен сигнал на прекращение работы
			// toshutdown = true
			// log.Printf("[goMakeBackup] got shutdown signal")
			// }
			// default:
			// }

			// выход, если получен сигнал на прекращение работы
			if toshutdown {
				break
			}
			// чтение файлов в кэш и выход, если проблемы с выгрузкой кассеты

		}
		if toshutdown {
			break
		}
		if !readToCache() {
			toshutdown = true
			break
		} // log.Printf("[goMakeBackup] start turnover")

		// log.Printf("%b", isFilesToBackup())
		if isFilesToBackup() {
			log.Printf("[goMakeBackup] is files to backup")
			tapes := tapesListWithFreeSpace()
			// log.Printf("[goMakeBackup] tapesListWithFreeSpace %v", tapes)
			for tapeNo, tapeFreeSpace := range tapes {
				package2write := createpackage2write(tapeNo, tapeFreeSpace)
				// log.Printf("[goMakeBackup] on tape %d package2write: %v", tapeNo, package2write)
				if len(package2write) > 0 { // есть что бэкапить
					// log.Printf("[goMakeBackup] tape %d, len %d, %v", tapeNo, len(package2write), package2write)
					if tapeLoad(tapeNo) { // если кассета загружена
						if tapeWriteBlocks(package2write, lastTapeRecordNo(tapeNo), tapeNo) {
							// log.Printf("[goMakeBackup] write finished")
						} else {
							log.Printf("[goMakeBackup] some error during write on tape %d", tapeNo)
							toshutdown = true // сигнал на прекращение подпрограммы
						}
						if !tapeUnload(tapeNo) { // ошибка с выгрузкой кассеты, запретить запись
							log.Printf("[goMakeBackup] can't unload tape, exiting subprogram")
							toshutdown = true // сигнал на прекращение подпрограммы
						}
					} else { // !-кассета недоступна, надо пометить ее как проблемную и выбрать другую (запустить цикл сбора заново)
						// cleanBlockFlagBackup(package2write) // снимаем в реестре с файлов флаг backup
						log.Printf("[goMakeBackup] tape did not load")
						break
					}
				}

				sqlUpdateNeedToBackupFlags() // обновить статус файлов needtobackup в таблице root_index
				cacheClear()                 // очистка кэша
				if !isFilesToBackup() {      // если бэкапить больше нечего, то выходим из цикла перебора кассет
					break
				}

				// отслеживание сигнала на закрытие функции
				toshutdown = chanWatcher4goMakeBackup(chanShutdownSchedulerInit)

				// select { // отслеживание сигнала на закрытие функции
				// case shutdownSignal := <-chanShutdownSchedulerInit:
				if toshutdown { // выход, если получен сигнал на прекращение работы
					log.Printf("[goMakeBackup] got shutdown signal")
					// toshutdown = true
					break // выходим из цикла перебора кассет
				}
				// default:
				// }
			}
			if isFilesToBackup() {
				log.Printf("[goMakeBackup] circle is over, but file to backup still is")
			}
		} else {
			cacheClear() // очистка кэша
		}
		// проверка получения сигнала на прекращение работы
		toshutdown = chanWatcher4goMakeBackup(chanShutdownSchedulerInit)
		// select {
		// case shutdownSignal := <-chanShutdownSchedulerInit:
		// 	if shutdownSignal { // выход, если получен сигнал на прекращение работы
		// 		toshutdown = true
		// 		log.Printf("[goMakeBackup] got shutdown signal")
		// 	}
		// default:
		// }

		// выход, если получен сигнал на прекращение работы
		if toshutdown {
			break
		}
	}
	chanShutdownSchedulerConfirm <- true // отправка подтверждения о завершении работы go-рутины
}

// создание пакета для записи на определенную кассету
func createpackage2write(tapeNo uint64, tapeFreeSpace uint64) map[uint64][]sqlPackageToTapeStruct {
	var package2writeFull = map[uint64][]sqlPackageToTapeStruct{} // объявление карты для хранения пакетов map[номер пакета][]{uid файлов}
	var tapeBlockSumm uint64                                      // счетчик объема файлов в блоке
	var tapeBlockCount uint64                                     // счетчик номера блока
	var sqlPackageToTape sqlPackageToTapeStruct                   // переменная результатов запроса в базе
	tapeBlockCount = 0                                            // счетчик номера блока
	tapeBlockSizeLimit := config.tapeBlockSizeLimit               // предельный размер блока в байтах

	// блок запроса uid файлов в пределах свободного места доступной кассеты, но которых нет на данной кассете
	tx, err := db.Begin()
	checkErrWithComment(err, "[createpackage2write] db.Begin")
	// checkerr(err)
	defer tx.Rollback()
	_, err = tx.Exec("SET @tot := 0")
	checkErrWithComment(err, "[createpackage2write] tx.Rollback")
	// checkerr(err)
	rows, err := tx.Query(`SELECT *
FROM
  (SELECT
    tb.uid,
    tb.size,
    @tot := @tot + tb.size AS total_summ,
    tb.name,
    tb.mtime
  FROM
    (SELECT
      uid,
      name,
      fatherid,
      mtime,
      size
    FROM
      root_index
    WHERE TYPE = 0
      AND deleted = 0
      AND needtobackup = 1
      AND mtime < NOW() - INTERVAL ? SECOND
    ORDER BY mtime
      AND fatherid) tb
    WHERE tb.uid NOT IN
    (SELECT
      file_uid
    FROM
      files_location
    WHERE tape_no = ? AND deleted != 1 AND todelete != 1)) etb HAVING total_summ < ?`, config.timeBeforeBackup, tapeNo, tapeFreeSpace)
	checkErrWithComment(err, "[createpackage2write] tx.Query for files to update")
	// checkerr(err)

	for rows.Next() {
		// перебор строк результата запроса
		err = rows.Scan(&sqlPackageToTape.uid, &sqlPackageToTape.size, &sqlPackageToTape.total_summ, &sqlPackageToTape.name, &sqlPackageToTape.mtime)
		checkErrWithComment(err, "[createpackage2write] rows.Scan, error in result parsing")
		// checkerr(err)

		if checkmakeready4backup(sqlPackageToTape.uid) == true { // если файл не открыт для записи, то помечается флагом для backup'а
			if (tapeBlockSumm + sqlPackageToTape.size) > tapeBlockSizeLimit { // если объем файлов с учетом нового в пределах блока превышает допустимый размер блока, то файл будет добавляться в следующий блок
				package2writeFull[tapeBlockCount] = append(package2writeFull[tapeBlockCount], sqlPackageToTapeStruct{sqlPackageToTape.uid, sqlPackageToTape.size, sqlPackageToTape.total_summ, sqlPackageToTape.name, sqlPackageToTape.mtime})
				if len(package2writeFull[tapeBlockCount]) > 0 { // увеличиваем номер блока только в том случае, если есть делается попытка добавить файл сверх объема в блок, который не пуст
					tapeBlockCount++  // увеличиваем счетчик номера блока
					tapeBlockSumm = 0 // обнуляем счетчик объема файлов в блоке
				}
			} else {
				package2writeFull[tapeBlockCount] = append(package2writeFull[tapeBlockCount], sqlPackageToTapeStruct{sqlPackageToTape.uid, sqlPackageToTape.size, sqlPackageToTape.total_summ, sqlPackageToTape.name, sqlPackageToTape.mtime})
				tapeBlockSumm = tapeBlockSumm + sqlPackageToTape.size // обновляем счетчик объема файлов в блоке
			}
		}
	}

	// rows.Close()
	tx.Commit()
	return package2writeFull
}

// помечает файл в реестре статусом backup, т.е. удалять и изменять нельзя, т.к. записывается
func checkmakeready4backup(uid uint64) bool {
	// log.Printf("[checkmakeready4backup] uid %d", uid)

	if !checkInCache(uid, false) { // не дает открыть, если файла нет в кэше и добавляем его в задание на чтение
		filesToReadMutex.Lock()
		filesToRead = append(filesToRead, uid)
		filesToReadMutex.Unlock()
		return false
	}

	createIfExistMapOpenFile(uid) // проверяем существование и, если необходимо, создаем uid в filesOpen реестре
	// log.Printf("[checkmakeready4backup] %v", filesOpen[uid])

	if (filesOpen[uid].writing == false) && (fileExists(config.cachePath + strconv.Itoa(int(uid)))) {
		filesOpenMutex.Lock()
		filesOpen[uid].backup = true
		filesOpenMutex.Unlock()
		return true // файл не открыт для записи и отмечается для backup
	} else {
		return false // файл не доступен для backup'а
	}
}

// снимает в реестре с блока файлов флаги backup
func cleanBlockFlagBackup(package2write map[uint64][]sqlPackageToTapeStruct) {
	for _, files := range package2write {
		for _, block := range files {
			filesOpenMutex.Lock()
			filesOpen[block.uid].backup = false
			filesOpenMutex.Unlock()
		}
	}
}

// возвращает номер последней записи на кассете
func lastTapeRecordNo(tapeNumber uint64) uint64 {
	log.Printf("[lastTapeRecordNo] tape number %d", tapeNumber)
	var lastTapeRecordNo uint64
	err := db.QueryRow("SELECT COALESCE(MAX(files_location.`tape_record_no`),0) AS lastTapeRecordNo FROM files_location WHERE tape_no = ? AND deleted = 0", tapeNumber).Scan(&lastTapeRecordNo)
	//
	if err != nil {
		log.Println("[lastTapeRecordNo] error during SQL request of last record number")
		checkerr(err)
	}
	log.Printf("[lastTapeRecordNo] lastTapeRecordNo %d", lastTapeRecordNo)
	return lastTapeRecordNo
}

// возвращает map с номером кассет и свободным местом map[uint]uint64
func tapesListWithFreeSpace() map[uint64]uint64 {
	var tapeNumber, tapeFreeSpace uint64
	tapesList := make(map[uint64]uint64)
	// rows, err := db.Query(`SELECT tapes.tape_no,
	//  kts.tape_format.capacity - COALESCE(SUM(files.size), 0) AS free_space
	// FROM
	//  kts.tapes tapes
	//  LEFT OUTER JOIN kts.files_location files
	//    ON tapes.tape_no = files.tape_no
	//  LEFT JOIN kts.tape_format
	//    ON tapes.tape_format = kts.tape_format.type
	// WHERE (files.deleted = 0 OR files.deleted IS NULL) AND kts.tapes.broken = 0
	// GROUP BY tapes.tape_no
	// HAVING free_space > 500 * 1024 ^ 3
	// ORDER BY free_space, tapes.tape_no ASC
	// `)

	//
	rows, err := db.Query(`SELECT
  tapes.tape_no,
  tapes.capacity - COALESCE(files.occupied, 0)
FROM
  (SELECT
    tapes.tape_no AS tape_no,
    tape_format.capacity AS capacity
  FROM
    tapes
    LEFT OUTER JOIN tape_format
      ON tapes.tape_format = tape_format.type) AS tapes
  LEFT JOIN
    (SELECT
      files.tape_no,
      SUM(files.size) AS occupied
    FROM
      files_location files
    WHERE files.deleted = 0
      OR files.deleted IS NULL
    GROUP BY files.tape_no) files
    ON tapes.tape_no = files.tape_no`)
	checkerr(err)
	defer rows.Close()

	for rows.Next() { // перебор строк результата запроса
		if err := rows.Scan(&tapeNumber, &tapeFreeSpace); err != nil {
			log.Fatal(err)
		}
		tapesList[tapeNumber] = tapeFreeSpace
		// log.Printf("[tapesListWithFreeSpace] tape %d freespace %d", tapeNumber, tapeFreeSpace)
	}

	// log.Printf("[tapesListWithFreeSpace] %v", tapesList)
	return tapesList
}

// проверка есть ли файлы для backup'а
func isFilesToBackup() bool {
	var name string
	err := db.QueryRow("SELECT name FROM root_index WHERE needtobackup = 1 AND type = 0 AND deleted = 0 AND mtime < NOW() - INTERVAL ? SECOND LIMIT 1", config.timeBeforeBackup).Scan(&name)
	if err != nil {
		// checkerr(err)
		return false
	} else {
		return true
	}
}

// снятие с файлов флага needtobackup, если количество его копий равно или больше установленного
func sqlUpdateNeedToBackupFlags() {
	files := make(map[uint64]uint)
	var uid uint64
	var count uint

	rows, err := db.Query(`SELECT file_uid, COUNT(tape_no) AS count FROM files_location WHERE deleted = 0 AND todelete = 0 AND broken = 0 GROUP BY file_uid`)
	checkerr(err)
	defer rows.Close()

	for rows.Next() { // перебор строк результата запроса
		if err := rows.Scan(&uid, &count); err != nil {
			log.Fatal(err)
		}
		files[uid] = count
	}

	for uid, count := range files {
		if count >= config.numberOfCopies {
			_, err := db.Exec("UPDATE root_index SET needtobackup = 0 WHERE uid = ?", uid)
			checkerr(err)
		}
	}
}

// очистка кэша
func cacheClear() {
	// log.Printf("[cacheClear]")
	var cacheFiles []uint64
	var isToDelete uint
	files, err := ioutil.ReadDir(config.cachePath)
	checkerr(err)
	for _, file := range files {
		if !file.IsDir() {
			cacheFiles = append(cacheFiles, uint64(string2uint(file.Name())))
		}
	}
	// log.Printf("[cacheClear] len cacheFiles %d", len(cacheFiles))
	for _, uid := range cacheFiles {
		// log.Printf("[cacheClear] query for uid %d", uid)
		row := db.QueryRow(`SELECT uid FROM root_index WHERE type = 0 AND needtobackup = 0 AND atime < NOW() - INTERVAL ? SECOND AND uid = ?`, config.cacheTimeLive, uid)
		// log.Printf(`SELECT uid FROM kts.index WHERE type = 0 AND needtobackup = 0 AND atime < NOW() - INTERVAL %d SECOND AND uid = %d`, config.cacheTimeLive, uid)
		err := row.Scan(&isToDelete)
		if err == sql.ErrNoRows {
			isToDelete = 0
		} else {
			checkerr(err)
		}
		if isToDelete != 0 {
			err = os.Remove(config.cachePath + uint2string(uid))
			checkerr(err)
			log.Printf("[cacheClear] uid %d in cache deleted", uid)
		}
	}
	// log.Printf("[cacheClear] finished")
}

// чтение в кэш
func readToCache() bool {
	// log.Printf("[readToCache] started")
	for {
		if len(filesToRead) != 0 {
			log.Printf("[readToCache] %v", filesToRead)
			// log.Printf("[readToCache] first %d", filesToRead[0:1])

			// узнаем на каких кассетах записан первый запрашиваемый файл
			rows, err := db.Query(`SELECT tape_no FROM files_location WHERE deleted = 0 AND todelete = 0 AND broken = 0 AND file_uid = ?`, filesToRead[0])
			if err == sql.ErrNoRows {
				// log.Printf("[readToCache] asked the files, which placed on broken tapes")
				return false
			} else {
				checkerr(err)
			}

			var tapeNoSlice []uint64 // кассеты, на которых хранится первый файл
			var tapeNoSliceItem uint64
			for rows.Next() {
				err = rows.Scan(&tapeNoSliceItem)
				checkerr(err)
				tapeNoSlice = append(tapeNoSlice, tapeNoSliceItem)
			}

			// запрашиваем номер кассеты, на котором больше всего файлов
			reqAddTapes := uintSliceToString(tapeNoSlice)
			reqAddFiles := uintSliceToString(filesToRead)
			req := fmt.Sprintf("SELECT tape_no, COUNT(file_uid) AS summ FROM files_location WHERE deleted = 0 AND broken = 0 AND todelete = 0 AND file_uid IN (%s) AND tape_no IN (%s) GROUP BY tape_no ORDER BY summ DESC LIMIT 1", reqAddFiles, reqAddTapes)

			var tapeToRead, nullVar uint64
			err = db.QueryRow(req).Scan(&tapeToRead, &nullVar)
			if err == sql.ErrNoRows {
				// файла нет на кассете
				log.Printf("[readToCache] !!! file %d is not on tape", filesToRead[0])
				break // на будущее - выход из цикла чтения
			} else {
				checkerr(err)
			}

			// формирование map'а читаемых файлов map[uint64][]uint64{} - map[номер пакета][]{uid файлов}
			req = fmt.Sprintf("SELECT file_uid, tape_record_no FROM files_location WHERE deleted = 0 AND todelete = 0 AND broken = 0 AND tape_no = %d AND file_uid IN (%s) ORDER BY tape_record_no ASC", tapeToRead, reqAddFiles)
			// log.Printf("[readToCache] req %s", req)

			rows, err = db.Query(req)
			checkerr(err)

			filesToReadFromTape := make(map[uint64][]uint64) // карта для списка файлов на чтение с конкретной кассеты
			var sqlResultUid uint64                          // переменные для чтения из результата sql запроса
			var sqlResultRecordNo uint64                     // переменные для чтения из результата sql запроса
			for rows.Next() {
				err = rows.Scan(&sqlResultUid, &sqlResultRecordNo)
				checkerr(err)
				filesToReadFromTape[sqlResultRecordNo] = append(filesToReadFromTape[sqlResultRecordNo], sqlResultUid)
				// log.Printf("[readToCache] add %d to block %d", sqlResultUid, sqlResultRecordNo)
			}
			log.Printf("[readToCache] filesToReadFromTape %v", filesToReadFromTape)

			if tapeLoad(tapeToRead) { // загрузка кассеты
				log.Printf("[readToCache] tape %d load", tapeToRead)
				if tapeReadBlocks(filesToReadFromTape, tapeToRead) { // чтение файлов с кассеты
					// log.Printf("[readToCache] error during read on tape %d block %v", tapeToRead, filesToReadFromTape)
					// здесь видимо надо пометить кассету как ошибочную для исключения из цикла до разбирательства
				} else { // возникла ошибка при чтении всего блока, кассета помечается как сбойная, все файлы на ней - для дублирования
					log.Printf("[readToCache] error during rewind on tape %d to block %v", tapeToRead, filesToReadFromTape)
					updateTapeSetToDuplicate(tapeToRead) // пометить кассету как ошибочную для исключения из цикла до разбирательства и отметка содержащихся на ней файлов для создания их дополнительной копии
					break
				}
			} else {
				log.Printf("[readToCache] tape did not load")
				break
			}

			if !tapeUnload(tapeToRead) { // при серьезной ошибке с выгрузкой кассеты выход из подпрограммы
				return false
			}
		} else {
			break // выход из цикла чтения
		}
	}
	return true
}

// uint slice to string
func uintSliceToString(slice []uint64) string {
	var IDs []string
	for _, i := range slice {
		IDs = append(IDs, uint2string(i))
	}
	return strings.Join(IDs, ", ")
}

// чтение сигнала о закрытии goрутины
func chanWatcher4goMakeBackup(chanShutdownSchedulerInit chan bool) bool {
	select {
	case shutdownSignal := <-chanShutdownSchedulerInit:
		if shutdownSignal { // выход, если получен сигнал на прекращение работы
			// toshutdown = true
			log.Printf("[chanWatcher4goMakeBackup] got shutdown signal")
			return true
		}
	default:
		return false
	}
	return false
}

// tape
package main

import (
	"archive/tar"
	"bufio"

	// "strconv"

	// "encoding/binary"
	// "context"
	"io"
	"log"
	"os"
	"os/exec"
	"sort"
	"strings"
	"time"

	// "github.com/mitchellh/ioprogress"
	// "github.com/machinebox/progress"
	// "github.com/schollz/progressbar"
	"github.com/cheggaaa/pb"
)

var tapeCurrentPosition uint64

// запись блоков на кассету
func tapeWriteBlocks(blocks map[uint64][]sqlPackageToTapeStruct, lastRecordNo uint64, tapeNo uint64) bool {
	recordNoOfBlock := make(map[uint64]uint64) // номер записанного блока каждого файла map[uid]recordableBlockNo
	var err error

	// log.Printf("[tapeWriteBlocks] last record No: %d", lastRecordNo)
	log.Printf("[tapeWriteBlocks] перемотка в конец блока %d для записи начиная с блока %d", lastRecordNo, lastRecordNo+1)
	if tapeRewindToBlock(lastRecordNo + 1) { // перемотка в конец ленты
		// tapeCurrentPosition = lastRecordNo + 1

		// формирование порядка перебора блоков для записи, внимание, здесь делается преобразование общепрограммного типа номера блока с uint64 на текущий для сортировки int
		sortedKeys := make([]int, 0, len(blocks))
		for k := range blocks {
			sortedKeys = append(sortedKeys, int(k))
		}
		sort.Ints(sortedKeys)

		// for blockNo, files := range blocks {
		// log.Printf("[tapeWriteBlocks] blocks order %v", sortedKeys)
		// log.Printf("[tapeWriteBlocks] blocks content: %v", blocks)
		for _, blockNo := range sortedKeys {
			// log.Printf("[tapeWriteBlocks] for blockNo: %d", blockNo)

			files := blocks[uint64(blockNo)]
			// log.Printf("[tapeWriteBlocks] files in block: %v", files)

			recordableBlockNo := lastRecordNo + uint64(blockNo) + 1 // делаем смещение на единицу, т.к. блоки в слайсе считаются от 0, а позиция на пленке равна последней записи
			log.Printf("[tapeWriteBlocks] write tape %d block %d lastrecordno %d", tapeNo, recordableBlockNo, lastRecordNo)

			var blockWriteHandler *os.File
			blockSize := calculateBlockSize(files) // вычисляем размер блока
			bar := pb.New(blockSize).SetUnits(pb.U_BYTES)
			bar.ShowTimeLeft = true
			bar.ShowSpeed = true
			bar.Start()

			if config.testMode {
				blockWriteHandler, err = os.Create(config.tempTapeStoragePath + uint2string(tapeNo) + "/" + uint2string(recordableBlockNo)) // создаем файл блока
				checkErrWithComment(err, "[tapeWriteBlocks] error while file in testmode for writing")
				// checkerr(err)
			} else {
				// infoTapeStatus()                                     // вывод текущий статус устройства
				blockWriteHandler, err = os.Create(config.driveName) // открываем ленточное устройство
				checkErrWithComment(err, "[tapeWriteBlocks] error while open tape device for writing")
				// checkerr(err)
			}
			blockWriteHandlerBar := io.MultiWriter(blockWriteHandler, bar)

			// blockWriteHandlerBuffer := bufio.NewWriter(blockWriteHandlerBar) // создаем буфер записи файла блока
			blockWriteHandlerBuffer := bufio.NewWriterSize(blockWriteHandlerBar, config.bufioSize) // создаем буфер записи файла блока

			tarWriteHandler := tar.NewWriter(blockWriteHandlerBuffer)
			for _, block := range files {
				// log.Printf("[tapeWriteBlocks] write file '%s' in block %d", block.name, recordableBlockNo)
				tarWrite(tarWriteHandler, block.uid, block.size)
				recordNoOfBlock[block.uid] = recordableBlockNo // записываем в map номер блока, в который записан файл
				// log.Printf("[tapeWriteBlocks] successfull")
			}
			tarWriteIndex(tarWriteHandler, files, tapeNo, recordableBlockNo) // записываем в конец архива индекс с его содержимым

			err = blockWriteHandlerBuffer.Flush()
			checkerr(err)
			// blockWriteHandlerBuffer.Reset(blockWriteHandler)
			blockWriteHandler.Close()
			bar.Finish()
			// infoTapeStatus() // вывод текущий статус устройства

			tapeCurrentPosition = recordableBlockNo + 1 // обновляем информацию о текущей позиции на ленте
		}
		SqlPutFilePlace(blocks, lastRecordNo, tapeNo, recordNoOfBlock)
		// log.Printf("[tapeWriteBlocks] recordNoOfBlock %v", recordNoOfBlock)
	} else {
		log.Printf("[tapeWriteBlocks] problem while rewind to end")
		return false
	}
	return true
}

// чтение блоков с кассеты
func tapeReadBlocks(blocks map[uint64][]uint64, tapeNo uint64) bool {
	log.Printf("[tapeReadBlocks] started")
	// log.Printf("[tapeReadBlocks] in: %v", blocks)
	readResult := make(map[uint64]bool)       // карта для отметки результатов чтения файлов, неудачные надо помечать в index'е на tobackup
	readResultTape := make(map[uint64]uint64) // карта для указания номера кассеты к прочитанным файлам

	// формирование порядка перебора блоков для записи, внимание, здесь делается преобразование общепрограммного типа номера блока с uint64 на текущий для сортировки int
	sortedKeys := make([]int, 0, len(blocks))
	for k := range blocks {
		sortedKeys = append(sortedKeys, int(k))
	}
	sort.Ints(sortedKeys)
	log.Printf("[tapeReadBlocks] blocks order %v", sortedKeys)

	for _, blockNo := range sortedKeys {
		// for recordNo, files := range blocks {
		recordNo := uint64(blockNo)
		files := blocks[recordNo]

		log.Printf("[tapeReadBlocks] preparing to get block %d, files %v", recordNo, files)
		if tapeRewindToBlock(recordNo) {
			log.Printf("[tapeReadBlocks] rewind successfull")
			if config.testMode {
				if !fileExists(config.tempTapeStoragePath + uint2string(tapeNo) + "/" + uint2string(recordNo)) { // файл не существует
					log.Printf("[tapeReadBlocks] could not open file %s", config.tempTapeStoragePath+uint2string(tapeNo)+"/"+uint2string(recordNo))
					return false
				}
			}

			var blockReadHandler *os.File
			var err error
			// var osOpenStr string
			if config.testMode {
				// osOpenStr = config.tempTapeStoragePath + uint2string(tapeNo) + "/" + uint2string(recordNo) // строка для открытия файла блока
				blockReadHandler, err = os.Open(config.tempTapeStoragePath + uint2string(tapeNo) + "/" + uint2string(recordNo)) // открываем файл блока
				checkErrWithComment(err, "[tapeReadBlocks] error while opening file to read in testmode")
				// checkerr(err)
			} else {
				// osOpenStr = config.driveName
				blockReadHandler, err = os.Open(config.driveName) // открываем файл блока
				// blockReadHandler, err = os.OpenFile(config.driveName, os.O_RDONLY, 0666)
				checkErrWithComment(err, "[tapeReadBlocks] error while opening tape device to read")
				// checkerr(err)
			}
			// blockReadHandler, err := os.Open(osOpenStr) // открываем файл блока
			// checkErrWithComment(err, "[tapeReadBlocks] error while opening device/file to read")

			blockReadHandlerBuffer := bufio.NewReaderSize(blockReadHandler, config.bufioSize) // создаем буфер чтения файла блока
			// blockReadHandlerBuffer := bufio.NewReader(blockReadHandler) // создаем буфер чтения файла блока
			tarReader := tar.NewReader(blockReadHandlerBuffer)
			// tarReader := tar.NewReader(blockReadHandler)

			for {
				header, err := tarReader.Next()
				if err == io.EOF || header.Name == "index.txt" || len(files) == 0 { // закрываем работу с архивом, если достигли конца файла или конца архива (последний файл - index.txt). Ввиду неправильной обработки конца записи на кассете отслеживаем конец архива по последнему файлу
					break
				}
				// if header.Name != "index.txt" {
				if checkInSlice(files, string2uint64(header.Name)) {
					files = removeFromSliceByContent(files, string2uint64(header.Name))
					log.Printf("[tapeReadBlocks] recordNo: %d file: %s operate", recordNo, header.Name)
					if tarRead(tarReader, header.Name, header.Size) { // извлечение файла из архива
						readResult[string2uint64(header.Name)] = true // отметка об успешном извлечении
						log.Printf("[tapeReadBlocks] file read successfully: %d", uint64(string2uint(header.Name)))
					} else {
						readResult[string2uint64(header.Name)] = false // отметка о неудаче
						log.Printf("[tapeReadBlocks] file not found: %d", string2uint64(header.Name))
					}
					readResultTape[string2uint64(header.Name)] = tapeNo // отметка номера кассеты, где был прочитан файл
				} else {
					log.Printf("[tapeReadBlocks] recordNo: %d file: %s skip", recordNo, header.Name)
				}
				// }
			}

			// blockReadHandlerBuffer.Reset(blockReadHandler)
			blockReadHandler.Close()
			// для избежание перепробега не производим перемотку до конца блока
			// tapeRewindToEndOfRecord() // поскольку возможно досрочное окончание чтения, то дается команда на проматывание ленты до конца блока
			// если есть файлы, которые не оказались в пакете, то отмечаем их как false
			for _, v := range files {
				if _, ok := readResult[v]; !ok {
					readResult[v] = false
					readResultTape[v] = tapeNo
				}
			}
			// т.к. возможно досрочное прерывание чтения блока, то счетчик позиции не увеличиваем
			// для избежание перепробега не производим перемотку до конца блока
			// tapeCurrentPosition++ // обновляем информацию о текущей позиции на ленте
		} else {
			log.Printf("[tapeReadBlocks] error during rewind to block %d", recordNo)
			return false
		}
	}

	updateFilesOpen(readResult, readResultTape) // исключение уже прочитанных в кэш файлов, отмечаем файлы с ошибкой чтения флагом needtobackup

	// при чтении надо обновить флаг atime
	return true
}

// чтение файла из tarHandler
func tarRead(tarReader *tar.Reader, uid string, size int64) bool {
	log.Printf("[tarRead] uid: %s", uid)

	bar := pb.New(int(size)).SetUnits(pb.U_BYTES)
	bar.ShowTimeLeft = true
	bar.ShowSpeed = true
	bar.Start()

	fw, err := os.Create(config.cachePath + uid)
	checkErrWithComment(err, "[tarRead] create file in cache")
	fwBar := io.MultiWriter(fw, bar)
	// checkerr(err)
	fw_buff_handle := bufio.NewWriterSize(fwBar, config.bufioSize)
	// fw_buff_handle := bufio.NewWriter(fw)
	summ, err := io.Copy(fw_buff_handle, tarReader)
	_ = summ
	checkErrWithComment(err, "[tarRead] io.Copy operation")

	err = fw_buff_handle.Flush()
	if err != nil {
		checkErrWithComment(err, "[tarRead] Flush write handle")
		// checkerr(err)
		return false
	}
	// log.Printf("[tarRead] wrote %d", summ)
	// fw_buff_handle.Reset(fw)
	fw.Close()
	bar.Finish()
	return true
}

// выгрузка кассеты на полку
func tapeUnload(tapeNumber uint64) bool {
	// запрос позиции кассеты
	var tapePosX, tapePosY uint
	err := db.QueryRow("SELECT `pos_x`, `pos_y` FROM tapes WHERE `tape_no` = ?", tapeNumber).Scan(&tapePosX, &tapePosY)
	if err != nil {
		log.Println("[tapeLoad] error during SQL request of X/Y position")
		return false
	}
	tapeCurrentPosition = 0
	return cmdUnLoad(tapePosX, tapePosY)
}

// загрузка кассеты, проверка ее номера
func tapeLoad(tapeNumber uint64) bool {
	// запрос позиции кассеты
	var tapePosX, tapePosY uint
	err := db.QueryRow("SELECT `pos_x`, `pos_y` FROM tapes WHERE `tape_no` = ?", tapeNumber).Scan(&tapePosX, &tapePosY)
	if err != nil {
		log.Println("[tapeLoad] error during SQL request of X/Y position")
		return false
	}

	// загрузка кассеты в привод
	if cmdLoad(tapePosX, tapePosY) {
		log.Println("[tapeLoad] cmd to load tape: ", tapeNumber)
		_, err := db.Exec("UPDATE tapes SET load_counter = load_counter + 1 WHERE tape_no = ?", tapeNumber)
		checkerr(err)
	} else {
		log.Println("[tapeLoad] mechanism error")
		return false
	}

	// проверка инициализации кассеты и ее инициализация, если пустая
	if config.testMode {
		if _, err := os.Stat(config.tempTapeStoragePath + uint2string(tapeNumber)); os.IsNotExist(err) {
			tapeInit(tapeNumber) // ВНИМАТЕЛЬНО, данный параметр должен быть ручным
			log.Printf("[tapeLoad] tape %d not initialized", tapeNumber)
			return false
		}
	}

	// проверка заголовка и индекса кассеты
	// tapeRewindToBlock(0) // перемотка на первую запись
	tapeRewindToBegin() // перемотка на первую запись
	var r *os.File
	if config.testMode {
		r, err = os.Open(config.tempTapeStoragePath + uint2string(tapeNumber) + "/0")
		checkerr(err)
	} else {
		r, err = os.Open(config.driveName)
		checkerr(err)
	}

	rb := bufio.NewReader(r)
	readbuf := make([]byte, 128)
	// var readbuflen int
	readbuf, _, err = rb.ReadLine()
	// readbuflen = len(readbuf)
	checkerr(err)
	// _, err = io.ReadAtLeast(r, readbuf, 13)
	// if err != nil {
	// 	log.Println("[tapeLoad] error during read tape header: %v", err)
	// 	// !!!!! не забыть убрать, ручная инициализация
	// 	tapeInit(tapeNumber)
	// 	return false
	// }
	r.Close()
	// log.Printf("[tapeLoad] %v %d %d", string(readbuf), len(readbuf)) //, readbuflen)
	if (len(string(readbuf)) != 13) || (err != nil) {
		log.Println("[tapeLoad] error during read tape header")
		// !!!!! не забыть убрать, ручная инициализация
		tapeInit(tapeNumber)
		return false
	}

	// log.Printf("[tapeLoad] string: %v clean: %v", string(readbuf), readbuf)
	if string2uint64(string(readbuf)[3:13]) != tapeNumber {
		log.Println("[tapeLoad] error in tape header, incorrect tape number")
		return false
	}
	if string(readbuf)[0:3] != "KTS" {
		log.Println("[tapeLoad] error in tape header, incorrect tape header")
		return false
	}
	tapeCurrentPosition = 1 // текущая позиция на ленте
	return true
}

// команда механизму на загрузку кассеты
func cmdLoad(tapePosX uint, tapePosY uint) bool {
	log.Printf("[cmdLoad] posX: %d  posY: %d", tapePosX, tapePosY)
	if !config.testMode {
		// cmdOut, err := exec.Command("mt", "-f", config.driveName, "status").Output()
		// log.Printf("[cmdLoad] cmd output: %s", cmdOut)
		// checkerr(err)
	}
	return true
}

// команда механизму на загрузку кассеты
func cmdUnLoad(tapePosX uint, tapePosY uint) bool {
	log.Printf("[cmdUnLoad] posX: %d  posY: %d", tapePosX, tapePosY)
	return true
}

// инициализация кассеты, запись первой метки
func tapeInit(tapeNumber uint64) {
	log.Printf("[tapeInit]")
	var err error
	var r *os.File

	if config.testMode {
		dir := config.tempTapeStoragePath + uint2string(tapeNumber)
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			err = os.MkdirAll(dir, 0755)
			if err != nil {
				panic(err)
			}
		}

		// создание файла с меткой
		r, err = os.Create(dir + "/0")
		checkerr(err)
	} else {
		// tapeRewindToBlock(0)
		tapeRewindToBegin()
		// открытие устройства для записи метки
		r, err = os.Create(config.driveName)
		checkerr(err)
	}

	// запись заголовка
	_, err = r.Write([]byte("KTS"))
	checkerr(err)

	// запись номера кассеты
	// b := make([]byte, 10)
	// binary.LittleEndian.PutUint64(b, tapeNumber)
	_, err = r.Write([]byte(uint2string10(tapeNumber)))
	checkerr(err)

	r.Close()
	tapeCurrentPosition = 1
}

// запись файла в tarHandler
func tarWrite(tarWriteHandler *tar.Writer, uid uint64, size uint64) {
	createIfExistMapOpenFile(uid) // проверка и создания map'а отслеживания состояния файла, если его не существует
	inputFileHandler, err := os.Open(config.cachePath + uint2string(uid))
	checkErrWithComment(err, "[tarWrite] error while opening cache file")
	// checkerr(err)
	// defer inputFileHandler.Close()

	// обновляем статус файла на чтение
	filesOpenMutex.Lock()
	filesOpen[uid].pipes++
	filesOpen[uid].reading = true // обновляем в списке открытых файлов
	filesOpenMutex.Unlock()

	inputFileHandlerBuffer := bufio.NewReaderSize(inputFileHandler, config.bufioSize)
	// inputFileHandlerBuffer := bufio.NewReaderSize(inputFileHandler, config.bufioSize)
	// inputFileHandlerBuffer := bufio.NewReader(inputFileHandler)

	// buffer := make([]byte, 128*1024)

	hdr := &tar.Header{
		// Name: name,
		Name: uint2string(uid),
		Mode: 0600,
		Size: int64(size),
	}
	err = tarWriteHandler.WriteHeader(hdr)
	checkErrWithComment(err, "[tarWrite] error in TAR write header")
	// checkerr(err)

	// _, _ = io.Copy(tw.Write(), r)

	// for {
	// 	readBytes, err := inputFileHandlerBuffer.Read(buffer)
	// 	if err == io.EOF || readBytes == 0 {
	// 		break
	// 	} else {
	// 		checkerr(err)
	// 	}
	// 	_, errw := tarWriteHandler.Write(buffer[:readBytes])
	// 	checkerr(errw)
	// }
	// var s int

	_, err = io.Copy(tarWriteHandler, inputFileHandlerBuffer)
	// _, err = dataCopy(tarWriteHandler, inputFileHandlerBuffer, size)

	checkErrWithComment(err, "[tarWrite] error while copy")

	err = tarWriteHandler.Flush()
	checkErrWithComment(err, "[tarWrite] error while flush buffer")

	filesOpenMutex.Lock()
	filesOpen[uid].pipes-- // обновляем счетчик открытых потоков файла

	if filesOpen[uid].pipes == 0 {
		// inputFileHandlerBuffer.Reset(inputFileHandler)
		inputFileHandler.Close() // закрытие файла
		delete(filesOpen, uid)   // удаляем неиспользуемую отметку
	} else {
		filesOpen[uid].backup = false // снятие ограничивающих статусов файла
	}
	filesOpenMutex.Unlock()
	// log.Printf("[tarWrite] filesOpen %v", filesOpen[uid])

}

// запись индекса блока в tarHandler
func tarWriteIndex(tarWriteHandler *tar.Writer, files []sqlPackageToTapeStruct, tapeNo uint64, recordableBlockNo uint64) {
	// log.Printf("[tarWriteIndex] files: %v", files)
	var data string
	data = "tapeNo: " + uint2string(tapeNo) + "\t blockNo: " + uint2string(recordableBlockNo) + "\n"
	for _, v := range files {
		values := []string{}
		values = append(values, uint2string(v.uid))
		values = append(values, v.name)
		data = data + strings.Join(values, "\t") + "\n"
	}

	hdr := &tar.Header{
		// Name: name,
		Name: "index.txt",
		Mode: 0600,
		Size: int64(len(data)),
	}
	err := tarWriteHandler.WriteHeader(hdr)
	checkErrWithComment(err, "[tarWriteIndex] error while index file header write")
	// checkerr(err)
	_, err = tarWriteHandler.Write([]byte(data))
	checkErrWithComment(err, "[tarWriteIndex] error while write index file")
	// checkerr(err)
	err = tarWriteHandler.Flush()
	checkErrWithComment(err, "[tarWriteIndex] error while flush buffer of index file")
	// checkerr(err)
}

// добавление информации о месте записи файла в базу данных
func SqlPutFilePlace(blocks map[uint64][]sqlPackageToTapeStruct, lastRecordNo uint64, tapeNo uint64, recordNoOfBlock map[uint64]uint64) {
	var recordType bool
	// log.Printf("[SqlPutFilePlace] %v", recordNoOfBlock)

	for _, files := range blocks {
		if len(files) > 1 {
			recordType = true
		} else {
			recordType = false
		}
		for _, block := range files {
			// log.Printf("[SqlPutFilePlace] tape %d, uid %d, tape record No %d, record type %t, size, %d, mtime %s", tapeNo, block.uid, recordNoOfBlock[block.uid], recordType, block.size, block.mtime)
			_, err := db.Exec("INSERT INTO files_location(`tape_no`, `file_uid`, `tape_record_no`, `saved`, `record_type`, `deleted`, `todelete`, `size`, `mtime`) VALUES(?, ?, ?, CURRENT_TIMESTAMP(), ?, 0, 0, ?, ?)", tapeNo, block.uid, recordNoOfBlock[block.uid], recordType, block.size, block.mtime)
			checkerr(err)
			// cleanSingleFlagBackup(block.uid) // снятие с файла флага backup, освобождение для работы
		}

	}
}

// промотка ленты до конца записи
func tapeRewindToEndOfRecord() {
	log.Printf("[tapeRewindToEndOfRecord] rewind to the end of the record")
	if !config.testMode {
		cmdOut, err := exec.Command("mt", "-f", config.driveName, "fsf").Output()
		checkerr(err)
		if cmdOut != nil {
			log.Printf("[tapeRewindToEndOfRecord] %s", cmdOut)
		}
	}
}

// перемотка кассеты к определенной записи
func tapeRewindToBlock(recordNo uint64) bool {
	log.Printf("[tapeRewindToBlock] rewind to position %d, currentPos %d", recordNo, tapeCurrentPosition)
	if !config.testMode {
		// if recordNo != tapeCurrentPosition { // не делать перемотку, если запрашиваемая позиция является текущей, отключено, т.к. возможно прерывание чтения записи
		cmdOut, err := exec.Command("mt", "-f", config.driveName, "asf", uint2string(recordNo)).Output()
		if cmdOut != nil {
			log.Printf("[tapeRewindToBlock] cmd output: %s", cmdOut)
		}
		checkErrWithComment(err, "[tapeRewindToBlock] error while exec")
		// } else {
		// 	log.Printf("[tapeRewindToBlock] tape at the position")
		// }
	}
	tapeCurrentPosition = recordNo
	return true
}

// перемотка в конец кассеты
func tapeRewindToEnd() bool {
	log.Printf("[tapeRewindToEnd] rewind to the end of the tape")
	if !config.testMode {
		// infoTapeStatus()
		cmdOut, err := exec.Command("mt", "-f", config.driveName, "eom").Output()
		checkerr(err)
		if cmdOut != nil {
			log.Printf("[tapeRewindToEnd] %s", cmdOut)
		}
		// infoTapeStatus()
		// timeout(3)
	}
	return true
}

// перемотка в начало кассеты
func tapeRewindToBegin() bool {
	log.Printf("[tapeRewindToBegin] rewind to the start of the tape")
	if !config.testMode {
		// infoTapeStatus()
		cmdOut, err := exec.Command("mt", "-f", config.driveName, "rewind").Output()
		checkerr(err)
		if cmdOut != nil {
			log.Printf("[tapeRewindToBegin] %s", cmdOut)
		}
		// infoTapeStatus()
		// timeout(3)
	}
	return true
}

// текущий статус устройства
func infoTapeStatus() {
	cmdOut, err := exec.Command("mt", "-f", config.driveName, "status").Output()
	checkErrWithComment(err, "[infoTapeStatus] error trying to exec mt status command")
	log.Printf("[infoTapeStatus] cmd output:\n%s", cmdOut)
	timeout(3)
}

// timeout 1s
func timeout(howlong int) {
	time.Sleep(time.Duration(howlong) * time.Second)
}

// copy function
func CopyBuffer(dst io.Writer, src io.Reader, buf []byte) (written int64, err error) {
	nr, err := src.Read(buf)
	if nr == 0 {
		return 0, err
	}
	nw, ew := dst.Write(buf[0:nr])
	if ew == nil && nw != nr {
		ew = io.ErrShortWrite
	}
	if ew != nil {
		err = ew
	}
	return int64(nw), err
}
func dataCopy(dst io.Writer, src io.Reader, size uint64) (written int64, err error) {
	log.Printf("[dataCopy]")
	// If the writer has a ReadFrom method, use it to to do the copy.
	// Avoids an allocation and a copy.
	if rt, ok := dst.(io.ReaderFrom); ok {
		log.Printf("h")
		return rt.ReadFrom(src)
	}
	// Similarly, if the reader has a WriteTo method, use it to to do the copy.
	if wt, ok := src.(io.WriterTo); ok {
		log.Printf("f")
		return wt.WriteTo(dst)
	}
	buf := make([]byte, config.bufioSize)
	var writtenInPerc float64
	for err == nil {
		writtenInPerc = (float64(written) / float64(size)) * 100
		log.Printf("[dataCopy] %0.0f%%", writtenInPerc)
		var nc int64
		nc, err = CopyBuffer(dst, src, buf)
		log.Printf("[dataCopy] in progress %d bytes", written)
		log.Print(".")
		written += nc
	}
	if err == io.EOF {
		log.Printf("e")

		err = nil
	}
	return written, err
}

// суммируем размер файлов в блоке
func calculateBlockSize(files []sqlPackageToTapeStruct) int {
	var summOfFiles int
	for _, block := range files {
		summOfFiles = summOfFiles + int(block.size)
	}
	return summOfFiles
}

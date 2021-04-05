package main

import (
	// "io"
	"io"
	"log"
	"os"
	"strconv"
	"sync"

	// "time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"

	// "bazil.org/fuse/fuseutil"
	"golang.org/x/net/context"
)

type File struct {
	Node
	mu   sync.Mutex
	file *os.File
	size uint64
	// pipes uint
	// blockForWrite bool
	// handlerSum uint
	// writers uint
	// data []byte
}

// возврат свойств файла
func (f *File) Attr(ctx context.Context, a *fuse.Attr) error {
	// log.Printf("[FileAttr] requested Attr for File '%s' with id '%d'", f.name, f.inode)
	content := SqlGetFilePropertiesByINode(f.inode)
	// f.mu.Lock()
	// defer f.mu.Unlock()
	a.Inode = f.inode
	if checkInCache(f.inode, false) {
		a.Mode = 0766 // файл в кэше
	} else {
		a.Mode = 0666 // файл на кассетах
	}
	a.Size = content.size
	a.Mtime = timestamp2time(content.mtime) // модифицирован
	a.Atime = timestamp2time(content.atime) // доступ
	a.Ctime = timestamp2time(content.ctime) // изменен
	return nil
}

// Open
var _ = fs.NodeOpener(&File{})

type FileHandle struct {
	r *os.File
	n *File
}

var _ fs.Handle = (*FileHandle)(nil)

func (f *File) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	log.Println("[Openfile] call on file", f.name)

	if !checkInCache(f.Node.inode, true) { // не дает открыть, если файла нет в кэше
		// return nil, fuse.EPERM
		return nil, fuse.EEXIST
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	createIfExistMapOpenFile(f.inode) // проверка и создания map'а под файл, если его не существует

	// ограничения по открытию
	if filesOpen[f.inode].writing == true { // запрет на открытие файла, если он открыт для записи
		// log.Println("[Openfile] blocked to write")
		return nil, fuse.ENOENT
	}
	if req.Flags == fuse.OpenWriteOnly && ((filesOpen[f.inode].pipes > 0) || (filesOpen[f.inode].backup == true)) { // запрет на открытие файла для записи, если он уже открыт для чтения или записывается на ленту
		// log.Println("[Openfile] already opened for read, can't write")
		return nil, fuse.ENOENT
	} else if req.Flags == fuse.OpenReadOnly && filesOpen[f.inode].writing == true { // запрет на открытие файла для чтения, если он уже открыт для записи
		// log.Println("[Openfile] already blocked to write, can't read")
		return nil, fuse.ENOENT
	} else if req.Flags == fuse.OpenWriteOnly && filesOpen[f.inode].pipes == 0 { // установка флага 'открыт для записи', если открыт для записи и больше нет открытий
		// log.Println("[Openfile] open for write")
		filesOpenMutex.Lock()
		filesOpen[f.inode].writing = true
		filesOpenMutex.Unlock()
	}

	r, err := os.OpenFile(config.cachePath+strconv.Itoa(int(f.inode)), os.O_CREATE|os.O_RDWR, 0666) // |os.O_TRUNC
	checkerr(err)
	filesOpenMutex.Lock()
	filesOpen[f.inode].reading = true // обновляем в списке открытых файлов
	filesOpen[f.inode].pipes++        // обновляем счетчик открытых потоков файла
	filesOpenMutex.Unlock()
	f.file = r
	return &FileHandle{r: r, n: f}, nil
}

// Read
var _ = fs.HandleReader(&FileHandle{})

func (fh *FileHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	// log.Printf("[handleRead] read file from position '%d' with size '%d'", req.Offset, req.Size)
	fh.n.mu.Lock()
	defer fh.n.mu.Unlock()

	resp.Data = make([]byte, cap(resp.Data))
	n, err := fh.r.ReadAt(resp.Data, req.Offset)
	if n != len(resp.Data) {
		resp.Data = resp.Data[:n]
	}
	if err == io.EOF {
		err = nil
	}
	return err
}

func (fh *FileHandle) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	// log.Printf("[handleWrite] write file '%s' from position '%d' with size '%d'", fh.n.name, req.Offset, len(req.Data))
	fh.n.mu.Lock()
	defer fh.n.mu.Unlock()
	var err error

	writtenSize, err := fh.r.WriteAt(req.Data, req.Offset)
	checkerr(err)
	fh.n.size = fh.n.size + uint64(writtenSize)
	return nil
}

func (f *File) Flush(ctx context.Context, req *fuse.FlushRequest) error {
	// log.Println("Flushing file", f.name)
	f.mu.Lock()
	defer f.mu.Unlock()
	return nil
}

func (fh *FileHandle) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	filesReleaseDeleteLock.Lock() // блокируем удаление/закрытие файлов, т.к. обращение к функции удаления происходит раньше, чем завершается его закрытие и обнуление флагов
	// log.Println("[Release] file", fh.n.name, fh.n.pipes)
	// 	снимать счетчик handle'ров
	// делать close только при количестве счетчиков равном 0
	fh.n.mu.Lock()
	defer fh.n.mu.Unlock()

	filesOpenMutex.Lock()
	filesOpen[fh.n.inode].pipes-- // обновляем счетчик открытых потоков файлов
	filesOpenMutex.Unlock()

	// log.Println("[Release]2 file", fh.n.name, fh.n.pipes)
	if filesOpen[fh.n.inode].pipes == 0 { // закрываем файл только в случае закрытия всех потоков
		// обновление в базе флагов изменения файлов
		if filesOpen[fh.n.inode].writing == true { // файл открывался для записи
			_, err := db.Exec("UPDATE root_index SET needtobackup = 1, mtime = CURRENT_TIMESTAMP(), atime = CURRENT_TIMESTAMP(), ctime = CURRENT_TIMESTAMP() WHERE uid = ?", fh.n.inode) // обновление дат открытия
			checkerr(err)
		} else { // файл открывался для чтения
			_, err := db.Exec("UPDATE root_index SET atime = CURRENT_TIMESTAMP() WHERE uid = ?", fh.n.inode)
			checkerr(err)
		}
		filesOpenMutex.Lock()
		filesOpen[fh.n.inode].writing = false // снимаем флаг на запись
		fh.n.file.Close()                     //закрываем файл
		filesOpen[fh.n.inode].reading = false // обновляем в списке открытых файлов
		if (filesOpen[fh.n.inode].reading == false) || (filesOpen[fh.n.inode].writing == false) || (filesOpen[fh.n.inode].backup == false) {
			delete(filesOpen, fh.n.inode) // удаляем неиспользуемую отметку
		}
		filesOpenMutex.Unlock()

	}
	SqlUpdateFileSize(fh.n.inode, fh.n.size)
	filesReleaseDeleteLock.Unlock() // снимаем удаление/закрытие файлов
	// log.Printf("[Release] %v", filesOpen[fh.n.inode])
	return nil
}

func (f *File) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	// log.Println("Fsync call on file", f.name)
	return nil
}

// проверка и создания map'а под файл, если его не существует
func createIfExistMapOpenFile(inode uint64) {
	if _, ok := filesOpen[inode]; !ok { // проверка и создания map'а под файл, если его не существует
		filesOpenMutex.Lock()
		filesOpen[inode] = &filesFlagsStruct{false, false, false, 0}
		filesOpenMutex.Unlock()
	}
}

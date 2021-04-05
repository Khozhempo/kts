package main

import (
	// "io"
	"log"
	"os"
	"strconv"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"golang.org/x/net/context" // need this cause bazil lib doesn't use syslib context lib
)

type Dir struct {
	Node
	files       *[]*File
	directories *[]*Dir
}

// возврат атрибутов папки по его имени и id
func (d *Dir) Attr(ctx context.Context, a *fuse.Attr) error {
	// log.Printf("[DirAttr] requested for directory '%s' with id '%d'", d.name, d.inode)
	a.Inode = d.inode
	a.Mode = os.ModeDir | 0666
	return nil
}

// возврат свойств объекта (файл, папка) по его имени и fatherids
func (d *Dir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	// log.Printf("[Lookup] requested for '%s' with fatherid '%d'", name, d.inode)

	content := SqlGetFileProperties(name, d.inode)
	if content.name == "" { // если пустой запрос, то возврат ошибки
		return nil, fuse.ENOENT
	} else if content.nameType == true { // если запрос папка
		s := &Dir{Node: Node{name: name, inode: content.uid}}
		return s, nil
	} else if content.nameType == false { // если запрос файл
		s := &File{Node: Node{name: name, inode: content.uid}, size: content.size}
		return s, nil
	}
	return nil, fuse.ENOENT
}

// возвращаем содержимое папки по его fatherd
func (d *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	// log.Printf("[ReadDirAll] reading content in '%s' with fatherid '%d'", d.name, d.inode)
	content := SqlGetFolderContent(d.inode)
	var children []fuse.Dirent
	for _, v := range content {
		if v.nameType == true {
			children = append(children, fuse.Dirent{Inode: v.uid, Type: fuse.DT_File, Name: v.name})
		} else if v.nameType == false {
			children = append(children, fuse.Dirent{Inode: v.uid, Type: fuse.DT_Dir, Name: v.name})
		}
	}
	return children, nil
}

// создание файл по его имени и fatherid
func (d *Dir) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	log.Printf("[Create] '%s' with fatherid '%d'", req.Name, d.inode)

	f := &File{}
	// проверка есть ли файл или папка с таким именем
	content := SqlGetFileProperties(req.Name, d.inode)
	var err error
	var w *os.File
	if content.name == "" {
		// если объекта такого нет
		// создаем запись с базе
		_, err := db.Exec("INSERT INTO root_index(`name`, `type`, fatherid, `size`, mtime, atime, ctime, needtobackup) VALUES(?, ?, ?, ?, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), 1)", req.Name, 0, d.inode, 0)
		checkerr(err)

		content := SqlGetFileProperties(req.Name, d.inode)
		f = &File{Node: Node{name: req.Name, inode: content.uid}, size: 0}
		w, err = os.OpenFile(config.cachePath+strconv.Itoa(int(content.uid)), os.O_CREATE|os.O_RDWR, 0666) // |os.O_TRUNC

		createIfExistMapOpenFile(content.uid) // проверка и создания map'а под файл, если его не существует
		// log.Printf("[Create] open with flags %v", req.Flags)
		if req.Flags == fuse.OpenWriteOnly+fuse.OpenCreate+fuse.OpenExclusive { // если открыт только для записи, то устанавливаем блокирующий флаг
			filesOpenMutex.Lock()
			filesOpen[f.inode].writing = true
			filesOpenMutex.Unlock()
			// f.blockForWrite = true
		}
		filesOpenMutex.Lock()
		filesOpen[f.inode].pipes++        // увеличиваем число открытых потоков на файл
		filesOpen[f.inode].reading = true // обновляем в списке открытых файлов
		filesOpenMutex.Unlock()
	} else {
		f.mu.Lock()
		defer f.mu.Unlock()

		createIfExistMapOpenFile(f.inode) // проверка и создания map'а под файл, если его не существует

		if filesOpen[f.inode].writing == true { // запрет на открытие файла, если он открыт для записи
			return nil, nil, fuse.ENOENT
		}
		// ограничения по открытию
		if req.Flags == fuse.OpenWriteOnly && ((filesOpen[f.inode].pipes > 0) || (filesOpen[f.inode].backup == true)) { // запрет на открытие файла для записи, если он уже открыт для чтения
			return nil, nil, fuse.ENOENT
		} else if req.Flags == fuse.OpenReadOnly && filesOpen[f.inode].writing == true { // запрет на открытие файла для чтения, если он уже открыт для записи или записывается на ленту
			return nil, nil, fuse.ENOENT
		} else if req.Flags == fuse.OpenWriteOnly+fuse.OpenCreate && filesOpen[f.inode].pipes == 0 { // установка флага 'открыт для записи', если открыт для записи и больше нет открытий
			// f.blockForWrite = true
			filesOpenMutex.Lock()
			filesOpen[f.inode].writing = true
			filesOpenMutex.Unlock()
		}
		w, err = os.OpenFile(config.cachePath+strconv.Itoa(int(content.uid)), os.O_CREATE|os.O_RDWR, 0666) //|os.O_TRUNC
		// filesOpen[f.inode] = true                                                                          // обновляем в списке открытых файлов
		filesOpenMutex.Lock()
		filesOpen[f.inode].pipes++        // обновляем количество открытых потоков
		filesOpen[f.inode].reading = true // обновляем в списке открытых файлов
		filesOpenMutex.Unlock()
	}
	checkerr(err)
	return f, &FileHandle{r: w, n: f}, nil
}

// удаление объекта файла/папки по имени и fatherid
func (d *Dir) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	filesReleaseDeleteLock.Lock() // блокируем удаление/закрытие файлов, т.к. обращение к функции удаления происходит раньше, чем завершается его закрытие и обнуление флагов
	log.Printf("[Remove] '%s' with fatherid '%d'", req.Name, d.inode)
	defer filesReleaseDeleteLock.Unlock() // снимаем удаление/закрытие файлов

	// проверка есть ли файл или папка с таким именем

	content := SqlGetFileProperties(req.Name, d.inode)
	createIfExistMapOpenFile(content.uid) // проверка и создания map'а под файл, если его не существует
	if content.name == "" {               // если объекта такого нет
		return fuse.ENOENT
	} else { // такой объект есть
		if req.Dir == false { // дополнительная проверка на то, что это файл
		}
		if _, ok := filesOpen[content.uid]; ok { // проверяем, что файл не открыт, т.е. на него есть флаг
			if (filesOpen[content.uid].reading == true) || (filesOpen[content.uid].writing == true) || (filesOpen[content.uid].backup == true) {
				return fuse.ENOENT
			}
		}

		_, err := db.Exec("UPDATE root_index SET deleted = 1 WHERE uid = ? AND `name` = ? AND `type` = ?", content.uid, content.name, content.nameType)
		checkerr(err)
		_, err = db.Exec("UPDATE files_location SET todelete = 1 WHERE file_uid = ?", content.uid) // обновление статуса записанных на кассету файлов
		checkerr(err)

		if fileExists(config.cachePath + strconv.Itoa(int(content.uid))) {
			err = os.Remove(config.cachePath + strconv.Itoa(int(content.uid)))
			checkerr(err)
		}
		if (filesOpen[content.uid].reading == false) || (filesOpen[content.uid].writing == false) || (filesOpen[content.uid].backup == false) {
			filesOpenMutex.Lock()
			delete(filesOpen, content.uid) // удаляем неиспользуемую отметку
			filesOpenMutex.Unlock()
		}
		return nil
	}
	return fuse.ENOENT
}

// создание папки по имени и fatherid
func (d *Dir) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	log.Printf("[Mkdir] request for '%s' with fatherid '%d'", req.Name, d.inode)

	dir := &Dir{}
	// проверка есть ли файл или папка с таким именем
	content := SqlGetFileProperties(req.Name, d.inode)
	if content.name == "" {
		// если объекта такого нет
		// создаем запись с базе
		_, err := db.Exec("INSERT INTO root_index(`name`, `type`, fatherid, `size`, mtime, atime, ctime) VALUES(?, ?, ?, ?, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())", req.Name, 1, d.inode, 0)
		checkerr(err)
		content := SqlGetFileProperties(req.Name, d.inode)

		dir = &Dir{Node: Node{name: req.Name, inode: content.uid}}
	}
	return dir, nil
}

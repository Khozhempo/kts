package main

import (
	// "log"

	"log"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"golang.org/x/net/context"
)

type FS struct {
	root *Dir
}

func (f *FS) Root() (fs.Node, error) {
	log.Printf("[Root]")
	return f.root, nil
}

func (f *FS) Statfs(ctx context.Context, req *fuse.StatfsRequest, res *fuse.StatfsResponse) error {
	// log.Printf("[Statfs]")
	var totalData uint64
	err := db.QueryRow("SELECT SUM(tape_format.capacity) FROM tapes tapes LEFT JOIN tape_format tape_format ON tapes.tape_format = tape_format.type WHERE tapes.broken = 0").Scan(&totalData)
	checkerr(err)
	// totalData = totalData / uint64(config.numberOfCopies)

	var occupiedData uint64
	// err = db.QueryRow("SELECT COALESCE(SUM(size),0) FROM kts.index WHERE deleted = 0").Scan(&occupiedData)
	err = db.QueryRow("SELECT COALESCE(SUM(size),0) FROM root_index WHERE deleted = 0").Scan(&occupiedData)
	checkerr(err)

	// res.Blocks = 100 // Total data blocks in file system.
	res.Bfree = 100 // Free blocks in file system.
	// res.Bavail = 1 << 34 // Free blocks in file system if you're not root.
	res.Files = 1 << 29 // Total files in file system.
	res.Ffree = 1 << 28 // Free files in file system.
	// res.Ffree = 1024000
	res.Namelen = 256      // Maximum file name length?
	res.Bsize = 128 * 1024 // Block size
	// res.Bsize = 49152
	// resp.Frsize = 1          // Fragment size, smallest addressable data size in the file system.
	res.Blocks = totalData / uint64(config.numberOfCopies) / uint64(res.Bsize)
	res.Bfree = (res.Blocks - (occupiedData/uint64(config.numberOfCopies))/uint64(res.Bsize))

	// log.Printf("[Statfs] %v", res)
	return nil
}

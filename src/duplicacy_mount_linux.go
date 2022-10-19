package duplicacy

import (
	"context"
	"crypto/sha1"
	"log"
	"os/user"
	"strconv"
	"sync"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	lru "github.com/hashicorp/golang-lru"
)

type BackupFS struct {
	fs.Inode
	fs.NodeOnAdder

	snapshots       map[int]*Snapshot
	manager         *BackupManager
	ino             uint64
	root            *node_t
	initedRevisions map[int]bool
	chunkCache      *lru.TwoQueueCache
	openmap         map[uint64]*node_t
	lock            sync.Mutex
}

func (self *BackupFS) OnAdd(ctx context.Context) {
	self.initRoot()

	queue := []NameBackupFileTuple{}

	for name, node := range self.root.chld {
		parent := self.Inode

		file := BackupFile{node: node}

		child := parent.NewPersistentInode(
			ctx, &file, fs.StableAttr{Mode: node.stat.Mode, Ino: node.stat.Ino})
		parent.AddChild(name, child, true)
		queue = append(queue, NameBackupFileTuple{name, &file})
	}

	for len(queue) > 0 {
		l := len(queue)
		curr := queue[l-1]
		queue = queue[:l-1]

		if len(curr.file.node.chld) == 0 {
			// revision root
			revision, err := getDirRevision(curr.name)
			if err != nil {
				continue
			}
			curr.file.revision = -revision
			continue
		}

		for name, node := range curr.file.node.chld {
			parent := curr.file.Inode

			file := BackupFile{node: node, backupFs: self}
			child := parent.NewPersistentInode(
				ctx, &file, fs.StableAttr{Mode: node.stat.Mode, Ino: node.stat.Ino})
			parent.AddChild(name, child, true)
			queue = append(queue, NameBackupFileTuple{name, &file})
		}
	}
}

type NameBackupFileTuple struct {
	name string
	file *BackupFile
}

type BackupFile struct {
	fs.Inode

	backupFs *BackupFS
	node     *node_t
	revision int
}

func (self *BackupFile) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	if self.revision < 0 {
		err := self.backupFs.addRevisionFiles(ctx, self)
		if err != nil {
			return ENOENT
		}
	}

	out.Ino = self.node.stat.Ino
	out.Mode = self.node.stat.Mode
	out.Nlink = self.node.stat.Nlink
	out.Uid = self.node.stat.Uid
	out.Gid = self.node.stat.Gid
	out.Size = uint64(self.node.stat.Size)
	out.Mtime = uint64(self.node.stat.Mtim)

	return 0
}

func (zf *BackupFile) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	return nil, 0, fs.OK
}

func (self *BackupFile) Read(ctx context.Context, fh fs.FileHandle, buff []byte, ofst int64) (fuse.ReadResult, syscall.Errno) {
	self.backupFs.lock.Lock()
	defer self.backupFs.lock.Unlock()

	if self.node.stat.Size == 0 {
		return fuse.ReadResultData([]byte{}), fs.OK
	}

	if self.node.chunkInfo == nil {
		return nil, ENOENT
	}

	snapshot := self.backupFs.snapshots[self.revision]

	readBytes, err := self.backupFs.readFileChunkCached(snapshot, self.node.chunkInfo, buff, ofst)
	if err != nil {
		LOG_INFO("MOUNTING_FILESYSTEM", "error reading file %s: %v", self.Path(&self.backupFs.Inode), err)
		return nil, syscall.EIO
	}
	LOG_INFO("MOUNTING_FILESYSTEM", "Read(%s, %d) -> %d, %x", self.Path(&self.backupFs.Inode), ofst, readBytes, sha1.Sum(buff[:readBytes]))
	return fuse.ReadResultData(buff), fs.OK
}

func (self *BackupFS) addRevisionFiles(ctx context.Context, file *BackupFile) (retErr error) {
	self.lock.Lock()
	defer self.lock.Unlock()

	inited, err := self.initRevision(-file.revision, file.Path(&self.Inode))
	if err != nil {
		retErr = err
		return
	}

	if !inited {
		return
	}

	queue := []*BackupFile{file}

	for len(queue) > 0 {
		l := len(queue)
		curr := queue[l-1]
		queue = queue[:l-1]

		for name, node := range curr.node.chld {
			parent := curr.Inode

			inode := BackupFile{node: node, backupFs: self, revision: -file.revision}
			child := parent.NewPersistentInode(
				ctx, &inode, fs.StableAttr{Mode: node.stat.Mode, Ino: node.stat.Ino})
			parent.AddChild(name, child, true)
			queue = append(queue, &inode)
		}
	}

	// revision root initialized, set to 0 to prevent calling
	// addRevisionFiles again
	file.revision = 0

	return
}

func MountFileSystem(fsPath string, manager *BackupManager) {
	LOG_INFO("MOUNTING_FILESYSTEM", "Mounting snapshot %s on %s", manager.snapshotID, fsPath)

	backupfs, err := NewBackupFS(manager)
	if err != nil {
		LOG_ERROR("MOUNTING_FILESYSTEM", "Failed to create BackupFS: %v", err)
		return
	}

	user, err := user.Current()
	if err != nil {
		log.Fatalf("Failed to get current user: %v\n", err)
	}

	uid, _ := strconv.Atoi(user.Uid)
	gid, _ := strconv.Atoi(user.Gid)

	server, err := fs.Mount(fsPath, &backupfs, &fs.Options{UID: uint32(uid), GID: uint32(gid)})
	if err != nil {
		log.Fatalf("Mount fail: %v\n", err)
	}
	server.Wait()

	return
}

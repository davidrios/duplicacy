package duplicacy

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	lru "github.com/hashicorp/golang-lru"
)

const (
	ENOENT  = 2
	EEXIST  = 27
	EISDIR  = 21
	ENOTDIR = 20

	S_IFDIR = 0040000
	S_IFMT  = 0170000
	S_IFREG = 0100000
)

type Stat_t struct {
	Ino      uint64
	Mode     uint32
	Nlink    uint32
	Uid      uint32
	Gid      uint32
	Size     int64
	Mtim     int64
	Birthtim int64
}

type node_t struct {
	stat      Stat_t
	chld      map[string]*node_t
	opencnt   int
	chunkInfo *mountChunkInfo
}

func (self *BackupFS) getNode(path string, fh uint64) *node_t {
	if ^uint64(0) == fh {
		_, _, node := self.lookupNode(path, nil)
		return node
	} else {
		return self.openmap[fh]
	}
}

func (self *BackupFS) openNode(path string, dir bool) (int, uint64) {
	_, _, node := self.lookupNode(path, nil)
	if nil == node {
		return ENOENT, ^uint64(0)
	}
	if !dir && S_IFDIR == node.stat.Mode&S_IFMT {
		return EISDIR, ^uint64(0)
	}
	if dir && S_IFDIR != node.stat.Mode&S_IFMT {
		return ENOTDIR, ^uint64(0)
	}
	node.opencnt++
	if 1 == node.opencnt {
		self.openmap[node.stat.Ino] = node
	}
	return 0, node.stat.Ino
}

func (self *BackupFS) lookupNode(path string, ancestor *node_t) (prnt *node_t, name string, node *node_t) {
	prnt = self.root
	name = ""
	node = self.root
	for _, c := range mountSplitPath(path) {
		if "" != c {
			prnt, name = node, c
			if node == nil {
				return
			}
			node = node.chld[c]
			if nil != ancestor && node == ancestor {
				name = "" // special case loop condition
				return
			}
		}
	}
	return
}

func (self *BackupFS) makeNode(path string, mode uint32, tmsp int64) (*node_t, int) {
	prnt, name, node := self.lookupNode(path, nil)
	if nil == prnt {
		return nil, ENOENT
	}
	if nil != node {
		return nil, EEXIST
	}
	self.ino++
	node = mountMakeNode(self.ino, mode, tmsp)
	prnt.chld[name] = node
	return node, 0
}

func (self *BackupFS) initRoot() error {
	if self.root != nil {
		return nil
	}

	self.openmap = map[uint64]*node_t{}

	revisions, err := self.manager.SnapshotManager.ListSnapshotRevisions(self.manager.snapshotID)
	if err != nil {
		LOG_ERROR("MOUNTING_FILESYSTEM", "Failed to list all revisions for snapshot %s: %v", self.manager.snapshotID, err)
		return errors.New("snapshot_revision_list_failed")
	}

	LOG_INFO("MOUNTING_FILESYSTEM", "Found %v revisions", len(revisions))

	alreadyCreated := make(map[string]bool)

	const DIR_MODE = S_IFDIR | 00555

	self.ino++
	self.root = mountMakeNode(self.ino, DIR_MODE, 0)
	self.snapshots = make(map[int]*Snapshot)

	for _, revision := range revisions {
		snapshot := self.manager.SnapshotManager.DownloadSnapshot(self.manager.snapshotID, revision)
		self.snapshots[revision] = snapshot

		creationTime := time.Unix(snapshot.StartTime, 0)

		year := strconv.Itoa(creationTime.Year())
		yearPath := fmt.Sprintf("/%s", year)
		if !alreadyCreated[yearPath] {
			date := time.Date(creationTime.Year(), 1, 1, 0, 0, 0, 0, time.Local)
			self.makeNode(yearPath, DIR_MODE, date.Unix())
			alreadyCreated[yearPath] = true
		}

		month := fmt.Sprintf("%02d", int(creationTime.Month()))
		monthPath := fmt.Sprintf("%s/%s", yearPath, month)
		if !alreadyCreated[monthPath] {
			date := time.Date(creationTime.Year(), creationTime.Month(), 1, 0, 0, 0, 0, time.Local)
			self.makeNode(monthPath, DIR_MODE, date.Unix())
			alreadyCreated[monthPath] = true
		}

		day := fmt.Sprintf("%02d", creationTime.Day())
		dayPath := fmt.Sprintf("%s/%s", monthPath, day)
		if !alreadyCreated[dayPath] {
			date := time.Date(creationTime.Year(), creationTime.Month(), creationTime.Day(), 0, 0, 0, 0, time.Local)
			self.makeNode(dayPath, DIR_MODE, date.Unix())
			alreadyCreated[dayPath] = true
		}

		dirname := fmt.Sprintf(
			"%02d%02d.%d",
			creationTime.Hour(), creationTime.Minute(), revision)
		dirPath := fmt.Sprintf("%s/%s", dayPath, dirname)
		date := time.Date(creationTime.Year(), creationTime.Month(), creationTime.Day(), creationTime.Hour(), creationTime.Minute(), 0, 0, time.Local)
		self.makeNode(dirPath, DIR_MODE, date.Unix())
	}

	return nil
}

func (self *BackupFS) initRevisionPath(path string) (revision int, err error) {
	initErr := self.initRoot()
	if initErr != nil {
		err = initErr
		return
	}

	components := mountSplitPath(path)
	if len(components) < 5 {
		return
	}

	revision, err = getDirRevision(components[4])
	if err != nil {
		return
	}

	root := strings.Join(components[:5], "/")

	_, err = self.initRevision(revision, root)

	return
}

func (self *BackupFS) initRevision(revision int, root string) (inited bool, err error) {
	if self.initedRevisions == nil {
		self.initedRevisions = map[int]bool{}
	}

	if self.initedRevisions[revision] {
		return
	}

	LOG_INFO("MOUNTING_FILESYSTEM", "initRevision %d", revision)

	snapshot := self.snapshots[revision]
	if snapshot == nil {
		LOG_INFO("MOUNTING_FILESYSTEM", "Snapshot revision not found")
		return
	}

	if !self.manager.SnapshotManager.DownloadSnapshotSequences(snapshot) {
		LOG_INFO("MOUNTING_FILESYSTEM", "Snapshot sequences download failed")
		return
	}

	snapshot.ListRemoteFiles(
		self.manager.config,
		self.manager.SnapshotManager.chunkOperator,
		func(entry *Entry) bool {
			if entry.Mode&0o20000000000 == 0o20000000000 {
				self.makeNode(fmt.Sprintf("%s/%s", root, entry.Path), S_IFDIR|(entry.Mode&00777), entry.Time)
			} else {
				node, err := self.makeNode(fmt.Sprintf("%s/%s", root, entry.Path), S_IFREG|(entry.Mode&00777), entry.Time)
				if err != 0 {
					return true
				}

				node.stat.Size = entry.Size
				node.chunkInfo = &mountChunkInfo{
					StartChunk:  entry.StartChunk,
					StartOffset: entry.StartOffset,
					EndChunk:    entry.EndChunk,
					EndOffset:   entry.EndOffset,
				}

				LOG_INFO("MOUNTING_FILESYSTEM", "created node: %s, %v, %v", entry.Path, node.stat, node.chunkInfo)
			}
			return true
		})

	inited = true
	self.initedRevisions[revision] = true

	return
}

func (self *BackupFS) readFileChunkCached(snapshot *Snapshot, chunkInfo *mountChunkInfo, buff []byte, ofst int64) (read int, retErr error) {
	newbuff := new(bytes.Buffer)

	// need to fill supplied buffer as much as possible
	for newbuff.Len() < len(buff) {
		params, err := calculateChunkReadParams(snapshot.ChunkLengths, chunkInfo, ofst)
		if err != nil {
			retErr = err
			return
		}

		hash := snapshot.ChunkHashes[params.chunkIndex]

		chunkSize := params.end - params.start
		readLen := len(buff) - newbuff.Len()
		if readLen < chunkSize {
			params.end = params.start + readLen
		}

		var data []byte
		cacheData, ok := self.chunkCache.Get(hash)
		if ok {
			catCacheData, ok := cacheData.([]byte)
			if !ok {
				retErr = errors.New("got invalid data from cache")
				return
			}
			data = catCacheData
		} else {
			LOG_INFO("MOUNTING_FILESYSTEM", "downloading chunk %x", hash)
			self.manager.SnapshotManager.CreateChunkOperator(false, 1, false)
			chunk := self.manager.SnapshotManager.chunkOperator.Download(hash, 0, false)
			data = chunk.GetBytes()
			self.chunkCache.Add(hash, data)
		}

		newbuff.Write(data[params.start:params.end])

		if params.chunkIndex == chunkInfo.EndChunk {
			break
		}

		ofst += int64(len(data[params.start:params.end]))
	}

	read = copy(buff, newbuff.Bytes())
	return
}

func getDirRevision(dirName string) (revision int, retErr error) {
	dirComponents := strings.Split(dirName, ".")
	if len(dirComponents) != 2 {
		retErr = errors.New("invalid name")
		return
	}

	revision, err := strconv.Atoi(dirComponents[1])
	if err != nil {
		retErr = errors.New("invalid name")
		return
	}

	return
}

type mountReadParams struct {
	chunkIndex int
	start      int
	end        int
}

type mountChunkInfo struct {
	StartChunk  int
	StartOffset int
	EndChunk    int
	EndOffset   int
}

func calculateChunkReadParams(chunkLengths []int, file *mountChunkInfo, ofst int64) (params mountReadParams, err error) {
	if ofst < 0 {
		err = errors.New("ofst cannot be negative")
		return
	}

	ofst += int64(file.StartOffset)
	lstart := ofst
	totalLen := int64(0)

	if len(chunkLengths) == 0 {
		err = errors.New("chunkLenghts cannot be empty")
		return
	}

	if len(chunkLengths) <= file.EndChunk {
		err = errors.New("chunkLenghts is not big enough")
		return
	}

	for params.chunkIndex = file.StartChunk; params.chunkIndex <= file.EndChunk; params.chunkIndex++ {
		chunkLen := int64(chunkLengths[params.chunkIndex])
		totalLen += chunkLen
		if ofst < totalLen || file.EndChunk == params.chunkIndex {
			break
		}

		lstart -= chunkLen
	}

	if totalLen == 0 {
		err = errors.New("no data in chunks")
		return
	}

	params.end = chunkLengths[params.chunkIndex]
	if params.chunkIndex == file.EndChunk {
		params.end = file.EndOffset
		if params.end > chunkLengths[params.chunkIndex] {
			err = errors.New("no data in chunks")
		}
	}

	params.start = int(lstart)
	if lstart > int64(params.end) {
		params.start = params.end
	}

	return
}

func mountMakeNode(ino uint64, mode uint32, tmsp int64) *node_t {
	self := node_t{
		stat: Stat_t{
			Ino:      ino,
			Uid:      0,
			Gid:      0,
			Mode:     mode,
			Nlink:    1,
			Mtim:     tmsp,
			Birthtim: tmsp,
		},
	}
	if S_IFDIR == self.stat.Mode&S_IFMT {
		self.chld = map[string]*node_t{}
	}
	return &self
}

func mountSplitPath(path string) []string {
	return strings.Split(path, "/")
}

func NewBackupFS(manager *BackupManager) (fs BackupFS, err error) {
	fs.manager = manager

	chunkCache, err := lru.New2Q(50)
	if err != nil {
		LOG_ERROR("MOUNTING_FILESYSTEM", "Failed to init cache: %v", err)
		err = errors.New("init cache failed")
		return
	}
	fs.chunkCache = chunkCache

	return
}

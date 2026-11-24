package mount

import (
	"sync"
	"sync/atomic"

	"github.com/seaweedfs/seaweedfs/weed/util"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

type FileHandleToInode struct {
	sync.RWMutex
	inode2fh map[uint64]*FileHandle
	fh2inode map[FileHandleId]uint64
}

func NewFileHandleToInode() *FileHandleToInode {
	return &FileHandleToInode{
		inode2fh: make(map[uint64]*FileHandle),
		fh2inode: make(map[FileHandleId]uint64),
	}
}

func (i *FileHandleToInode) GetFileHandle(fh FileHandleId) *FileHandle {
	i.RLock()
	defer i.RUnlock()
	inode, found := i.fh2inode[fh]
	if found {
		return i.inode2fh[inode]
	}
	return nil
}

func (i *FileHandleToInode) FindFileHandle(inode uint64) (fh *FileHandle, found bool) {
	i.RLock()
	defer i.RUnlock()
	fh, found = i.inode2fh[inode]
	return
}

// MarkInodeRenamed sets isRenamed on any file handle associated with the
// given inode.  This prevents the async flush from recreating a renamed
// file's metadata under its old path.
func (i *FileHandleToInode) MarkInodeRenamed(inode uint64) {
	i.RLock()
	defer i.RUnlock()
	if fh, ok := i.inode2fh[inode]; ok {
		fh.isRenamed = true
	}
}

// AcquireFileHandle fully initializes a handle — entry and the log position it
// reflects — before exposing it in the map, and reports whether one already
// existed. An existing handle only gets its counter bumped: its entry belongs
// to whoever holds the handle lock, so callers install there (AcquireHandle)
// rather than under this one.
func (i *FileHandleToInode) AcquireFileHandle(wfs *WFS, inode uint64, entry *filer_pb.Entry, versionTsNs int64, signature int32, appendMode ...bool) (*FileHandle, bool) {
	fh, _, existed := i.AcquireOpenFileHandle(wfs, inode, entry, versionTsNs, signature, len(appendMode) > 0 && appendMode[0])
	return fh, existed
}

func (i *FileHandleToInode) AcquireOpenFileHandle(wfs *WFS, inode uint64, entry *filer_pb.Entry, versionTsNs int64, signature int32, appendMode bool) (*FileHandle, FileHandleId, bool) {
	// Bind before publishing the handle or queuing any async close. Capture
	// under the inode-table lock, but release it before taking the fhMap lock.
	var writebackError *atomic.Bool
	if wfs.option.WritebackCache {
		wfs.inodeToPath.RLock()
		state, found := wfs.writebackContentErrors.Load(inode)
		if !found {
			state, _ = wfs.writebackContentErrors.LoadOrStore(inode, new(atomic.Bool))
		}
		writebackError = state.(*atomic.Bool)
		wfs.inodeToPath.RUnlock()
	}
	i.Lock()
	defer i.Unlock()
	fh, found := i.inode2fh[inode]
	if !found {
		fh = newFileHandle(wfs, FileHandleId(util.RandomUint64()), inode, entry)
		fh.writebackContentError = writebackError
		fh.fhs[fh.fh] = appendMode
		fh.entryVersionTsNs.Store(versionTsNs)
		fh.entryVersionSignature.Store(signature)
		i.inode2fh[inode] = fh
		i.fh2inode[fh.fh] = inode
		return fh, fh.fh, false
	}
	fh.counter++
	openFh := FileHandleId(util.RandomUint64())
	fh.fhs[openFh] = appendMode
	i.fh2inode[openFh] = inode
	return fh, openFh, true
}

func (i *FileHandleToInode) isAppendHandle(openFh FileHandleId) bool {
	i.RLock()
	defer i.RUnlock()
	if fh := i.inode2fh[i.fh2inode[openFh]]; fh != nil {
		return fh.fhs[openFh]
	}
	return false
}

func (i *FileHandleToInode) ReleaseByHandle(fh FileHandleId) *FileHandle {
	i.Lock()
	defer i.Unlock()

	inode, found := i.fh2inode[fh]
	if !found {
		return nil
	}

	fhHandle, fhFound := i.inode2fh[inode]
	if !fhFound {
		delete(i.fh2inode, fh)
		return nil
	}

	// If the counter is already <= 0, a prior Release already started the
	// drain.  Return nil to prevent double-processing.
	if fhHandle.counter <= 0 {
		return nil
	}

	delete(fhHandle.fhs, fh)
	if fh != fhHandle.fh {
		delete(i.fh2inode, fh)
	}
	fhHandle.counter--
	if fhHandle.counter <= 0 {
		if fhHandle.asyncFlushPending {
			// Handle stays in fhMap so rename/unlink can still find it
			// via FindFileHandle during the background drain.
			return fhHandle
		}
		// Fence before a reopen can see the inode without this handle.
		if fhHandle.dirtyContent && !fhHandle.isDeleted {
			(&contentPublication{failure: fhHandle.writebackContentError}).abandon()
		}
		delete(i.inode2fh, inode)
		delete(i.fh2inode, fhHandle.fh)
		for openFh := range fhHandle.fhs {
			delete(i.fh2inode, openFh)
		}
		return fhHandle
	}
	return nil
}

// RemoveFileHandle removes a handle from both maps.  Called after an async
// drain completes to clean up the handle that was intentionally kept in the
// maps during the flush.
func (i *FileHandleToInode) RemoveFileHandle(fh FileHandleId, inode uint64) {
	i.Lock()
	defer i.Unlock()
	if fhHandle, found := i.inode2fh[inode]; found {
		for openFh := range fhHandle.fhs {
			delete(i.fh2inode, openFh)
		}
		delete(i.fh2inode, fhHandle.fh)
	}
	delete(i.inode2fh, inode)
	delete(i.fh2inode, fh)
}

package mount

import (
	"syscall"

	"github.com/seaweedfs/go-fuse/v2/fuse"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// Lookup is called by the kernel when the VFS wants to know
// about a file inside a directory. Many lookup calls can
// occur in parallel, but only one call happens for each (dir,
// name) pair.

func (wfs *WFS) Lookup(cancel <-chan struct{}, header *fuse.InHeader, name string, out *fuse.EntryOut) (code fuse.Status) {

	var s fuse.Status
	if name, s = checkName(name); s != fuse.OK {
		return s
	}

	dirPath, code := wfs.inodeToPath.GetPath(header.NodeId)
	if code != fuse.OK {
		if name == "." || name == ".." {
			return fuse.Status(syscall.ESTALE)
		}
		return
	}

	mountRoot := util.FullPath(wfs.option.FilerMountRootPath)
	if mountRoot == "" {
		mountRoot = "/"
	}

	var fullFilePath util.FullPath
	switch name {
	case ".":
		fullFilePath = dirPath
	case "..":
		if dirPath == mountRoot || dirPath == "/" {
			fullFilePath = mountRoot
		} else {
			parentDir, _ := dirPath.DirAndName()
			fullFilePath = util.FullPath(parentDir)
		}
	default:
		fullFilePath = dirPath.Child(name)
	}

	if fullFilePath == "/" || fullFilePath == mountRoot {
		wfs.inodeToPath.IncrementNlookup(1)
		var attrOut fuse.AttrOut
		wfs.setRootAttr(&attrOut)
		if wfs.option.PosixDirNlink {
			wfs.applyDirNlink(&attrOut.Attr, mountRoot)
		}
		out.NodeId = 1
		out.Generation = 0
		out.EntryValid = wfs.entryValidSec
		out.AttrValid = attrOut.AttrValid
		out.Attr = attrOut.Attr
		return fuse.OK
	}

	// Use shared lookup logic that checks cache first, then filer if needed
	ctx, cancelFunc := ctxFromCancel(cancel)
	defer cancelFunc()
	localEntry, _, status := wfs.lookupEntry(ctx, fullFilePath)
	if status != fuse.OK {
		if status == fuse.ENOENT && name == "." {
			return fuse.Status(syscall.ESTALE)
		}
		return status
	}

	if name == "." && localEntry.Inode != 0 && localEntry.Inode != header.NodeId {
		return fuse.Status(syscall.ESTALE)
	}

	possibleInode := localEntry.Inode
	if name == "." && possibleInode == 0 {
		possibleInode = header.NodeId
	}
	inode := wfs.inodeToPath.Lookup(fullFilePath, localEntry.Crtime.Unix(), localEntry.IsDirectory(), len(localEntry.HardLinkId) > 0, possibleInode, true)

	if fh, found := wfs.fhMap.FindFileHandle(inode); found {
		fh.entryLock.RLock()
		if entry := fh.GetEntry().GetEntry(); entry != nil {
			parentDir, _ := fullFilePath.DirAndName()
			glog.V(4).Infof("lookup opened file %s size %d", fullFilePath, filer.FileSize(entry))
			localEntry = filer.FromPbEntry(parentDir, entry)
		}
		fh.entryLock.RUnlock()
	}

	wfs.outputFilerEntry(out, inode, localEntry)

	if localEntry.IsDirectory() {
		wfs.applyInMemoryDirMtime(&out.Attr, inode)
		if wfs.option.PosixDirNlink {
			wfs.applyDirNlink(&out.Attr, fullFilePath)
		}
	}

	return fuse.OK

}

//go:build linux

package mount

import (
	"unsafe"

	"github.com/seaweedfs/go-fuse/v2/fuse"
)

const fuseOpInit = uint32(26)

type fuseOperationHandler struct {
	Name string
	Func func(server, req unsafe.Pointer)
}

//go:linkname fuseGetHandler github.com/seaweedfs/go-fuse/v2/fuse.getHandler
func fuseGetHandler(o uint32) *fuseOperationHandler

//go:linkname fuseReqInData github.com/seaweedfs/go-fuse/v2/fuse.(*request).inData
func fuseReqInData(req unsafe.Pointer) unsafe.Pointer

//go:linkname fuseReqOutData github.com/seaweedfs/go-fuse/v2/fuse.(*request).outData
func fuseReqOutData(req unsafe.Pointer) unsafe.Pointer

// Enable FUSE_EXPORT_SUPPORT (fuse.CAP_EXPORT_SUPPORT) during FUSE_INIT on
// Linux so the kernel FUSE superblock sets fc->export_support = 1. Without
// this flag, open_by_handle_at() (used by NFS-Ganesha FSAL_VFS and knfsd)
// immediately returns ESTALE inside the kernel's fuse_get_dentry() whenever an
// inode is evicted from the kernel icache (e.g. drop_caches=2/3 or memory
// pressure), without issuing FUSE_LOOKUP(nodeid, ".") or FUSE_LOOKUP(nodeid, "..")
// to recover the evicted inode.
func init() {
	h := fuseGetHandler(fuseOpInit)
	if h == nil || h.Func == nil {
		return
	}
	origInit := h.Func
	h.Func = func(server, req unsafe.Pointer) {
		origInit(server, req)
		in := (*fuse.InitIn)(fuseReqInData(req))
		out := (*fuse.InitOut)(fuseReqOutData(req))
		if in != nil && out != nil && (in.Flags&uint32(fuse.CAP_EXPORT_SUPPORT)) != 0 {
			out.Flags |= uint32(fuse.CAP_EXPORT_SUPPORT)
		}
	}
}

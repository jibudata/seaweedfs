package mount

import (
	"context"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
)

// mergeGCTimeout bounds the deletion of the chunks a merge replaced.  The
// delete is one volume-server round trip per volume; the merge itself already
// spent seconds re-uploading the file, so a bounded wait keeps an unresponsive
// volume server from stalling the flush that committed the merge.
const mergeGCTimeout = 30 * time.Second

// replacedChunks returns the chunks of old that new does not reference any
// more.  The comparison is by file id: two chunks with the same id name the
// same needle, and only a needle that no list names may be deleted.
func replacedChunks(old, new []*filer_pb.FileChunk) []*filer_pb.FileChunk {
	kept := make(map[string]struct{}, len(new))
	for _, c := range new {
		if id := chunkNeedleId(c); id != "" {
			kept[id] = struct{}{}
		}
	}
	var replaced []*filer_pb.FileChunk
	for _, c := range old {
		id := chunkNeedleId(c)
		if id == "" {
			continue
		}
		if _, ok := kept[id]; ok {
			continue
		}
		replaced = append(replaced, c)
	}
	return replaced
}

// chunkNeedleId renders a chunk's file id in the full "volume,key,cookie"
// form the volume server's batch delete parses.  A chunk that carries only the
// short "volume,key" form is expanded with cookie 0: the delete skips the
// cookie check, so the cookie only has to be present in the id.
func chunkNeedleId(c *filer_pb.FileChunk) string {
	if c == nil {
		return ""
	}
	if c.Fid != nil {
		return needle.NewFileId(needle.VolumeId(c.Fid.VolumeId), c.Fid.FileKey, c.Fid.Cookie).String()
	}
	if fid, err := filer_pb.ToFileIdObject(c.GetFileIdString()); err == nil && fid != nil {
		return needle.NewFileId(needle.VolumeId(fid.VolumeId), fid.FileKey, fid.Cookie).String()
	}
	return ""
}

// deleteMergedChunks marks the chunks a merge replaced as deleted on the
// volume servers.
//
// The filer deletes the chunks an entry no longer references when it stores
// the new one.  That diff is computed against the entry the filer holds,
// which is not necessarily the list this mount replaced: a manifest the filer
// cannot resolve makes it drop the whole diff - it then deletes nothing at
// all - and a chunk list the filer re-compacted under a different name leaves
// the replaced needles unreferenced.  A needle that stays live while nothing
// references it is invisible to compaction and to volume.vacuum, so it is
// never reclaimed and the space it holds never shows up in a capacity metric.
//
// The delete runs only after the merge's new chunk list was committed, and
// only for needles the committed entry does not name: a commit that failed
// must leave the old chunks alone, and a chunk the new list still names must
// survive.
func (wfs *WFS) deleteMergedChunks(ctx context.Context, fileFullPath util.FullPath, replaced, committed []*filer_pb.FileChunk) {
	if len(replaced) == 0 {
		return
	}
	committedIds := make(map[string]struct{}, len(committed))
	for _, c := range committed {
		if id := chunkNeedleId(c); id != "" {
			committedIds[id] = struct{}{}
		}
	}

	var ids []string
	var unresolved int
	for _, c := range replaced {
		id := chunkNeedleId(c)
		if id == "" {
			continue
		}
		if _, ok := committedIds[id]; ok {
			continue
		}
		if c.IsChunkManifest {
			// The manifest's sub-chunks are the data it covers.  Nothing
			// references them once the manifest is gone, so they go with it.
			// If they cannot be resolved, the manifest is left alone as well:
			// deleting it would orphan the data it holds.
			members, err := filer.ResolveOneChunkManifest(ctx, wfs.LookupFn(), c, wfs.CacheInvalidator())
			if err != nil {
				glog.V(0).Infof("merge gc %s: resolve manifest %s: %v", fileFullPath, id, err)
				unresolved++
				continue
			}
			for _, m := range members {
				mid := chunkNeedleId(m)
				if mid == "" {
					continue
				}
				if _, ok := committedIds[mid]; ok {
					continue
				}
				ids = append(ids, mid)
			}
		}
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		return
	}

	if wfs.filerClient == nil {
		// Without the filer client the volumes of the chunks cannot be
		// located, so the delete cannot run.  The chunks stay live; the
		// operator procedure (volume.fsck) is what reclaims them.
		glog.V(0).Infof("merge gc %s: no filer client, cannot locate the volumes of %d replaced chunk(s)", fileFullPath, len(ids))
		return
	}

	deleteCtx, cancel := context.WithTimeout(ctx, mergeGCTimeout)
	defer cancel()
	lookupFunc := func(vids []string) (map[string]*operation.LookupResult, error) {
		locations, err := wfs.filerClient.LookupVolumeIdsWithFallback(deleteCtx, vids)
		if err != nil && len(locations) == 0 {
			return nil, err
		}
		results := make(map[string]*operation.LookupResult, len(locations))
		for vid, locs := range locations {
			results[vid] = &operation.LookupResult{VolumeOrFileId: vid, Locations: toOperationLocations(locs)}
		}
		return results, nil
	}
	results := operation.DeleteFileIdsWithLookupVolumeId(deleteCtx, wfs.option.GrpcDialOption, ids, false, lookupFunc)
	deleted, failed := 0, 0
	for _, r := range results {
		if r.Error != "" {
			failed++
			continue
		}
		deleted++
	}
	glog.V(0).Infof("MGCLOG file=%s replaced=%d deleted=%d failed=%d unresolved=%d",
		fileFullPath, len(replaced), deleted, failed, unresolved)
}

// toOperationLocations converts volume locations into the form the delete
// path expects.
func toOperationLocations(locs []wdclient.Location) []operation.Location {
	var locations []operation.Location
	for _, loc := range locs {
		locations = append(locations, operation.Location{
			Url:       loc.Url,
			PublicUrl: loc.PublicUrl,
			GrpcPort:  loc.GrpcPort,
		})
	}
	return locations
}

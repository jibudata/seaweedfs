package weed_server

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// acquireRenamePathLocks holds both paths across commit and notification so a
// fenced lookup cannot read renamed state under a fence preceding its events.
// Path order avoids deadlock with a reverse rename; descendants are not
// locked. The returned func releases both.
func (fs *FilerServer) acquireRenamePathLocks(intention string, oldPath, newPath util.FullPath) func() {
	firstPath, secondPath := oldPath, newPath
	if secondPath < firstPath {
		firstPath, secondPath = secondPath, firstPath
	}
	firstLock := fs.entryLockTable.AcquireLock(intention, firstPath, util.ExclusiveLock)
	if secondPath == firstPath {
		return func() { fs.entryLockTable.ReleaseLock(firstPath, firstLock) }
	}
	secondLock := fs.entryLockTable.AcquireLock(intention, secondPath, util.ExclusiveLock)
	return func() {
		fs.entryLockTable.ReleaseLock(secondPath, secondLock)
		fs.entryLockTable.ReleaseLock(firstPath, firstLock)
	}
}

func (fs *FilerServer) AtomicRenameEntry(ctx context.Context, req *filer_pb.AtomicRenameEntryRequest) (*filer_pb.AtomicRenameEntryResponse, error) {

	glog.V(1).Infof("AtomicRenameEntry %v", req)

	oldParent := util.FullPath(filepath.ToSlash(req.OldDirectory))
	newParent := util.FullPath(filepath.ToSlash(req.NewDirectory))

	if err := fs.filer.CanRename(ctx, oldParent, newParent, req.OldName); err != nil {
		return nil, err
	}

	defer fs.acquireRenamePathLocks("AtomicRenameEntry", oldParent.Child(req.OldName), newParent.Child(req.NewName))()

	// a SQL store keeps the transaction in the context and refuses calls
	// through it once committed; the work after the commit uses this one
	afterCommitCtx := ctx
	effects, err := fs.runRenameTransaction(ctx, fmt.Sprintf("%s/%s", req.OldDirectory, req.OldName), func(ctx context.Context, effects *renameEffects) (bool, error) {
		oldEntry, err := fs.filer.FindEntry(ctx, oldParent.Child(req.OldName))
		if err != nil {
			return false, fmt.Errorf("%s/%s not found: %v", req.OldDirectory, req.OldName, err)
		}

		if moveErr := fs.moveEntry(ctx, nil, oldParent, oldEntry, newParent, req.NewName, req.Signatures, false, &effects.metadataEvents, &effects.pendingChunkDeletes); moveErr != nil {
			return false, fmt.Errorf("%s/%s move error: %v", req.OldDirectory, req.OldName, moveErr)
		}
		return fs.filer.TransactionCovers(ctx, oldParent.Child(req.OldName), newParent.Child(req.NewName)) && retryableRename(oldEntry, effects), nil
	})
	if err != nil {
		return nil, err
	}
	fs.finishRename(afterCommitCtx, effects, req.Signatures)

	return &filer_pb.AtomicRenameEntryResponse{}, nil
}

func (fs *FilerServer) StreamRenameEntry(req *filer_pb.StreamRenameEntryRequest, stream filer_pb.SeaweedFiler_StreamRenameEntryServer) (err error) {

	glog.V(1).Infof("StreamRenameEntry %v", req)

	oldParent := util.FullPath(filepath.ToSlash(req.OldDirectory))
	newParent := util.FullPath(filepath.ToSlash(req.NewDirectory))

	if err := fs.filer.CanRename(stream.Context(), oldParent, newParent, req.OldName); err != nil {
		return err
	}

	defer fs.acquireRenamePathLocks("StreamRenameEntry", oldParent.Child(req.OldName), newParent.Child(req.NewName))()

	afterCommitCtx := context.Background()
	var held *renameResponseBuffer
	effects, err := fs.runRenameTransaction(afterCommitCtx, fmt.Sprintf("%s/%s", req.OldDirectory, req.OldName), func(ctx context.Context, effects *renameEffects) (bool, error) {
		held = nil
		oldEntry, err := fs.filer.FindEntry(ctx, oldParent.Child(req.OldName))
		if err != nil {
			return false, fmt.Errorf("%s/%s not found: %v", req.OldDirectory, req.OldName, err)
		}

		if oldEntry.IsDirectory() {
			// follow https://pubs.opengroup.org/onlinepubs/000095399/functions/rename.html
			targetDir := newParent.Child(req.NewName)
			newEntry, err := fs.filer.FindEntry(ctx, targetDir)
			if err == nil {
				if !newEntry.IsDirectory() {
					return false, fmt.Errorf("%s is not directory", targetDir)
				}
				entries, _, listErr := fs.filer.ListDirectoryEntries(ctx, targetDir, "", false, 1, "", "", "")
				if listErr != nil {
					return false, fmt.Errorf("check target directory %s: %w", targetDir, listErr)
				}
				if len(entries) > 0 {
					return false, fmt.Errorf("%s is not empty", targetDir)
				}
			} else if !errors.Is(err, filer_pb.ErrNotFound) {
				return false, fmt.Errorf("lookup target directory %s: %w", targetDir, err)
			}
		}

		// The client applies each response as it arrives. When the store's
		// transaction covers the rename's writes, a file's responses wait for
		// the commit: a rename the store refuses never reaches the client, and
		// one that runs again sends them once. A directory sends its
		// children's as it moves them, which keeps the client of a long rename
		// from timing out; having sent them, it does not run again. Without
		// such a transaction the writes show as they are made, and so do the
		// responses.
		responses := stream
		if !oldEntry.IsDirectory() && fs.filer.TransactionCovers(ctx, oldParent.Child(req.OldName), newParent.Child(req.NewName)) {
			held = &renameResponseBuffer{SeaweedFiler_StreamRenameEntryServer: stream}
			responses = held
		}
		if moveErr := fs.moveEntry(ctx, responses, oldParent, oldEntry, newParent, req.NewName, req.Signatures, false, &effects.metadataEvents, &effects.pendingChunkDeletes); moveErr != nil {
			return false, fmt.Errorf("%s/%s move error: %v", req.OldDirectory, req.OldName, moveErr)
		}
		return held != nil && retryableRename(oldEntry, effects), nil
	})
	if err != nil {
		return err
	}
	if held == nil {
		fs.finishRename(afterCommitCtx, effects, req.Signatures)
		return nil
	}
	// Committed. The held responses go out while the rename finishes what the
	// commit leaves (events, chunk deletions): a client that stopped reading
	// cannot hold that work up. The call returns, and gives up the rename's
	// path locks, once both are done.
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		fs.finishRename(afterCommitCtx, effects, req.Signatures)
	}()
	err = held.release()
	<-finished
	return err
}

// renameAttempts bounds how often a rename runs its transaction while the
// store refuses the commit for a concurrent write to one of its keys.
const renameAttempts = 5

// renameEffects is what a rename's transaction leaves for after its commit.
type renameEffects struct {
	metadataEvents      []metadataEvent
	pendingChunkDeletes []*filer_pb.FileChunk
}

// runRenameTransaction runs attempt in a new store transaction and commits it.
// A store with optimistic transactions (TiKV) refuses the commit when another
// writer changed one of the transaction's keys after it began: a flush of the
// file being renamed, say. While attempt reports that nothing it did has left
// the transaction, it runs again in a new one, which reads that write and
// moves it along instead of failing the rename.
func (fs *FilerServer) runRenameTransaction(ctx context.Context, oldPath string, attempt func(ctx context.Context, effects *renameEffects) (retryable bool, err error)) (*renameEffects, error) {
	for n := 1; ; n++ {
		txCtx, err := fs.filer.BeginTransaction(ctx)
		if err != nil {
			return nil, err
		}
		effects := &renameEffects{}
		retryable, err := attempt(txCtx, effects)
		if err != nil {
			fs.filer.RollbackTransaction(txCtx)
			return nil, err
		}
		commitErr := fs.filer.CommitTransaction(txCtx)
		if commitErr == nil {
			return effects, nil
		}
		fs.filer.RollbackTransaction(txCtx)
		if !retryable || n == renameAttempts || !errors.Is(commitErr, filer.ErrTransactionConflict) {
			return nil, fmt.Errorf("%s move commit error: %v", oldPath, commitErr)
		}
		glog.V(0).Infof("rename %s: running it again (%d of %d) after a concurrent write: %v", oldPath, n+1, renameAttempts, commitErr)
		time.Sleep(time.Duration(n) * 10 * time.Millisecond)
	}
}

// retryableRename reports whether a rename's transaction may run again after
// the store refused its commit: a file move that did nothing outside the
// transaction. A remote object copied or deleted on the way, for the file or
// for the target it replaced, would stay copied or deleted. The caller also
// checks that the transaction covers the rename's writes (TransactionCovers).
func retryableRename(oldEntry *filer.Entry, effects *renameEffects) bool {
	if oldEntry.IsDirectory() || oldEntry.Remote != nil {
		return false
	}
	for _, event := range effects.metadataEvents {
		if event.oldEntry != nil && event.oldEntry.Remote != nil {
			return false
		}
	}
	return true
}

// finishRename does what a committed rename leaves for after its commit.
func (fs *FilerServer) finishRename(ctx context.Context, effects *renameEffects, signatures []int32) {
	// Chunks from an overwritten rename target are only deletable after the
	// rename transaction has committed: anything that fails mid-rename (move,
	// child moves, oldPath delete, CommitTransaction) would otherwise leave
	// live metadata pointing at freshly-deleted chunks.
	if len(effects.pendingChunkDeletes) > 0 {
		fs.filer.DeleteChunksNotRecursive(effects.pendingChunkDeletes)
	}
	for _, event := range effects.metadataEvents {
		event.notify(fs.filer, ctx, signatures)
	}
}

// renameResponseBuffer holds a rename's responses for its client until the
// transaction that produced them has committed.
type renameResponseBuffer struct {
	filer_pb.SeaweedFiler_StreamRenameEntryServer
	held []*filer_pb.StreamRenameEntryResponse
}

// Send keeps resp for after the commit. It fails as the stream would once the
// client has gone away, so a rename whose client canceled is rolled back, as
// it is when the responses go out at once, instead of committed unseen.
func (b *renameResponseBuffer) Send(resp *filer_pb.StreamRenameEntryResponse) error {
	if err := b.Context().Err(); err != nil {
		return err
	}
	b.held = append(b.held, resp)
	return nil
}

func (b *renameResponseBuffer) release() error {
	for _, resp := range b.held {
		if err := b.SeaweedFiler_StreamRenameEntryServer.Send(resp); err != nil {
			return err
		}
	}
	return nil
}

type metadataEvent struct {
	oldEntry     *filer.Entry
	newEntry     *filer.Entry
	deleteChunks bool
}

func (event metadataEvent) notify(f *filer.Filer, ctx context.Context, signatures []int32) {
	f.NotifyUpdateEvent(ctx, event.oldEntry, event.newEntry, event.deleteChunks, false, signatures)
}

func (fs *FilerServer) moveEntry(ctx context.Context, stream filer_pb.SeaweedFiler_StreamRenameEntryServer, oldParent util.FullPath, entry *filer.Entry, newParent util.FullPath, newName string, signatures []int32, skipTargetLookup bool, metadataEvents *[]metadataEvent, pendingChunkDeletes *[]*filer_pb.FileChunk) error {

	if err := fs.moveSelfEntry(ctx, stream, oldParent, entry, newParent, newName, func() error {
		if entry.IsDirectory() {
			if err := fs.moveFolderSubEntries(ctx, stream, oldParent, entry, newParent, newName, signatures, metadataEvents, pendingChunkDeletes); err != nil {
				return err
			}
		}
		return nil
	}, signatures, skipTargetLookup, metadataEvents, pendingChunkDeletes); err != nil {
		return fmt.Errorf("fail to move %s => %s: %v", oldParent.Child(entry.Name()), newParent.Child(newName), err)
	}

	return nil
}

func (fs *FilerServer) moveFolderSubEntries(ctx context.Context, stream filer_pb.SeaweedFiler_StreamRenameEntryServer, oldParent util.FullPath, entry *filer.Entry, newParent util.FullPath, newName string, signatures []int32, metadataEvents *[]metadataEvent, pendingChunkDeletes *[]*filer_pb.FileChunk) error {

	currentDirPath := oldParent.Child(entry.Name())
	newDirPath := newParent.Child(newName)

	glog.V(1).Infof("moving folder %s => %s", currentDirPath, newDirPath)

	lastFileName := ""
	includeLastFile := false
	for {

		entries, hasMore, err := fs.filer.ListDirectoryEntries(ctx, currentDirPath, lastFileName, includeLastFile, 1024, "", "", "")
		if err != nil {
			return err
		}

		// println("found", len(entries), "entries under", currentDirPath)

		for _, item := range entries {
			lastFileName = item.Name()
			// println("processing", lastFileName)
			newChildPath := newDirPath.Child(item.Name())
			skipTarget := fs.filer.Store.SameActualStore(newDirPath, newChildPath)
			err := fs.moveEntry(ctx, stream, currentDirPath, item, newDirPath, item.Name(), signatures, skipTarget, metadataEvents, pendingChunkDeletes)
			if err != nil {
				return err
			}
		}
		if !hasMore {
			break
		}
	}
	return nil
}

func (fs *FilerServer) moveSelfEntry(ctx context.Context, stream filer_pb.SeaweedFiler_StreamRenameEntryServer, oldParent util.FullPath, entry *filer.Entry, newParent util.FullPath, newName string, moveFolderSubEntries func() error, signatures []int32, skipTargetLookup bool, metadataEvents *[]metadataEvent, pendingChunkDeletes *[]*filer_pb.FileChunk) error {

	oldPath, newPath := oldParent.Child(entry.Name()), newParent.Child(newName)

	glog.V(1).Infof("moving entry %s => %s", oldPath, newPath)

	if oldPath == newPath {
		glog.V(1).Infof("skip moving entry %s => %s", oldPath, newPath)
		return nil
	}

	sourceEntry := entry.ShallowClone()
	sourceEntry.FullPath = oldPath

	var existingTarget *filer.Entry
	if !skipTargetLookup {
		if targetEntry, findErr := fs.filer.FindEntry(ctx, newPath); findErr == nil {
			existingTarget = targetEntry.ShallowClone()
		} else if findErr != filer_pb.ErrNotFound {
			return findErr
		}
	}
	if existingTarget != nil {
		switch {
		case existingTarget.IsDirectory() && !entry.IsDirectory():
			return fmt.Errorf("%s: %w", existingTarget.FullPath, filer_pb.ErrExistingIsDirectory)
		case !existingTarget.IsDirectory() && entry.IsDirectory():
			return fmt.Errorf("%s: %w", existingTarget.FullPath, filer_pb.ErrExistingIsFile)
		}
		if deleteErr := fs.filer.DeleteEntryMetaAndData(
			filer.WithSuppressedMetadataEvents(ctx),
			newPath,
			false,
			false,
			false,
			false,
			signatures,
			0,
		); deleteErr != nil {
			return deleteErr
		}
	}

	// add to new directory
	newEntry := &filer.Entry{
		FullPath:        newPath,
		Attr:            entry.Attr,
		Chunks:          entry.GetChunks(),
		Extended:        entry.Extended,
		Content:         entry.Content,
		HardLinkCounter: entry.HardLinkCounter,
		HardLinkId:      entry.HardLinkId,
		Remote:          entry.Remote,
		Quota:           entry.Quota,
	}
	if skipTargetLookup {
		if newEntry.FullPath.IsLongerFileName(fs.filer.MaxFilenameLength) {
			return filer_pb.ErrEntryNameTooLong
		}
		if createErr := fs.filer.Store.InsertEntryKnownAbsent(filer.WithSuppressedMetadataEvents(ctx), newEntry); createErr != nil {
			return fmt.Errorf("insert entry %s: %v", newEntry.FullPath, createErr)
		}
	} else {
		if createErr := fs.filer.CreateEntry(filer.WithSuppressedMetadataEvents(ctx), newEntry, nil, false, false, signatures, false, fs.filer.MaxFilenameLength); createErr != nil {
			return createErr
		}
	}
	if existingTarget != nil {
		toDelete, err := filer.MinusChunks(ctx, fs.filer.MasterClient.GetLookupFileIdFunction(), existingTarget.GetChunks(), newEntry.GetChunks(), fs.filer.MasterClient)
		if err != nil {
			glog.ErrorfCtx(ctx, "Failed to resolve overwrite target chunks during rename. new: %v, old: %v", newEntry.GetChunks(), existingTarget.GetChunks())
		} else {
			// the target may be one of several hard links to its file: the
			// rename unlinked that name only, and the other names still read
			// the file's chunks
			toDelete = fs.filer.KeepHardLinkedChunks(ctx, existingTarget, newEntry, toDelete)
			if len(toDelete) > 0 {
				// Defer chunk deletion until after CommitTransaction so that a
				// failure in any subsequent step (child moves, oldPath delete,
				// stream send, or the commit itself) leaves the chunks intact for
				// the rolled-back rename.
				*pendingChunkDeletes = append(*pendingChunkDeletes, toDelete...)
			}
		}
	}
	if stream != nil {
		if err := stream.Send(&filer_pb.StreamRenameEntryResponse{
			Directory: string(oldParent),
			EventNotification: &filer_pb.EventNotification{
				OldEntry: &filer_pb.Entry{
					Name: entry.Name(),
				},
				NewEntry:           newEntry.ToProtoEntry(),
				DeleteChunks:       false,
				NewParentPath:      string(newParent),
				IsFromOtherCluster: false,
				Signatures:         nil,
			},
			TsNs: time.Now().UnixNano(),
		}); err != nil {
			return err
		}
	}

	if existingTarget != nil {
		*metadataEvents = append(*metadataEvents, metadataEvent{
			oldEntry:     existingTarget,
			deleteChunks: true,
		})
	}
	*metadataEvents = append(*metadataEvents, metadataEvent{
		oldEntry: sourceEntry,
		newEntry: newEntry,
	})

	if moveFolderSubEntries != nil {
		if moveChildrenErr := moveFolderSubEntries(); moveChildrenErr != nil {
			return moveChildrenErr
		}
	}

	// delete old entry
	ctx = context.WithValue(ctx, "OP", "MV")
	deleteErr := fs.filer.DeleteEntryMetaAndData(filer.WithSuppressedMetadataEvents(ctx), oldPath, false, false, false, false, signatures, 0)
	if deleteErr != nil {
		return deleteErr
	}
	if stream != nil {
		if err := stream.Send(&filer_pb.StreamRenameEntryResponse{
			Directory: string(oldParent),
			EventNotification: &filer_pb.EventNotification{
				OldEntry: &filer_pb.Entry{
					Name: entry.Name(),
				},
				NewEntry:           nil,
				DeleteChunks:       false,
				NewParentPath:      "",
				IsFromOtherCluster: false,
				Signatures:         nil,
			},
			TsNs: time.Now().UnixNano(),
		}); err != nil {
			return err
		}
	}

	return nil

}

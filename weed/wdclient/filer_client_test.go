package wdclient

import (
	"sync/atomic"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb"
)

func newTestFilerClient(addrs ...pb.ServerAddress) *FilerClient {
	health := make([]*filerHealth, len(addrs))
	for i := range health {
		health[i] = &filerHealth{}
	}
	return &FilerClient{
		filerAddresses: addrs,
		filerHealth:    health,
		vidMapClient:   newVidMapClient(nil, "", 0),
	}
}

func filerAddressList(fc *FilerClient) []pb.ServerAddress {
	out := make([]pb.ServerAddress, len(fc.filerAddresses))
	copy(out, fc.filerAddresses)
	return out
}

func TestApplyDiscoveredFilersPrunesStaleAddress(t *testing.T) {
	a := pb.ServerAddress("10.0.0.1:18888")
	b := pb.ServerAddress("10.0.0.2:18888") // gets replaced by c
	c := pb.ServerAddress("10.0.0.3:18888")

	fc := newTestFilerClient(a, b)
	// Give b a non-trivial failure count so we can confirm it leaves with its health.
	atomic.StoreInt32(&fc.filerHealth[1].failureCount, 7)
	// Give a a known failure count so we can confirm survivor health is preserved.
	atomic.StoreInt32(&fc.filerHealth[0].failureCount, 2)
	atomic.StoreInt32(&fc.filerIndex, 1) // active filer is b

	fc.applyDiscoveredFilers(map[pb.ServerAddress]struct{}{
		a: {},
		c: {},
	})

	got := filerAddressList(fc)
	if len(got) != 2 || got[0] != a || got[1] != c {
		t.Fatalf("expected [%s %s], got %v", a, c, got)
	}
	if len(fc.filerHealth) != 2 {
		t.Fatalf("expected 2 health entries, got %d", len(fc.filerHealth))
	}
	if got := atomic.LoadInt32(&fc.filerHealth[0].failureCount); got != 2 {
		t.Errorf("survivor health was reset: want 2, got %d", got)
	}
	if got := atomic.LoadInt32(&fc.filerHealth[1].failureCount); got != 0 {
		t.Errorf("newly added filer should start with fresh health, got failureCount=%d", got)
	}
	// Active filer (b) disappeared; index must reset rather than dangle.
	if idx := atomic.LoadInt32(&fc.filerIndex); idx != 0 {
		t.Errorf("expected filerIndex reset to 0 after active filer removed, got %d", idx)
	}
}

func TestApplyDiscoveredFilersKeepsIndexOnSurvivor(t *testing.T) {
	a := pb.ServerAddress("10.0.0.1:18888") // gets replaced
	b := pb.ServerAddress("10.0.0.2:18888") // active, survives
	c := pb.ServerAddress("10.0.0.3:18888") // new

	fc := newTestFilerClient(a, b)
	atomic.StoreInt32(&fc.filerIndex, 1) // active filer is b

	fc.applyDiscoveredFilers(map[pb.ServerAddress]struct{}{
		b: {},
		c: {},
	})

	got := filerAddressList(fc)
	if len(got) != 2 || got[0] != b || got[1] != c {
		t.Fatalf("expected [%s %s], got %v", b, c, got)
	}
	// b moved to index 0 after a was pruned; index should follow.
	if idx := atomic.LoadInt32(&fc.filerIndex); idx != 0 {
		t.Errorf("expected filerIndex to follow surviving active filer to position 0, got %d", idx)
	}
}

func TestApplyDiscoveredFilersNoChangeIsNoop(t *testing.T) {
	a := pb.ServerAddress("10.0.0.1:18888")
	b := pb.ServerAddress("10.0.0.2:18888")

	fc := newTestFilerClient(a, b)
	originalHealthA := fc.filerHealth[0]
	originalHealthB := fc.filerHealth[1]

	fc.applyDiscoveredFilers(map[pb.ServerAddress]struct{}{
		a: {},
		b: {},
	})

	if fc.filerHealth[0] != originalHealthA || fc.filerHealth[1] != originalHealthB {
		t.Errorf("no-op refresh should not reallocate health entries")
	}
}

func TestOnVolumeLocationChangeDropsRetiredServer(t *testing.T) {
	fc := newTestFilerClient()
	retired := Location{Url: "retired:8080"}
	live := Location{Url: "live:8080"}
	fc.addLocation(7, retired)
	fc.addLocation(7, live)
	fc.addLocation(8, retired)

	fc.OnVolumeLocationChange(retired, nil, []uint32{7, 8, 9})

	locations, found := fc.GetLocations(7)
	if !found || len(locations) != 1 || locations[0].Url != live.Url {
		t.Errorf("vid 7 kept only the surviving location: found=%v %v", found, locations)
	}
	if locations, found := fc.GetLocations(8); found && len(locations) > 0 {
		t.Errorf("vid 8 lost its last location but stayed cached: %v", locations)
	}
}

func TestOnVidMapResetClearsCachedLocations(t *testing.T) {
	fc := newTestFilerClient()
	fc.addLocation(7, Location{Url: "live:8080"})

	fc.OnVidMapReset()

	if locations, found := fc.GetLocations(7); found && len(locations) > 0 {
		t.Errorf("vid 7 survived the reset: %v", locations)
	}
}

func TestOnVolumeLocationChangeReaddClearsTombstone(t *testing.T) {
	retired := Location{Url: "retired:8080"}
	fc := newTestFilerClient()
	fc.vidMapClient = newVidMapClient(&testLocationProvider{
		locations: map[string][]Location{"5": {retired}},
	}, "", 0)
	ctx := context.Background()

	fc.OnVolumeLocationChange(retired, nil, []uint32{5})
	if locations, _ := fc.LookupVolumeIdsWithFallback(ctx, []string{"5"}); len(locations["5"]) > 0 {
		t.Fatalf("removed location still served: %v", locations)
	}

	fc.OnVolumeLocationChange(retired, []uint32{5}, nil)
	locations, err := fc.LookupVolumeIdsWithFallback(ctx, []string{"5"})
	if err != nil || len(locations["5"]) != 1 || locations["5"][0].Url != retired.Url {
		t.Fatalf("re-added location not served again: %v, err %v", locations, err)
	}
}

func hookedFilerClient(mc *MasterClient, provider VolumeLocationProvider) *FilerClient {
	fc := newTestFilerClient()
	fc.vidMapClient = newVidMapClient(provider, "", 0)
	mc.SetOnVolumeLocationChangeFn(fc.OnVolumeLocationChange)
	mc.SetOnVidMapResetFn(fc.OnVidMapReset)
	return fc
}

func expectServed(t *testing.T, fc *FilerClient, vid string, want ...Location) {
	t.Helper()
	locations, err := fc.LookupVolumeIdsWithFallback(context.Background(), []string{vid})
	got := locations[vid]
	if len(want) == 0 {
		if len(got) != 0 {
			t.Fatalf("volume %s still served from %v", vid, got)
		}
		return
	}
	if err != nil || len(got) != len(want) {
		t.Fatalf("volume %s served from %v, err %v, want %v", vid, got, err, want)
	}
	for i := range want {
		if got[i].Url != want[i].Url {
			t.Fatalf("volume %s served from %v, want %v", vid, got, want)
		}
	}
}

// ec.decode mounts the regular copy on a shard server before the shards come
// off it, so the later shard removal must leave that copy servable.
func TestDecodedCopyOutlivesShardRemoval(t *testing.T) {
	a := Location{Url: "a:8080"}
	b := Location{Url: "b:8080"}
	provider := &testLocationProvider{locations: map[string][]Location{"5": {a, b}}}
	mc := moveClient()
	fc := hookedFilerClient(mc, provider)
	pushVolumeLocation(mc, a.Url, nil, nil, []uint32{5}, nil)
	pushVolumeLocation(mc, b.Url, nil, nil, []uint32{5}, nil)
	expectServed(t, fc, "5", a, b)

	pushVolumeLocation(mc, a.Url, []uint32{5}, nil, nil, nil)
	pushVolumeLocation(mc, b.Url, nil, nil, nil, []uint32{5})
	pushVolumeLocation(mc, a.Url, nil, nil, nil, []uint32{5})

	provider.locations["5"] = []Location{a, b}
	expectServed(t, fc, "5", a)
}

// ec.encode registers the shards before the regular copy is deleted from the
// same server, so the server stays a read candidate for the ec volume.
func TestShardServerOutlivesRegularRemoval(t *testing.T) {
	a := Location{Url: "a:8080"}
	provider := &testLocationProvider{locations: map[string][]Location{"5": {a}}}
	mc := moveClient()
	fc := hookedFilerClient(mc, provider)
	pushVolumeLocation(mc, a.Url, []uint32{5}, nil, nil, nil)
	expectServed(t, fc, "5", a)

	pushVolumeLocation(mc, a.Url, nil, nil, []uint32{5}, nil)
	pushVolumeLocation(mc, a.Url, nil, []uint32{5}, nil, nil)

	expectServed(t, fc, "5", a)
}

// The filer caches shard servers like regular copies, so the last shard
// leaving a server retires it from the cache too.
func TestRetiredShardServerLeavesCache(t *testing.T) {
	a := Location{Url: "a:8080"}
	b := Location{Url: "b:8080"}
	provider := &testLocationProvider{locations: map[string][]Location{"5": {a, b}}}
	mc := moveClient()
	fc := hookedFilerClient(mc, provider)
	pushVolumeLocation(mc, a.Url, nil, nil, []uint32{5}, nil)
	pushVolumeLocation(mc, b.Url, nil, nil, []uint32{5}, nil)
	expectServed(t, fc, "5", a, b)

	pushVolumeLocation(mc, a.Url, nil, nil, nil, []uint32{5})

	if cached, _ := fc.GetLocations(5); len(cached) != 1 || cached[0].Url != b.Url {
		t.Fatalf("cache holds %v, want only %v", cached, b)
	}
	fc.InvalidateCache("5,01")
	expectServed(t, fc, "5", b)
}

// A volume moved to another server while the old one stays up: the old
// server would proxy the read and succeed, so no failed read would ever
// evict it. The pushed removal has to.
func TestMovedVolumeLeavesOldServer(t *testing.T) {
	old := Location{Url: "old:8080"}
	moved := Location{Url: "new:8080"}
	provider := &testLocationProvider{locations: map[string][]Location{"5": {old}}}
	mc := moveClient()
	fc := hookedFilerClient(mc, provider)
	pushVolumeLocation(mc, old.Url, []uint32{5}, nil, nil, nil)
	expectServed(t, fc, "5", old)

	pushVolumeLocation(mc, moved.Url, []uint32{5}, nil, nil, nil)
	pushVolumeLocation(mc, old.Url, nil, []uint32{5}, nil, nil)

	provider.locations["5"] = []Location{old, moved}
	expectServed(t, fc, "5", moved)
}

// A server back at the same URL after a master reconnect is re-told as an
// add, which lifts the fence its earlier removal left.
func TestReconnectReplayLiftsFence(t *testing.T) {
	a := Location{Url: "a:8080"}
	provider := &testLocationProvider{locations: map[string][]Location{"5": {a}}}
	mc := moveClient()
	fc := hookedFilerClient(mc, provider)
	pushVolumeLocation(mc, a.Url, []uint32{5}, nil, nil, nil)
	pushVolumeLocation(mc, a.Url, nil, []uint32{5}, nil, nil)
	expectServed(t, fc, "5")

	mc.resetVidMap()
	pushVolumeLocation(mc, a.Url, []uint32{5}, nil, nil, nil)

	expectServed(t, fc, "5", a)
}

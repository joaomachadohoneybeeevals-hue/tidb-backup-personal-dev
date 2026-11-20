// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package driver

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/stretchr/testify/require"
	tikvstore "github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/testutils"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
)

type mockErrInterceptor struct {
	err error
}

func (m *mockErrInterceptor) OnGet(_ context.Context, _ kv.Snapshot, k kv.Key, _ ...kv.GetOption) (kv.ValueEntry, error) {
	return kv.ValueEntry{}, m.err
}

func (m *mockErrInterceptor) OnBatchGet(_ context.Context, _ kv.Snapshot, _ []kv.Key, _ ...kv.BatchGetOption) (map[string]kv.ValueEntry, error) {
	return nil, m.err
}

func (m *mockErrInterceptor) OnIter(_ kv.Snapshot, _ kv.Key, _ kv.Key) (kv.Iterator, error) {
	return nil, m.err
}

func (m *mockErrInterceptor) OnIterReverse(_ kv.Snapshot, _ kv.Key, _ kv.Key) (kv.Iterator, error) {
	return nil, m.err
}

func validCommitTS(t *testing.T, commitTS uint64) {
	require.Greater(t, commitTS, uint64(0))
	now := time.Now()
	tm := oracle.GetTimeFromTS(commitTS)
	require.InDelta(t, now.Unix(), tm.Unix(), 10)
}

func TestTxnGet(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Close())
	}()
	clearStoreData(t, store)

	prepareSnapshot(t, store, [][]any{{"k1", "v1"}})
	txn, err := store.Begin()
	require.NoError(t, err)
	require.NotNil(t, txn)

	// should return snapshot value if no dirty data
	entry, err := txn.Get(context.Background(), kv.Key("k1"))
	require.NoError(t, err)
	require.Equal(t, kv.NewValueEntry([]byte("v1"), 0), entry)
	// should return the CommitTS for option WithReturnCommitTS
	entry, err = txn.Get(context.Background(), kv.Key("k1"), kv.WithReturnCommitTS())
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), entry.Value)
	validCommitTS(t, entry.CommitTS)

	// insert but not commit
	err = txn.Set(kv.Key("k1"), kv.Key("v1+"))
	require.NoError(t, err)

	// should return dirty data if dirty data exists
	entry, err = txn.Get(context.Background(), kv.Key("k1"))
	require.NoError(t, err)
	require.Equal(t, kv.NewValueEntry([]byte("v1+"), 0), entry)
	// dirty data's commitTS should be 0
	entry, err = txn.Get(context.Background(), kv.Key("k1"), kv.WithReturnCommitTS())
	require.NoError(t, err)
	require.Equal(t, kv.NewValueEntry([]byte("v1+"), 0), entry)

	err = txn.Set(kv.Key("k2"), []byte("v2+"))
	require.NoError(t, err)

	// should return dirty data if dirty data exists
	entry, err = txn.Get(context.Background(), kv.Key("k2"))
	require.NoError(t, err)
	require.Equal(t, kv.NewValueEntry([]byte("v2+"), 0), entry)

	// delete but not commit
	err = txn.Delete(kv.Key("k1"))
	require.NoError(t, err)

	// should return kv.ErrNotExist if deleted
	entry, err = txn.Get(context.Background(), kv.Key("k1"))
	require.Equal(t, kv.ValueEntry{}, entry)
	require.True(t, kv.ErrNotExist.Equal(err))

	// should return kv.ErrNotExist if not exist
	entry, err = txn.Get(context.Background(), kv.Key("kn"))
	require.Equal(t, kv.ValueEntry{}, entry)
	require.True(t, kv.ErrNotExist.Equal(err))

	// should return kv.ErrNotExist if not exist (WithReturnCommitTS specified)
	entry, err = txn.Get(context.Background(), kv.Key("k1"), kv.WithReturnCommitTS())
	require.Equal(t, kv.ValueEntry{}, entry)
	require.True(t, kv.ErrNotExist.Equal(err))

	// make snapshot returns error
	errInterceptor := &mockErrInterceptor{err: errors.New("error")}
	txn.SetOption(kv.SnapInterceptor, errInterceptor)

	// should return kv.ErrNotExist because k1 is deleted in memBuff
	entry, err = txn.Get(context.Background(), kv.Key("k1"))
	require.Equal(t, kv.ValueEntry{}, entry)
	require.True(t, kv.ErrNotExist.Equal(err))

	// should return dirty data because k2 is in memBuff
	entry, err = txn.Get(context.Background(), kv.Key("k2"))
	require.NoError(t, err)
	require.Equal(t, kv.NewValueEntry([]byte("v2+"), 0), entry)

	// should return error because kn is read from snapshot
	entry, err = txn.Get(context.Background(), kv.Key("kn"))
	require.Equal(t, kv.ValueEntry{}, entry)
	require.Equal(t, errInterceptor.err, err)
}

func TestTxnBatchGet(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Close())
	}()
	clearStoreData(t, store)

	prepareSnapshot(t, store, [][]any{{"k1", "v1"}, {"k2", "v2"}, {"k3", "v3"}, {"k4", "v4"}})
	txn, err := store.Begin()
	require.NoError(t, err)

	// Get the snapshot commit ts
	entry, err := txn.Get(context.Background(), kv.Key("k1"), kv.WithReturnCommitTS())
	require.NoError(t, err)
	validCommitTS(t, entry.CommitTS)
	commitTS := entry.CommitTS

	// Test BatchGet from snapshot
	result, err := txn.BatchGet(context.Background(), []kv.Key{kv.Key("k1"), kv.Key("k2"), kv.Key("k3"), kv.Key("kn")})
	require.NoError(t, err)
	require.Equal(t, 3, len(result))
	require.Equal(t, kv.NewValueEntry([]byte("v1"), 0), result["k1"])
	require.Equal(t, kv.NewValueEntry([]byte("v2"), 0), result["k2"])
	require.Equal(t, kv.NewValueEntry([]byte("v3"), 0), result["k3"])

	// Test BatchGet from snapshot with WithReturnCommitTS option
	result, err = txn.BatchGet(context.Background(), []kv.Key{kv.Key("k1"), kv.Key("k2"), kv.Key("k3"), kv.Key("kn")}, kv.WithReturnCommitTS())
	require.NoError(t, err)
	require.Equal(t, 3, len(result))
	require.Equal(t, kv.NewValueEntry([]byte("v1"), commitTS), result["k1"])
	require.Equal(t, kv.NewValueEntry([]byte("v2"), commitTS), result["k2"])
	require.Equal(t, kv.NewValueEntry([]byte("v3"), commitTS), result["k3"])

	// make some dirty data
	err = txn.Set(kv.Key("k1"), []byte("v1+"))
	require.NoError(t, err)
	err = txn.Set(kv.Key("k4"), []byte("v4+"))
	require.NoError(t, err)
	err = txn.Delete(kv.Key("k2"))
	require.NoError(t, err)

	result, err = txn.BatchGet(context.Background(), []kv.Key{kv.Key("k1"), kv.Key("k2"), kv.Key("k3"), kv.Key("k4"), kv.Key("kn")})
	require.NoError(t, err)
	require.Equal(t, 3, len(result))
	require.Equal(t, kv.NewValueEntry([]byte("v1+"), 0), result["k1"])
	require.Equal(t, kv.NewValueEntry([]byte("v3"), 0), result["k3"])
	require.Equal(t, kv.NewValueEntry([]byte("v4+"), 0), result["k4"])

	// return data if not read from snapshot
	result, err = txn.BatchGet(context.Background(), []kv.Key{kv.Key("k1"), kv.Key("k4")})
	require.NoError(t, err)
	require.Equal(t, 2, len(result))
	require.Equal(t, kv.NewValueEntry([]byte("v1+"), 0), result["k1"])
	require.Equal(t, kv.NewValueEntry([]byte("v4+"), 0), result["k4"])

	// test WithReturnCommitTS option
	result, err = txn.BatchGet(context.Background(), []kv.Key{kv.Key("k1"), kv.Key("k2"), kv.Key("k3"), kv.Key("k4")}, kv.WithReturnCommitTS())
	require.Equal(t, 3, len(result))
	require.Equal(t, kv.NewValueEntry([]byte("v1+"), 0), result["k1"])
	require.Equal(t, kv.NewValueEntry([]byte("v3"), commitTS), result["k3"])
	require.Equal(t, kv.NewValueEntry([]byte("v4+"), 0), result["k4"])

	// make snapshot returns error
	errInterceptor := &mockErrInterceptor{err: errors.New("error")}
	txn.SetOption(kv.SnapInterceptor, errInterceptor)

	// fails if read from snapshot
	result, err = txn.BatchGet(context.Background(), []kv.Key{kv.Key("k3")})
	require.Nil(t, result)
	require.Equal(t, errInterceptor.err, err)
	result, err = txn.BatchGet(context.Background(), []kv.Key{kv.Key("k1"), kv.Key("k3"), kv.Key("k4")})
	require.Nil(t, result)
	require.Equal(t, errInterceptor.err, err)
	result, err = txn.BatchGet(context.Background(), []kv.Key{kv.Key("k1"), kv.Key("k4"), kv.Key("kn")})
	require.Nil(t, result)
	require.Equal(t, errInterceptor.err, err)
}

func TestTxnScan(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Close())
	}()
	clearStoreData(t, store)

	prepareSnapshot(t, store, [][]any{{"k1", "v1"}, {"k3", "v3"}, {"k5", "v5"}, {"k7", "v7"}, {"k9", "v9"}})
	txn, err := store.Begin()
	require.NoError(t, err)

	iter, err := txn.Iter(kv.Key("k3"), kv.Key("k9"))
	require.NoError(t, err)
	checkIter(t, iter, [][]any{{"k3", "v3"}, {"k5", "v5"}, {"k7", "v7"}})

	iter, err = txn.IterReverse(kv.Key("k9"), nil)
	require.NoError(t, err)
	checkIter(t, iter, [][]any{{"k7", "v7"}, {"k5", "v5"}, {"k3", "v3"}, {"k1", "v1"}})

	iter, err = txn.IterReverse(kv.Key("k9"), kv.Key("k3"))
	require.NoError(t, err)
	checkIter(t, iter, [][]any{{"k7", "v7"}, {"k5", "v5"}, {"k3", "v3"}})

	// make some dirty data
	err = txn.Set(kv.Key("k1"), []byte("v1+"))
	require.NoError(t, err)
	err = txn.Set(kv.Key("k3"), []byte("v3+"))
	require.NoError(t, err)
	err = txn.Set(kv.Key("k31"), []byte("v31+"))
	require.NoError(t, err)
	err = txn.Delete(kv.Key("k5"))
	require.NoError(t, err)

	iter, err = txn.Iter(kv.Key("k3"), kv.Key("k9"))
	require.NoError(t, err)
	checkIter(t, iter, [][]any{{"k3", "v3+"}, {"k31", "v31+"}, {"k7", "v7"}})

	iter, err = txn.IterReverse(kv.Key("k9"), nil)
	require.NoError(t, err)
	checkIter(t, iter, [][]any{{"k7", "v7"}, {"k31", "v31+"}, {"k3", "v3+"}, {"k1", "v1+"}})

	// make snapshot returns error
	errInterceptor := &mockErrInterceptor{err: errors.New("error")}
	txn.SetOption(kv.SnapInterceptor, errInterceptor)

	iter, err = txn.Iter(kv.Key("k1"), kv.Key("k2"))
	require.Equal(t, errInterceptor.err, err)
	require.Nil(t, iter)
}

type lockReq struct {
	keys     [][]byte
	regionID uint64
	epoch    *metapb.RegionEpoch
	// rejected is true when the store answered with a region error.
	rejected bool
}

func (r lockReq) hasKey(k []byte) bool {
	for _, key := range r.keys {
		if string(key) == string(k) {
			return true
		}
	}
	return false
}

// routingViolations returns the lock requests for k that the store accepted
// under a region other than owner at its current epoch.
func routingViolations(reqs []lockReq, k []byte, owner *metapb.Region) []lockReq {
	var violations []lockReq
	for _, r := range reqs {
		if r.rejected || !r.hasKey(k) {
			continue
		}
		if r.regionID != owner.GetId() || r.epoch.GetVersion() != owner.GetRegionEpoch().GetVersion() {
			violations = append(violations, r)
		}
	}
	return violations
}

// lockReqRecorder records the region routing carried by every pessimistic
// lock request. It looks at pessimistic lock requests only; every other
// request type passes through untouched.
type lockReqRecorder struct {
	tikv.Client
	mu   sync.Mutex
	reqs []lockReq

	// When misrouteTo is set, the next pessimistic lock request for
	// misrouteKey is sent to misrouteTo instead of the region the client chose.
	misrouteKey  []byte
	misrouteTo   *metapb.Region
	misroutePeer *metapb.Peer
}

func (c *lockReqRecorder) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	if req.Type != tikvrpc.CmdPessimisticLock {
		return c.Client.SendRequest(ctx, addr, req, timeout)
	}
	r := lockReq{}
	for _, m := range req.PessimisticLock().Mutations {
		r.keys = append(r.keys, m.Key)
	}
	c.mu.Lock()
	if c.misrouteTo != nil && r.hasKey(c.misrouteKey) {
		req.RegionId = c.misrouteTo.GetId()
		req.RegionEpoch = c.misrouteTo.GetRegionEpoch()
		req.Peer = c.misroutePeer
		c.misrouteTo = nil
	}
	c.mu.Unlock()

	resp, err := c.Client.SendRequest(ctx, addr, req, timeout)
	if err == nil {
		regionErr, _ := resp.GetRegionError()
		r.regionID, r.epoch, r.rejected = req.RegionId, req.RegionEpoch, regionErr != nil
		c.mu.Lock()
		c.reqs = append(c.reqs, r)
		c.mu.Unlock()
	}
	return resp, err
}

func (c *lockReqRecorder) misrouteNext(k []byte, region *metapb.Region, peer *metapb.Peer) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.misrouteKey, c.misrouteTo, c.misroutePeer = k, region, peer
}

func (c *lockReqRecorder) take() []lockReq {
	c.mu.Lock()
	defer c.mu.Unlock()
	reqs := c.reqs
	c.reqs = nil
	return reqs
}

// splitLockEnv is a mock store whose only region has been split after the
// region cache was warmed with the pre-split region.
type splitLockEnv struct {
	store   kv.Storage
	cluster testutils.Cluster
	rec     *lockReqRecorder
	// keys holds one key on each side of the split, so whichever half keeps
	// the old region ID is covered.
	keys []kv.Key
	// oldRegionID is the ID of the pre-split region; one half keeps it.
	oldRegionID uint64
}

func newSplitLockEnv(t *testing.T, refreshCache bool) *splitLockEnv {
	env := &splitLockEnv{
		rec:  &lockReqRecorder{},
		keys: []kv.Key{kv.Key("a"), kv.Key("z")},
	}
	var err error
	env.store, err = mockstore.NewMockStore(
		mockstore.WithClusterInspector(func(c testutils.Cluster) {
			_, _, env.oldRegionID = mockstore.BootstrapWithSingleStore(c)
			env.cluster = c
		}),
		mockstore.WithClientHijacker(func(c tikv.Client) tikv.Client {
			env.rec.Client = c
			return env.rec
		}),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, env.store.Close())
	})

	// Warm the region cache with the pre-split region.
	for _, k := range env.keys {
		txn, err := env.lock(t, k, tikvstore.LockAlwaysWait)
		require.NoError(t, err)
		require.NoError(t, txn.Rollback())
	}

	newRegionID, newPeerID := env.cluster.AllocID(), env.cluster.AllocID()
	env.cluster.Split(env.oldRegionID, newRegionID, []byte("m"), []uint64{newPeerID}, newPeerID)

	if refreshCache {
		// The region cache already reflects the split when the lock request
		// is issued.
		cache := env.store.(interface {
			GetRegionCache() *tikv.RegionCache
		}).GetRegionCache()
		bo := tikv.NewBackofferWithVars(context.Background(), 5000, nil)
		for _, k := range env.keys {
			loc, err := cache.LocateKey(bo, k)
			require.NoError(t, err)
			cache.InvalidateCachedRegion(loc.Region)
			_, err = cache.LocateKey(bo, k)
			require.NoError(t, err)
		}
	}
	env.rec.take()
	return env
}

func (env *splitLockEnv) lock(t *testing.T, k kv.Key, waitTime int64) (kv.Transaction, error) {
	txn, err := env.store.Begin()
	require.NoError(t, err)
	txn.SetOption(kv.Pessimistic, true)
	lockCtx := tikvstore.NewLockCtx(txn.StartTS(), waitTime, time.Now())
	if err := txn.LockKeys(context.Background(), lockCtx, k); err != nil {
		require.NoError(t, txn.Rollback())
		return nil, err
	}
	return txn, nil
}

// owner returns the region that owns k according to the mock cluster.
func (env *splitLockEnv) owner(t *testing.T, k kv.Key) (*metapb.Region, *metapb.Peer) {
	region, peer, _, _ := env.cluster.GetRegionByKey(codec.EncodeBytes(nil, k))
	require.NotNil(t, region)
	return region, peer
}

// TestPessimisticLockAcrossSplit guards the routing invariant documented on
// tikvTxn.LockKeys: a pessimistic lock on a key whose region has just changed
// must be acquired on the region that owns the key now.
func TestPessimisticLockAcrossSplit(t *testing.T) {
	for _, refreshCache := range []bool{false, true} {
		t.Run(fmt.Sprintf("refreshCache=%v", refreshCache), func(t *testing.T) {
			env := newSplitLockEnv(t, refreshCache)

			sawRejection := false
			for _, k := range env.keys {
				holder, err := env.lock(t, k, tikvstore.LockAlwaysWait)
				require.NoError(t, err)

				// Every request the store accepted must name the region that
				// owns the key now, at its current epoch.
				owner, _ := env.owner(t, k)
				reqs := env.rec.take()
				require.Empty(t, routingViolations(reqs, k, owner))
				accepted := 0
				for _, r := range reqs {
					if !r.hasKey(k) {
						continue
					}
					if r.rejected {
						sawRejection = true
						continue
					}
					accepted++
				}
				require.Equal(t, 1, accepted)

				// Release the lock before taking the next one, so no lock
				// request in this test ever meets another transaction's lock.
				require.NoError(t, holder.Rollback())
			}
			if !refreshCache {
				// A stale cache entry must be caught by the epoch check and retried.
				require.True(t, sawRejection)
			}
		})
	}

	// Negative control: prove the routing check above fails when a lock is
	// accepted under the stale region. The client routes correctly here, so
	// the misroute is injected at the RPC layer.
	t.Run("misroutedLockIsDetected", func(t *testing.T) {
		env := newSplitLockEnv(t, true)

		// One half of the split keeps the old region ID. Take the key that
		// moved to the new region, and the region that kept the old ID.
		var (
			moved        kv.Key
			owner, stale *metapb.Region
			stalePeer    *metapb.Peer
		)
		for _, k := range env.keys {
			region, peer := env.owner(t, k)
			if region.GetId() == env.oldRegionID {
				stale, stalePeer = region, peer
			} else {
				moved, owner = k, region
			}
		}
		require.NotNil(t, stale)
		require.NotNil(t, owner)

		// Send the lock for the moved key to the old region at that region's
		// current epoch, so the store's epoch check cannot reject it.
		env.rec.misrouteNext(moved, stale, stalePeer)
		holder, err := env.lock(t, moved, tikvstore.LockAlwaysWait)
		require.NoError(t, err)

		violations := routingViolations(env.rec.take(), moved, owner)
		require.Len(t, violations, 1, "the routing check must flag a lock accepted under the old region")
		require.Equal(t, stale.GetId(), violations[0].regionID)

		require.NoError(t, holder.Rollback())
	})
}

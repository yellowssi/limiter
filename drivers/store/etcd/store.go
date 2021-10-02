package etcd

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/client/v3"

	"github.com/ulule/limiter/v3"
	"github.com/ulule/limiter/v3/drivers/store/common"
)

// Store is the etcd store.
type Store struct {
	// Prefix used for the key.
	Prefix string
	// client used to communicate with redis server.
	client *clientv3.Client
}

// NewStore returns an instance of etcd store with defaults.
func NewStore(client *clientv3.Client) limiter.Store {
	return NewStoreWithOptions(client, limiter.StoreOptions{
		Prefix:          limiter.DefaultPrefix,
		CleanUpInterval: limiter.DefaultCleanUpInterval,
	})
}

// NewStoreWithOptions returns an instance of etcd store with options.
func NewStoreWithOptions(client *clientv3.Client, options limiter.StoreOptions) limiter.Store {
	return &Store{
		client: client,
		Prefix: options.Prefix,
	}
}

// Get returns the limit for given identifier
func (store *Store) Get(ctx context.Context, key string, rate limiter.Rate) (limiter.Context, error) {
	key = fmt.Sprintf("%s/%s", store.Prefix, key)
	count, ttl, err := store.Increment(ctx, key, 1, int64(rate.Period.Seconds()))
	if err != nil {
		return limiter.Context{}, err
	}

	now := time.Now()
	expiration := now.Add(rate.Period)
	if ttl > 0 {
		expiration = now.Add(time.Duration(ttl) * time.Millisecond)
	}

	return common.GetContextFromState(now, rate, expiration, count), nil
}

// Peek returns the limit for given identifier, without modification on current values.
func (store *Store) Peek(ctx context.Context, key string, rate limiter.Rate) (limiter.Context, error) {
	key = fmt.Sprintf("%s/%s", store.Prefix, key)
	count, ttl, err := GetKeyCountExpiration(ctx, store.client, key)
	if err != nil {
		return limiter.Context{}, err
	}

	now := time.Now()
	expiration := now.Add(rate.Period)
	if ttl > 0 {
		expiration = now.Add(time.Duration(ttl) * time.Millisecond)
	}

	return common.GetContextFromState(now, rate, expiration, count), nil
}

// Reset returns the limit for given identifier which is set to zero.
func (store *Store) Reset(ctx context.Context, key string, rate limiter.Rate) (limiter.Context, error) {
	key = fmt.Sprintf("%s/%s", store.Prefix, key)
	_, err := store.client.Delete(ctx, key)
	if err != nil {
		return limiter.Context{}, err
	}

	count := int64(0)
	now := time.Now()
	expiration := now.Add(rate.Period)

	return common.GetContextFromState(now, rate, expiration, count), nil
}

// Increment increments given value on key.
// If key is undefined or expired, it will create it.
func (store *Store) Increment(ctx context.Context, key string, count int64, ttl int64) (int64, int64, error) {
	rsp, err := store.client.Get(ctx, key)
	if err != nil {
		return 0, 0, err
	}
	if ttl > 0 {
		for i := 0; i < int(count); i++ {
			if err = PutKeyLease(ctx, store.client, key, ttl); err != nil {
				return 0, 0, err
			}
		}
	}
	if rsp.Count == 0 {
		if ttl > 0 {
			if err = PutKeyWithExpiration(ctx, store.client, key, "", ttl); err != nil {
				return 0, 0, err
			}
		}
		return count, ttl, nil
	}
	ttl, err = getExpiration(ctx, store.client, *rsp.Kvs[0])
	if err != nil {
		return 0, 0, err
	}
	count, err = GetKeyCount(ctx, store.client, key)
	if err != nil {
		return 0, 0, err
	}
	return count, ttl, nil
}

func PutKeyWithExpiration(ctx context.Context, client *clientv3.Client, key string, value string, expiration int64) error {
	lease, err := client.Grant(ctx, expiration)
	if err != nil {
		return err
	}
	_, err = client.Put(ctx, key, value, clientv3.WithLease(lease.ID))
	return err
}

func PutKeyLease(ctx context.Context, client *clientv3.Client, key string, expiration int64) error {
	id := uuid.New()
	return PutKeyWithExpiration(ctx, client, key+"/"+id.String(), "", expiration)
}

func GetKeyCountExpiration(ctx context.Context, client *clientv3.Client, key string) (int64, int64, error) {
	rsp, err := client.Get(ctx, key)
	if err != nil {
		return 0, 0, err
	}
	if rsp.Count == 0 {
		return 0, 0, nil
	}
	count, err := GetKeyCount(ctx, client, key)
	if err != nil {
		return 0, 0, err
	}
	ttl, err := getExpiration(ctx, client, *rsp.Kvs[0])
	if err != nil {
		return 0, 0, err
	}
	return count, ttl, nil
}

func GetKeyCount(ctx context.Context, client *clientv3.Client, key string) (int64, error) {
	rsp, err := client.Get(ctx, key+"/", clientv3.WithPrefix(), clientv3.WithKeysOnly())
	if err != nil {
		return 0, err
	}
	return rsp.Count, nil
}

func GetKeyExpiration(ctx context.Context, client *clientv3.Client, key string) (int64, error) {
	rsp, err := client.Get(ctx, key)
	if err != nil {
		return 0, err
	}
	if rsp.Count == 0 {
		return 0, nil
	}
	ttl, err := getExpiration(ctx, client, *rsp.Kvs[0])
	if err != nil {
		return 0, err
	}
	return ttl, nil
}

func getExpiration(ctx context.Context, client *clientv3.Client, kv mvccpb.KeyValue) (int64, error) {
	var ttl = int64(0)
	// parse ttl
	leaseID := clientv3.LeaseID(kv.Lease)
	if leaseID != clientv3.NoLease {
		ttlResponse, err := client.TimeToLive(ctx, leaseID)
		if err != nil {
			return 0, err
		}
		ttl = ttlResponse.TTL
	}

	return ttl, nil
}

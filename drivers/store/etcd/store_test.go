package etcd_test

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/ulule/limiter/v3"
	"github.com/ulule/limiter/v3/drivers/store/etcd"
	"github.com/ulule/limiter/v3/drivers/store/tests"
)

func TestEtcdStoreSequentialAccess(t *testing.T) {
	is := require.New(t)

	client, err := newEtcdClient()
	is.NoError(err)
	is.NotNil(client)

	store := etcd.NewStoreWithOptions(client, limiter.StoreOptions{
		Prefix: "/limiter-etcd/sequential-test",
	})
	is.NotNil(store)

	tests.TestStoreSequentialAccess(t, store)
}

func TestEtcdStoreConcurrentAccess(t *testing.T) {
	is := require.New(t)

	client, err := newEtcdClient()
	is.NoError(err)
	is.NotNil(client)

	store := etcd.NewStoreWithOptions(client, limiter.StoreOptions{
		Prefix: "/limiter-etcd/concurrent-test",
	})
	is.NotNil(store)

	tests.TestStoreConcurrentAccess(t, store)
}

func TestEtcdClientExpiration(t *testing.T) {
	is := require.New(t)

	client, err := newEtcdClient()
	is.NoError(err)
	is.NotNil(client)

	key := "foobar"
	value := int64(642)
	keyNoExpiration := int64(-1)
	keyNotExist := int64(-2)

	ctx := context.Background()
	_, err = client.Delete(ctx, key)
	is.NoError(err)

	ttl, err := etcd.GetKeyExpiration(ctx, client, key)
	is.NoError(err)
	is.Equal(keyNotExist, ttl)

	_, err = client.Put(ctx, key, strconv.FormatInt(value, 10))
	is.NoError(err)

	ttl, err = etcd.GetKeyExpiration(ctx, client, key)
	is.NoError(err)
	is.Equal(keyNoExpiration, ttl)

	err = etcd.PutKeyWithExpiration(ctx, client, key, strconv.FormatInt(value, 10), 1)
	is.NoError(err)

	time.Sleep(100 * time.Millisecond)

	ttl, err = etcd.GetKeyExpiration(ctx, client, key)
	is.NoError(err)

	expected := int64(0)
	actual := ttl
	is.Greater(actual, expected)
}

func newEtcdClient() (*clientv3.Client, error) {
	endpoint := "localhost:2379"
	if os.Getenv("ETCD_ENDPOINTS") != "" {
		endpoint = os.Getenv("ETCD_ENDPOINT")
	}

	return clientv3.New(clientv3.Config{
		Endpoints: []string{endpoint},
	})
}

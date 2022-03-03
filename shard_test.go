package s3ds

import (
	"fmt"
	"testing"

	ds "github.com/ipfs/go-datastore"
)

func TestIdentity(t *testing.T) {
	cases := []struct {
		shardFunc string
		cid       string
		s3Key     string
	}{
		{
			shardFunc: "/repo/s3/shard/v1/identity/0",
			cid:       "a",
			s3Key:     "a",
		},
		{
			shardFunc: "/repo/s3/shard/v1/identity/1",
			cid:       "a",
			s3Key:     "a",
		},
		{
			shardFunc: "/repo/s3/shard/v1/identity/2",
			cid:       "ab",
			s3Key:     "ab",
		},
		{
			shardFunc: "/repo/s3/shard/v1/identity/1",
			cid:       "abc",
			s3Key:     "abc",
		},
		{
			shardFunc: "/repo/s3/shard/v1/identity/0",
			cid:       "abcd",
			s3Key:     "abcd",
		},
		{
			shardFunc: "/repo/s3/shard/v1/identity/100000000",
			cid:       "CIQJ7IHPGOFUJT5UMXIW6CUDSNH6AVKMEOXI3UM3VLYJRZUISUMGCXQ",
			s3Key:     "CIQJ7IHPGOFUJT5UMXIW6CUDSNH6AVKMEOXI3UM3VLYJRZUISUMGCXQ",
		},
	}
	for _, c := range cases {
		t.Run(fmt.Sprintf("%s %s", c.shardFunc, c.cid), func(t *testing.T) {
			id, parseErr := ParseShardFunc(c.shardFunc)
			if parseErr != nil {
				t.Fatalf(parseErr.Error())
			}

			k := ds.NewKey(c.cid)
			noslash := k.String()[1:]
			s3Key := id.fun(noslash)
			if s3Key != c.s3Key {
				t.Fatalf("expected %s, got %s", c.s3Key, s3Key)
			}
		})
	}
}

func TestPrefix(t *testing.T) {
	cases := []struct {
		shardFunc string
		cid       string
		s3Key     string
	}{
		{
			shardFunc: "/repo/s3/shard/v1/prefix/2",
			cid:       "a",
			s3Key:     "a_",
		},
		{
			shardFunc: "/repo/s3/shard/v1/prefix/2",
			cid:       "ab",
			s3Key:     "ab",
		},
		{
			shardFunc: "/repo/s3/shard/v1/prefix/2",
			cid:       "abc",
			s3Key:     "ab",
		},
		{
			shardFunc: "/repo/s3/shard/v1/prefix/2",
			cid:       "abcd",
			s3Key:     "ab",
		},
		{
			shardFunc: "/repo/s3/shard/v1/prefix/3",
			cid:       "CIQJ7IHPGOFUJT5UMXIW6CUDSNH6AVKMEOXI3UM3VLYJRZUISUMGCXQ",
			s3Key:     "CIQ",
		},
		{
			shardFunc: "/repo/s3/shard/v1/prefix/8",
			cid:       "CIQJ7IHPGOFUJT5UMXIW6CUDSNH6AVKMEOXI3UM3VLYJRZUISUMGCXQ",
			s3Key:     "CIQJ7IHP",
		},
	}
	for _, c := range cases {
		t.Run(fmt.Sprintf("%s %s", c.shardFunc, c.cid), func(t *testing.T) {
			id, parseErr := ParseShardFunc(c.shardFunc)
			if parseErr != nil {
				t.Fatalf(parseErr.Error())
			}

			k := ds.NewKey(c.cid)
			noslash := k.String()[1:]
			s3Key := id.fun(noslash)
			if s3Key != c.s3Key {
				t.Fatalf("expected %s, got %s", c.s3Key, s3Key)
			}
		})
	}
}

func TestSuffix(t *testing.T) {
	cases := []struct {
		shardFunc string
		cid       string
		s3Key     string
	}{
		{
			shardFunc: "/repo/s3/shard/v1/suffix/2",
			cid:       "a",
			s3Key:     "_a",
		},
		{
			shardFunc: "/repo/s3/shard/v1/suffix/2",
			cid:       "ab",
			s3Key:     "ab",
		},
		{
			shardFunc: "/repo/s3/shard/v1/suffix/2",
			cid:       "abc",
			s3Key:     "bc",
		},
		{
			shardFunc: "/repo/s3/shard/v1/suffix/2",
			cid:       "abcd",
			s3Key:     "cd",
		},
		{
			shardFunc: "/repo/s3/shard/v1/suffix/3",
			cid:       "CIQJ7IHPGOFUJT5UMXIW6CUDSNH6AVKMEOXI3UM3VLYJRZUISUMGCXQ",
			s3Key:     "CXQ",
		},
		{
			shardFunc: "/repo/s3/shard/v1/suffix/8",
			cid:       "CIQJ7IHPGOFUJT5UMXIW6CUDSNH6AVKMEOXI3UM3VLYJRZUISUMGCXQ",
			s3Key:     "ISUMGCXQ",
		},
	}
	for _, c := range cases {
		t.Run(fmt.Sprintf("%s %s", c.shardFunc, c.cid), func(t *testing.T) {
			id, parseErr := ParseShardFunc(c.shardFunc)
			if parseErr != nil {
				t.Fatalf(parseErr.Error())
			}

			k := ds.NewKey(c.cid)
			noslash := k.String()[1:]
			s3Key := id.fun(noslash)
			if s3Key != c.s3Key {
				t.Fatalf("expected %s, got %s", c.s3Key, s3Key)
			}
		})
	}
}

func TestNextToLast(t *testing.T) {
	cases := []struct {
		shardFunc string
		cid       string
		s3Key     string
	}{
		{
			shardFunc: "/repo/s3/shard/v1/next-to-last/2",
			cid:       "a",
			s3Key:     "__",
		},
		{
			shardFunc: "/repo/s3/shard/v1/next-to-last/2",
			cid:       "ab",
			s3Key:     "_a",
		},
		{
			shardFunc: "/repo/s3/shard/v1/next-to-last/2",
			cid:       "abc",
			s3Key:     "ab",
		},
		{
			shardFunc: "/repo/s3/shard/v1/next-to-last/2",
			cid:       "abcd",
			s3Key:     "bc",
		},
		{
			shardFunc: "/repo/s3/shard/v1/next-to-last/3",
			cid:       "CIQJ7IHPGOFUJT5UMXIW6CUDSNH6AVKMEOXI3UM3VLYJRZUISUMGCXQ",
			s3Key:     "GCX",
		},
		{
			shardFunc: "/repo/s3/shard/v1/next-to-last/8",
			cid:       "CIQJ7IHPGOFUJT5UMXIW6CUDSNH6AVKMEOXI3UM3VLYJRZUISUMGCXQ",
			s3Key:     "UISUMGCX",
		},
	}
	for _, c := range cases {
		t.Run(fmt.Sprintf("%s %s", c.shardFunc, c.cid), func(t *testing.T) {
			id, parseErr := ParseShardFunc(c.shardFunc)
			if parseErr != nil {
				t.Fatalf(parseErr.Error())
			}

			k := ds.NewKey(c.cid)
			noslash := k.String()[1:]
			s3Key := id.fun(noslash)
			if s3Key != c.s3Key {
				t.Fatalf("expected %s, got %s", c.s3Key, s3Key)
			}
		})
	}
}

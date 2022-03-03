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
		s3Dir     string
	}{
		{
			shardFunc: "/repo/s3/shard/v1/identity/0",
			cid:       "a",
			s3Dir:     "",
		},
		{
			shardFunc: "/repo/s3/shard/v1/identity/1",
			cid:       "a",
			s3Dir:     "",
		},
		{
			shardFunc: "/repo/s3/shard/v1/identity/2",
			cid:       "ab",
			s3Dir:     "",
		},
		{
			shardFunc: "/repo/s3/shard/v1/identity/1",
			cid:       "abc",
			s3Dir:     "",
		},
		{
			shardFunc: "/repo/s3/shard/v1/identity/0",
			cid:       "abcd",
			s3Dir:     "",
		},
		{
			shardFunc: "/repo/s3/shard/v1/identity/100000000",
			cid:       "CIQJ7IHPGOFUJT5UMXIW6CUDSNH6AVKMEOXI3UM3VLYJRZUISUMGCXQ",
			s3Dir:     "",
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
			s3Dir := id.fun(noslash)
			if s3Dir != c.s3Dir {
				t.Fatalf("expected %s, got %s", c.s3Dir, s3Dir)
			}
		})
	}
}

func TestPrefix(t *testing.T) {
	cases := []struct {
		shardFunc string
		cid       string
		s3Dir     string
	}{
		{
			shardFunc: "/repo/s3/shard/v1/prefix/2",
			cid:       "a",
			s3Dir:     "a_",
		},
		{
			shardFunc: "/repo/s3/shard/v1/prefix/2",
			cid:       "ab",
			s3Dir:     "ab",
		},
		{
			shardFunc: "/repo/s3/shard/v1/prefix/2",
			cid:       "abc",
			s3Dir:     "ab",
		},
		{
			shardFunc: "/repo/s3/shard/v1/prefix/2",
			cid:       "abcd",
			s3Dir:     "ab",
		},
		{
			shardFunc: "/repo/s3/shard/v1/prefix/3",
			cid:       "CIQJ7IHPGOFUJT5UMXIW6CUDSNH6AVKMEOXI3UM3VLYJRZUISUMGCXQ",
			s3Dir:     "CIQ",
		},
		{
			shardFunc: "/repo/s3/shard/v1/prefix/8",
			cid:       "CIQJ7IHPGOFUJT5UMXIW6CUDSNH6AVKMEOXI3UM3VLYJRZUISUMGCXQ",
			s3Dir:     "CIQJ7IHP",
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
			s3Dir := id.fun(noslash)
			if s3Dir != c.s3Dir {
				t.Fatalf("expected %s, got %s", c.s3Dir, s3Dir)
			}
		})
	}
}

func TestSuffix(t *testing.T) {
	cases := []struct {
		shardFunc string
		cid       string
		s3Dir     string
	}{
		{
			shardFunc: "/repo/s3/shard/v1/suffix/2",
			cid:       "a",
			s3Dir:     "_a",
		},
		{
			shardFunc: "/repo/s3/shard/v1/suffix/2",
			cid:       "ab",
			s3Dir:     "ab",
		},
		{
			shardFunc: "/repo/s3/shard/v1/suffix/2",
			cid:       "abc",
			s3Dir:     "bc",
		},
		{
			shardFunc: "/repo/s3/shard/v1/suffix/2",
			cid:       "abcd",
			s3Dir:     "cd",
		},
		{
			shardFunc: "/repo/s3/shard/v1/suffix/3",
			cid:       "CIQJ7IHPGOFUJT5UMXIW6CUDSNH6AVKMEOXI3UM3VLYJRZUISUMGCXQ",
			s3Dir:     "CXQ",
		},
		{
			shardFunc: "/repo/s3/shard/v1/suffix/8",
			cid:       "CIQJ7IHPGOFUJT5UMXIW6CUDSNH6AVKMEOXI3UM3VLYJRZUISUMGCXQ",
			s3Dir:     "ISUMGCXQ",
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
			s3Dir := id.fun(noslash)
			if s3Dir != c.s3Dir {
				t.Fatalf("expected %s, got %s", c.s3Dir, s3Dir)
			}
		})
	}
}

func TestNextToLast(t *testing.T) {
	cases := []struct {
		shardFunc string
		cid       string
		s3Dir     string
	}{
		{
			shardFunc: "/repo/s3/shard/v1/next-to-last/2",
			cid:       "a",
			s3Dir:     "__",
		},
		{
			shardFunc: "/repo/s3/shard/v1/next-to-last/2",
			cid:       "ab",
			s3Dir:     "_a",
		},
		{
			shardFunc: "/repo/s3/shard/v1/next-to-last/2",
			cid:       "abc",
			s3Dir:     "ab",
		},
		{
			shardFunc: "/repo/s3/shard/v1/next-to-last/2",
			cid:       "abcd",
			s3Dir:     "bc",
		},
		{
			shardFunc: "/repo/s3/shard/v1/next-to-last/3",
			cid:       "CIQJ7IHPGOFUJT5UMXIW6CUDSNH6AVKMEOXI3UM3VLYJRZUISUMGCXQ",
			s3Dir:     "GCX",
		},
		{
			shardFunc: "/repo/s3/shard/v1/next-to-last/8",
			cid:       "CIQJ7IHPGOFUJT5UMXIW6CUDSNH6AVKMEOXI3UM3VLYJRZUISUMGCXQ",
			s3Dir:     "UISUMGCX",
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
			s3Dir := id.fun(noslash)
			if s3Dir != c.s3Dir {
				t.Fatalf("expected %s, got %s", c.s3Dir, s3Dir)
			}
		})
	}
}

package s3ds

import (
	"fmt"
	"strconv"
	"strings"
)

var IPFS_DEF_SHARD = NextToLast(2)
var IPFS_DEF_SHARD_STR = IPFS_DEF_SHARD.String()

const PREFIX = "/repo/s3/shard/"

// TODO: Read existing sharding method
// const SHARDING_FN = "SHARDING"
// const README_FN = "_README"

type ShardFunc func(string) string

type ShardIdV1 struct {
	funName string
	param   int
	fun     ShardFunc
}

func (f *ShardIdV1) String() string {
	return fmt.Sprintf("%sv1/%s/%d", PREFIX, f.funName, f.param)
}

func (f *ShardIdV1) Func() ShardFunc {
	return f.fun
}

func Identity() *ShardIdV1 {
	return &ShardIdV1{
		funName: "identity",
		param:   0,
		fun: func(noslash string) string {
			return noslash
		},
	}
}

func Prefix(prefixLen int) *ShardIdV1 {
	padding := strings.Repeat("_", prefixLen)
	return &ShardIdV1{
		funName: "prefix",
		param:   prefixLen,
		fun: func(noslash string) string {
			return (noslash + padding)[:prefixLen]
		},
	}
}

func Suffix(suffixLen int) *ShardIdV1 {
	padding := strings.Repeat("_", suffixLen)
	return &ShardIdV1{
		funName: "suffix",
		param:   suffixLen,
		fun: func(noslash string) string {
			str := padding + noslash
			return str[len(str)-suffixLen:]
		},
	}
}

func NextToLast(suffixLen int) *ShardIdV1 {
	padding := strings.Repeat("_", suffixLen+1)
	return &ShardIdV1{
		funName: "next-to-last",
		param:   suffixLen,
		fun: func(noslash string) string {
			str := padding + noslash
			offset := len(str) - suffixLen - 1
			return str[offset : offset+suffixLen]
		},
	}
}

func ParseShardFunc(str string) (*ShardIdV1, error) {
	str = strings.TrimSpace(str)

	if len(str) == 0 {
		return nil, fmt.Errorf("empty shard identifier")
	}

	trimmed := strings.TrimPrefix(str, PREFIX)
	if str == trimmed { // nothing trimmed
		return nil, fmt.Errorf("invalid or no prefix in shard identifier: %s", str)
	}
	str = trimmed

	parts := strings.Split(str, "/")
	if len(parts) != 3 {
		return nil, fmt.Errorf("invalid shard identifier: %s", str)
	}

	version := parts[0]
	if version != "v1" {
		return nil, fmt.Errorf("expected 'v1' for version string got: %s", version)
	}

	funName := parts[1]

	param, err := strconv.Atoi(parts[2])
	if err != nil {
		return nil, fmt.Errorf("invalid parameter: %v", err)
	}

	switch funName {
	case "identity":
		return Identity(), nil
	case "prefix":
		return Prefix(param), nil
	case "suffix":
		return Suffix(param), nil
	case "next-to-last":
		return NextToLast(param), nil
	default:
		return nil, fmt.Errorf("expected 'identity', 'prefix', 'suffix' or 'next-to-last' got: %s", funName)
	}

}

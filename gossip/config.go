package gossip

import (
	"errors"
	"net"
	"strconv"

	"github.com/lni/goutils/stringutil"
	"go.uber.org/zap/zapcore"
)

type GossipOptions struct {
	Name               string
	MoveToGrpcAddr     string
	RubikGrpcAddr      string
	GossipNodes        int
	LogDir             string
	LogLevel           zapcore.Level
	DisableCoordinates bool
}

type GossipConfig struct {
	// GossipProbeInterval define the probe interval used by the gossip
	// service in tests.
	// GossipProbeInterval time.Duration `json:"gossipProbeInterval"`
	// BindAddress is the address for the gossip service to bind to and listen on.
	// Both UDP and TCP ports are used by the gossip service. The local gossip
	// service should be able to receive gossip service related messages by
	// binding to and listening on this address. BindAddress is usually in the
	// format of IP:Port, Hostname:Port or DNS Name:Port.
	BindAddress string `json:"-"`
	BindPort    uint16 `json:"bindPort"`
	// AdvertiseAddress is the address to advertise to other NodeHost instances
	// used for NAT traversal. Gossip services running on remote NodeHost
	// instances will use AdvertiseAddress to exchange gossip service related
	// messages. AdvertiseAddress is in the format of IP:Port.
	// AdvertiseAddress string
	// Seed is a list of AdvertiseAddress of remote NodeHost instances. Local
	// NodeHost instance will try to contact all of them to bootstrap the gossip
	// service. At least one reachable NodeHost instance is required to
	// successfully bootstrap the gossip service. Each seed address is in the
	// format of IP:Port, Hostname:Port or DNS Name:Port.
	//
	// It is ok to include seed addresses that are temporarily unreachable, e.g.
	// when launching the first NodeHost instance in your deployment, you can
	// include AdvertiseAddresses from other NodeHost instances that you plan to
	// launch shortly afterwards.
	Seeds []string `json:"seeds"`

	// 用于当cluster的数据在gossip内更新后，存储到文件中的接口
	clusterCallback ClusterCallback `json:"-"`
}

// IsEmpty returns a boolean flag indicating whether the GossipConfig instance
// is empty.
func (g *GossipConfig) IsEmpty() bool {
	return len(g.BindAddress) == 0 && len(g.Seeds) == 0
}

func (g *GossipConfig) SetClusterCallback(fn ClusterCallback) {
	g.clusterCallback = fn
}

// Validate validates the GossipConfig instance.
func (g *GossipConfig) Validate() error {
	if len(g.BindAddress) > 0 && !stringutil.IsValidAddress(g.BindAddress) {
		return errors.New("invalid GossipConfig.BindAddress")
	} else if len(g.BindAddress) == 0 {
		return errors.New("BindAddress not set")
	}

	if g.clusterCallback == nil {
		return errors.New("clusterCallback not set")
	}

	// if len(g.AdvertiseAddress) > 0 && !isValidAdvertiseAddress(g.AdvertiseAddress) {
	// 	return errors.New("invalid GossipConfig.AdvertiseAddress")
	// }
	if len(g.Seeds) == 0 {
		return errors.New("seed nodes not set")
	}
	count := 0
	for _, v := range g.Seeds {
		if v != g.BindAddress /*&& v != g.AdvertiseAddress*/ {
			count++
		}
		if !stringutil.IsValidAddress(v) {
			return errors.New("invalid GossipConfig.Seed value")
		}
	}
	if count == 0 {
		return errors.New("no valid seed node")
	}
	return nil
}

func isValidAdvertiseAddress(addr string) bool {
	host, sp, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	port, err := strconv.ParseUint(sp, 10, 16)
	if err != nil {
		return false
	}
	if port > 65535 {
		return false
	}
	// the memberlist package doesn't allow hostname or DNS name to be used in
	// advertise address
	return stringutil.IPV4Regex.MatchString(host)
}

func parseAddress(addr string) (string, int, error) {
	host, sp, err := net.SplitHostPort(addr)
	if err != nil {
		return "", 0, err
	}
	port, err := strconv.ParseUint(sp, 10, 16)
	if err != nil {
		return "", 0, err
	}
	return host, int(port), nil
}

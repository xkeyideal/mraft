package gossip

import (
	"fmt"
	"log"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/xkeyideal/mraft/gossip/coordinate"

	zlog "github.com/xkeyideal/mraft/logger"

	"github.com/hashicorp/memberlist"
	"github.com/lni/goutils/syncutil"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type GossipManager struct {
	config GossipConfig
	opts   GossipOptions

	aliveInstance *aliveInstance

	clusterBroadcasts    *memberlist.TransmitLimitedQueue
	membershipBroadcasts *memberlist.TransmitLimitedQueue

	log *zap.Logger

	cfg  *memberlist.Config
	list *memberlist.Memberlist

	ed *eventDelegate
	d  *delegate

	// Estimates the round trip time between two nodes using Gossip's network
	// coordinate model of the cluster.
	coordClient    *coordinate.Client
	coordCache     map[string]*coordinate.Coordinate
	coordCacheLock sync.RWMutex

	stopper *syncutil.Stopper
}

func NewGossipManager(config GossipConfig, opts GossipOptions) (*GossipManager, error) {
	if config.BindAddress == "" {
		config.BindAddress = fmt.Sprintf("0.0.0.0:%d", config.BindPort)
	}

	if err := config.Validate(); err != nil {
		return nil, err
	}

	bindAddr, bindPort, err := parseAddress(config.BindAddress)
	if err != nil {
		return nil, err
	}

	cfg := memberlist.DefaultWANConfig()
	cfg.BindAddr = bindAddr
	cfg.BindPort = bindPort
	if opts.GossipNodes > 0 {
		cfg.GossipNodes = opts.GossipNodes
	}
	cfg.UDPBufferSize = 65535
	cfg.Logger = newGossipLogWrapper(opts.LogDir, opts.LogLevel)
	cfg.Name = opts.Name

	g := &GossipManager{
		config:        config,
		opts:          opts,
		aliveInstance: newAliveInstance(),
		cfg:           cfg,
		log:           zlog.NewLogger(filepath.Join(opts.LogDir, "self-gossip-user.log"), opts.LogLevel, false),
	}

	list, err := memberlist.Create(cfg)
	if err != nil {
		return nil, err
	}
	g.list = list

	stopper := syncutil.NewStopper()
	ed := newEventDelegate(stopper, g)
	cfg.Events = ed
	g.ed = ed
	g.stopper = stopper

	d := newDelegate(Meta{
		MoveToGrpcAddr: opts.MoveToGrpcAddr,
		RubikGrpcAddr:  opts.RubikGrpcAddr,
	}, g, config.clusterCallback)
	cfg.Delegate = d
	g.d = d

	// Set up network coordinate client.
	if !opts.DisableCoordinates {
		g.coordClient, err = coordinate.NewClient(coordinate.DefaultConfig())
		if err != nil {
			return nil, fmt.Errorf("Failed to create coordinate client: %v", err)
		}

		g.coordCache = make(map[string]*coordinate.Coordinate)
		g.coordCache[opts.Name] = g.coordClient.GetCoordinate()
		cfg.Ping = &pingDelegate{g: g}
	}

	g.clusterBroadcasts = &memberlist.TransmitLimitedQueue{
		NumNodes: func() int {
			return list.NumMembers()
		},
		RetransmitMult: 3,
	}

	g.membershipBroadcasts = &memberlist.TransmitLimitedQueue{
		NumNodes: func() int {
			return list.NumMembers()
		},
		RetransmitMult: 3,
	}

	seed := make([]string, 0, len(config.Seeds))
	seed = append(seed, config.Seeds...)
	err = g.join(seed)
	if err != nil {
		return nil, err
	}

	g.ed.start()

	g.stopper.RunWorker(func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if len(g.list.Members()) > 1 {
					return
				}
				g.join(seed)
			case <-g.stopper.ShouldStop():
				return
			}
		}
	})

	return g, nil
}

func (g *GossipManager) join(seed []string) error {
	localNode := g.list.LocalNode()
	fields := []zap.Field{
		zap.String("name", localNode.Name),
		zap.String("address", localNode.Address()),
	}

	count, err := g.list.Join(seed)
	if err != nil {
		g.log.Warn("[multiraft] [self-gossip-user] [gossipmanager] [join] []",
			append(fields,
				zap.Strings("seed", seed),
				zap.Error(err),
			)...,
		)
		return err
	}

	g.log.Debug("[multiraft] [self-gossip-user] [gossipmanager] [join] []",
		append(fields,
			zap.Int("count", count),
		)...,
	)

	return nil
}

func (g *GossipManager) GetClusterMessage() *RaftClusterMessage {
	return g.d.queryCluster()
}

func (g *GossipManager) UpdateClusterMessage(cluster *RaftClusterMessage) {
	localNode := g.list.LocalNode()
	fields := []zap.Field{
		zap.String("name", localNode.Name),
		zap.String("address", localNode.Address()),
	}

	buf, err := encodeMessage(messageClusterType, cluster)
	if err != nil {
		g.log.Warn("[multiraft] [self-gossip-user] [apicall] [cluster] [update]",
			append(fields,
				zap.Stringer("message", cluster),
				zap.Error(err),
			)...,
		)
		return
	}

	g.log.Debug("[multiraft] [self-gossip-user] [apicall] [cluster] [update]",
		append(fields,
			zap.Stringer("message", cluster),
		)...,
	)
	g.d.localUpdateCluster(cluster)

	g.clusterBroadcasts.QueueBroadcast(newBroadcast(buf))
}

func (g *GossipManager) GetMembershipMessage(clusterId uint64) *MemberInfo {
	return g.d.queryMembership(clusterId)
}

func (g *GossipManager) GetMembershipMessages() map[uint64]*MemberInfo {
	g.d.mlock.RLock()
	defer g.d.mlock.RUnlock()

	return g.d.membership.MemberInfos
}

func (g *GossipManager) UpdateMembershipMessage(membership *RaftMembershipMessage) {
	localNode := g.list.LocalNode()
	fields := []zap.Field{
		zap.String("name", localNode.Name),
		zap.String("address", localNode.Address()),
	}

	buf, err := encodeMessage(messageMembershipType, membership)
	if err != nil {
		g.log.Warn("[multiraft] [self-gossip-user] [apicall] [membership] [update]",
			append(fields,
				zap.Stringer("message", membership),
				zap.Error(err),
			)...,
		)
		return
	}

	g.log.Debug("[multiraft] [self-gossip-user] [apicall] [membership] [update]",
		append(fields,
			zap.Stringer("message", membership),
		)...,
	)

	g.d.localUpdateMembership(membership)

	g.membershipBroadcasts.QueueBroadcast(newBroadcast(buf))
}

func (g *GossipManager) GetAliveInstances() map[string]bool {
	return g.aliveInstance.getMoveToInstances()
}

func (g *GossipManager) GetRubikInstances() map[string]bool {
	return g.aliveInstance.getRubikInstances()
}

func (g *GossipManager) GetMoveToRubik() map[string]string {
	return g.aliveInstance.getMoveToRubik()
}

func (g *GossipManager) SetNodeMeta(meta Meta) error {
	// Check that the meta data length is okay
	b, err := encodeMessage(tagMagicByte, meta)
	if err != nil {
		return err
	}

	if len(b) > memberlist.MetaMaxSize {
		return fmt.Errorf("Encoded length of meta exceeds limit of %d bytes",
			memberlist.MetaMaxSize)
	}

	g.d.meta = meta
	return g.list.UpdateNode(2 * time.Second)
}

// GetCoordinate returns the network coordinate of the local node.
func (g *GossipManager) GetCoordinate() (*coordinate.Coordinate, error) {
	if !g.opts.DisableCoordinates {
		return g.coordClient.GetCoordinate(), nil
	}

	return nil, fmt.Errorf("Coordinates are disabled")
}

func (g *GossipManager) GetCachedCoordinate(name string) (coord *coordinate.Coordinate, ok bool) {
	if !g.opts.DisableCoordinates {
		g.coordCacheLock.RLock()
		defer g.coordCacheLock.RUnlock()
		if coord, ok = g.coordCache[name]; ok {
			return coord, true
		}
	}

	return nil, false
}

func (g *GossipManager) Close() error {
	g.log.Sync()
	g.stopper.Stop()

	if err := g.list.Leave(2 * time.Second); err != nil {
		return errors.Wrapf(err, "leave memberlist failed")
	}

	if err := g.list.Shutdown(); err != nil {
		return errors.Wrapf(err, "shutdown memberlist failed")
	}
	return nil
}

type gossipLogWriter struct {
	logger *zap.Logger
}

func (l *gossipLogWriter) Write(p []byte) (int, error) {
	str := strings.TrimSuffix(string(p), "\n")

	switch {
	case strings.HasPrefix(str, "[WARN] "):
		str = strings.TrimPrefix(str, "[WARN] ")
		l.logger.Warn(str)
	case strings.HasPrefix(str, "[DEBUG] "):
		str = strings.TrimPrefix(str, "[DEBUG] ")
		l.logger.Debug(str)
	case strings.HasPrefix(str, "[INFO] "):
		str = strings.TrimPrefix(str, "[INFO] ")
		l.logger.Info(str)
	case strings.HasPrefix(str, "[ERR] "):
		str = strings.TrimPrefix(str, "[ERR] ")
		l.logger.Error(str)
	default:
		l.logger.Warn(str)
	}

	return len(p), nil
}

func newGossipLogWrapper(logDir string, level zapcore.Level) *log.Logger {
	return log.New(&gossipLogWriter{
		logger: zlog.NewLogger(filepath.Join(logDir, "self-gossip-system.log"), level, false),
	}, "", 0)
}

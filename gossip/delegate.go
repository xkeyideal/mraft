package gossip

import (
	"errors"
	"fmt"
	"sync"

	"github.com/hashicorp/memberlist"
	"go.uber.org/zap"
)

type ClusterCallback func(cluster *RaftClusterMessage) error

type Meta struct {
	MoveToGrpcAddr string
	RubikGrpcAddr  string
}

const (
	tagMagicByte messageType = 255
)

type delegate struct {
	meta Meta

	g *GossipManager

	clock   sync.RWMutex
	cluster *RaftClusterMessage

	mlock      sync.RWMutex
	membership *RaftMembershipMessage

	fn ClusterCallback
}

var _ memberlist.Delegate = &delegate{}

func newDelegate(meta Meta, g *GossipManager, fn ClusterCallback) *delegate {
	return &delegate{
		meta: meta,
		g:    g,
		cluster: &RaftClusterMessage{
			Revision: 0,
		},
		membership: &RaftMembershipMessage{
			MemberInfos: make(map[uint64]*MemberInfo),
		},
		fn: fn,
	}
}

func (d *delegate) localUpdateCluster(cluster *RaftClusterMessage) {
	localNode := d.g.list.LocalNode()
	fields := []zap.Field{
		zap.String("name", localNode.Name),
		zap.String("address", localNode.Address()),
	}

	if cluster.Revision <= d.cluster.Revision {
		if cluster.Revision < d.cluster.Revision {
			fields = append(fields, zap.Int64("revision", cluster.Revision),
				zap.Int64("d_revision", d.cluster.Revision))
			d.g.log.Warn("[multiraft] [self-gossip-user] [local] [cluster] [revisionlater]", fields...)
		}
		return
	}

	d.g.log.Debug("[multiraft] [self-gossip-user] [local] [cluster] [update]",
		append(fields,
			zap.Stringer("new-cluster", cluster),
			zap.Stringer("old-cluster", d.cluster),
		)...,
	)

	d.clock.Lock()
	// gossip内存里更新cluster后，还需要通知存储更新
	err := d.fn(cluster)
	if err != nil {
		d.g.log.Error("[multiraft] [self-gossip-user] [local] [cluster] [store]",
			append(fields,
				zap.Stringer("new-cluster", cluster),
				zap.Stringer("old-cluster", d.cluster),
				zap.Error(err),
			)...,
		)
	} else {
		d.g.log.Warn("[multiraft] [self-gossip-user] [local] [cluster] [store]",
			append(fields,
				zap.Stringer("new-cluster", cluster),
				zap.Stringer("old-cluster", d.cluster),
			)...,
		)
	}

	d.cluster = cluster

	d.clock.Unlock()
}

func (d *delegate) queryCluster() *RaftClusterMessage {
	d.clock.RLock()
	defer d.clock.RUnlock()

	return d.cluster
}

func (d *delegate) localUpdateMembership(membership *RaftMembershipMessage) {
	d.mlock.Lock()
	defer d.mlock.Unlock()

	localNode := d.g.list.LocalNode()
	fields := []zap.Field{
		zap.String("name", localNode.Name),
		zap.String("address", localNode.Address()),
	}

	for clusterId, info := range membership.MemberInfos {
		v, ok := d.membership.MemberInfos[clusterId]
		if !ok {
			d.g.log.Debug("[multiraft] [self-gossip-user] [local] [membership] [update]",
				append(fields,
					zap.Uint64("clusterId", clusterId),
					zap.Stringer("new-memberinfo", info),
					zap.String("old-memberinfo", "none"),
				)...,
			)

			d.membership.MemberInfos[clusterId] = info
		} else {
			if info.ConfigChangeId <= v.ConfigChangeId {
				if info.ConfigChangeId < v.ConfigChangeId {
					d.g.log.Warn("[multiraft] [self-gossip-user] [local] [membership] [revisionlater]",
						append(fields,
							zap.Uint64("clusterId", clusterId),
							zap.Int64("revision", int64(info.ConfigChangeId)),
							zap.Int64("d_revision", int64(v.ConfigChangeId)),
						)...,
					)
				}
				continue
			}

			d.g.log.Debug("[multiraft] [self-gossip-user] [local] [membership] [update]",
				append(fields,
					zap.Uint64("clusterId", clusterId),
					zap.Stringer("new-memberinfo", info),
					zap.Stringer("old-memberinfo", d.membership.MemberInfos[clusterId]),
				)...,
			)

			d.membership.MemberInfos[clusterId] = info
		}
	}
}

func (d *delegate) queryMembership(clusterId uint64) *MemberInfo {
	d.mlock.RLock()
	defer d.mlock.RUnlock()

	return d.membership.MemberInfos[clusterId]
}

// NodeMeta is used to retrieve meta-data about the current node
// when broadcasting an alive message. It's length is limited to
// the given byte size
// memberlist 设置的meta的最大长度是MetaMaxSize=512
func (d *delegate) NodeMeta(limit int) []byte {
	b, _ := encodeMessage(tagMagicByte, d.meta)
	if len(b) > limit {
		panic(fmt.Errorf("Node tags '%v' exceeds length limit of %d bytes", d.meta, limit))
	}

	return b
}

// NotifyMsg([]byte)：每当用户有新数据加到广播队列时，会调此方法通知其他节点同步数据
// 即NotifyMsg通过接受其他节点的变化后的数据，来更新自己节点本地的数据
// 与LocalState不同的是，NotifyMsg可以是增量数据的同步，即QueueBroadcast里发送的数据
func (d *delegate) NotifyMsg(buf []byte) {
	// If we didn't actually receive any data, then ignore it.
	if len(buf) == 0 {
		return
	}

	localNode := d.g.list.LocalNode()
	fields := []zap.Field{
		zap.String("name", localNode.Name),
		zap.String("address", localNode.Address()),
	}

	t := messageType(buf[0])

	switch t {
	case messageClusterType:
		cluster := &RaftClusterMessage{}
		if err := decodeMessage(buf[1:], &cluster); err != nil {
			d.g.log.Error("[multiraft] [self-gossip-user] [notifymsg] [cluster] [decode]",
				append(fields,
					zap.String("message", string(buf[1:])),
					zap.Error(err),
				)...,
			)
			break
		}

		d.g.log.Debug("[multiraft] [self-gossip-user] [notifymsg] [cluster] [update]",
			append(fields,
				zap.Stringer("cluster", cluster),
			)...,
		)
		d.localUpdateCluster(cluster)
	case messageMembershipType:
		membership := &RaftMembershipMessage{}
		if err := decodeMessage(buf[1:], &membership); err != nil {
			d.g.log.Error("[multiraft] [self-gossip-user] [notifymsg] [membership] [decode]",
				append(fields,
					zap.String("message", string(buf[1:])),
					zap.Error(err),
				)...,
			)
			break
		}

		d.g.log.Debug("[multiraft] [self-gossip-user] [notifymsg] [membership] [update]",
			append(fields,
				zap.Stringer("membership", membership),
			)...,
		)
		d.localUpdateMembership(membership)
	default:
		d.g.log.Warn("[multiraft] [self-gossip-user] [notifymsg] [unknowntype] []",
			append(fields,
				zap.Int8("type", int8(t)),
				zap.String("message", string(buf[1:])),
			)...,
		)
	}
}

func (d *delegate) GetBroadcasts(overhead, limit int) [][]byte {
	msgs := [][]byte{}
	bytesUsed := 0

	// Get any additional cluster broadcasts
	queryMsgs := d.g.clusterBroadcasts.GetBroadcasts(overhead, limit)
	if queryMsgs != nil {
		for _, m := range queryMsgs {
			lm := len(m)
			bytesUsed += lm + overhead
		}
		msgs = append(msgs, queryMsgs...)
	}

	// Get any additional membership broadcasts
	eventMsgs := d.g.membershipBroadcasts.GetBroadcasts(overhead, limit-bytesUsed)
	if eventMsgs != nil {
		for _, m := range eventMsgs {
			lm := len(m)
			bytesUsed += lm + overhead
		}
		msgs = append(msgs, eventMsgs...)
	}

	return msgs
}

// 2、LocalState(join bool) []byte、MergeRemoteState(buf []byte, join bool)：
// 每隔PushPullInterval 周期，本地memberlist回调LocalState方法，把本地全部数据发送到其他节点；
// 其他节点memberlist回调MergeRemoteState，接受数据进行同步。
// 一个数新增数据的广播，另一个是通过tcp全量数据同步（加快节点同步状态；加强一致性保障）
// 与NotifyMsg不同的是，LocalState必须是全量数据同步
// join参数表示自身当前是否是第一次加入集群
func (d *delegate) LocalState(join bool) []byte {
	d.clock.RLock()
	defer d.clock.RUnlock()
	d.mlock.RLock()
	defer d.mlock.RUnlock()

	pp := &PushPullMessage{
		Cluster:    d.cluster,
		Membership: d.membership,
	}

	localNode := d.g.list.LocalNode()
	fields := []zap.Field{
		zap.String("name", localNode.Name),
		zap.String("address", localNode.Address()),
	}

	buf, err := encodeMessage(messagePushPullType, &pp)
	if err != nil {
		d.g.log.Warn("[multiraft] [self-gossip-user] [localstate] [encode]",
			append(fields,
				zap.Int8("type", int8(messagePushPullType)),
				zap.Stringer("message", pp),
				zap.Error(err),
			)...,
		)
		return nil
	}

	d.g.log.Debug("[multiraft] [self-gossip-user] [localstate] [encode]",
		append(fields,
			zap.Bool("join", join),
			zap.Int8("type", int8(messagePushPullType)),
			zap.Stringer("message", pp),
		)...,
	)

	return buf
}

// MergeRemoteState  每隔PushPullInterval周期, 接受其他节点的数据
// 本地节点可以根据数据比对，来更新本地的数据
// join参数表示自身当前是否是第一次加入集群
func (d *delegate) MergeRemoteState(buf []byte, join bool) {
	localNode := d.g.list.LocalNode()
	fields := []zap.Field{
		zap.String("name", localNode.Name),
		zap.String("address", localNode.Address()),
	}

	if len(buf) == 0 {
		d.g.log.Error("[multiraft] [self-gossip-user] [mergeremotestate] [bufnone]",
			append(fields,
				zap.Bool("join", join),
				zap.Error(errors.New("Remote state is zero bytes")),
			)...,
		)
		return
	}

	// Check the message type
	if messageType(buf[0]) != messagePushPullType {
		d.g.log.Error("[multiraft] [self-gossip-user] [mergeremotestate] [unknowntype]",
			append(fields,
				zap.Bool("join", join),
				zap.Int8("type", int8(buf[0])),
			)...,
		)
		return
	}

	// Attempt a decode
	pp := PushPullMessage{}
	if err := decodeMessage(buf[1:], &pp); err != nil {
		d.g.log.Warn("[multiraft] [self-gossip-user] [mergeremotestate] [decode]",
			append(fields,
				zap.Bool("join", join),
				zap.Int8("type", int8(messagePushPullType)),
				zap.String("message", string(buf[1:])),
				zap.Error(err),
			)...,
		)
		return
	}

	d.localUpdateCluster(pp.Cluster)
	d.g.log.Debug("[multiraft] [self-gossip-user] [mergeremotestate] [cluster] [update]",
		append(fields,
			zap.Bool("join", join),
			zap.Stringer("cluster", pp.Cluster),
		)...,
	)

	d.localUpdateMembership(pp.Membership)
	d.g.log.Debug("[multiraft] [self-gossip-user] [mergeremotestate] [membership] [update]",
		append(fields,
			zap.Bool("join", join),
			zap.Stringer("membership", pp.Membership),
		)...,
	)
}

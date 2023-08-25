package gossip

import (
	"sync"

	"github.com/hashicorp/memberlist"
	"github.com/lni/goutils/syncutil"
	"go.uber.org/zap"
)

type eventDelegate struct {
	g *GossipManager
	memberlist.ChannelEventDelegate
	ch      chan memberlist.NodeEvent
	stopper *syncutil.Stopper
	nodes   sync.Map
}

type aliveInstance struct {
	mu sync.RWMutex

	// moveTo grpcaddr映射rubik对外提供服务grpcaddr
	moveToRubik map[string]string

	moveToInstances map[string]bool
	rubikInstances  map[string]bool
}

func newAliveInstance() *aliveInstance {
	return &aliveInstance{
		moveToRubik:     make(map[string]string),
		moveToInstances: make(map[string]bool),
		rubikInstances:  make(map[string]bool),
	}
}

func (ai *aliveInstance) updateInstance(meta *Meta, alive bool) {
	if meta == nil {
		return
	}

	ai.mu.Lock()
	if alive {
		ai.moveToRubik[meta.MoveToGrpcAddr] = meta.RubikGrpcAddr
	} else {
		delete(ai.moveToRubik, meta.MoveToGrpcAddr)
	}
	ai.moveToInstances[meta.MoveToGrpcAddr] = alive
	ai.rubikInstances[meta.RubikGrpcAddr] = alive
	ai.mu.Unlock()
}

func (ai *aliveInstance) getMoveToInstances() map[string]bool {
	ai.mu.RLock()
	defer ai.mu.RUnlock()

	return ai.moveToInstances
}

func (ai *aliveInstance) getRubikInstances() map[string]bool {
	ai.mu.RLock()
	defer ai.mu.RUnlock()

	return ai.rubikInstances
}

func (ai *aliveInstance) getMoveToRubik() map[string]string {
	ai.mu.RLock()
	defer ai.mu.RUnlock()

	return ai.moveToRubik
}

func newEventDelegate(s *syncutil.Stopper, g *GossipManager) *eventDelegate {
	ch := make(chan memberlist.NodeEvent, 10)
	ed := &eventDelegate{
		g:                    g,
		stopper:              s,
		ch:                   ch,
		ChannelEventDelegate: memberlist.ChannelEventDelegate{Ch: ch},
	}
	return ed
}

func (d *eventDelegate) decodeMeta(e memberlist.NodeEvent, fields []zap.Field) *Meta {
	if len(e.Node.Meta) == 0 || messageType(e.Node.Meta[0]) != tagMagicByte {
		d.g.log.Warn("[multiraft] [self-gossip-user] [eventdelegate] [metaType]",
			append(fields,
				zap.Int("event", int(e.Event)),
				zap.String("nodename", e.Node.Name),
				zap.String("nodeaddress", e.Node.Address()),
				zap.String("meta", string(e.Node.Meta)),
				zap.String("err", "meta messageType error"),
			)...)
		return nil
	}

	meta := &Meta{}
	if err := decodeMessage(e.Node.Meta[1:], &meta); err != nil {
		d.g.log.Warn("[multiraft] [self-gossip-user] [eventdelegate] [metaDecode]",
			append(fields,
				zap.Int("event", int(e.Event)),
				zap.String("nodename", e.Node.Name),
				zap.String("nodeaddress", e.Node.Address()),
				zap.String("meta", string(e.Node.Meta)),
				zap.Error(err),
			)...)
		return nil
	}

	return meta
}

func (d *eventDelegate) start() {
	localNode := d.g.list.LocalNode()
	fields := []zap.Field{
		zap.String("name", localNode.Name),
		zap.String("address", localNode.Address()),
	}

	d.stopper.RunWorker(func() {
		for {
			select {
			case <-d.stopper.ShouldStop():
				return
			case e := <-d.ch:
				meta := d.decodeMeta(e, fields)
				if e.Event == memberlist.NodeJoin || e.Event == memberlist.NodeUpdate {
					d.g.log.Info("[multiraft] [self-gossip-user] [eventdelegate] [update]",
						append(fields,
							zap.Int("event", int(e.Event)),
							zap.String("nodename", e.Node.Name),
							zap.String("nodeaddress", e.Node.Address()),
							zap.String("meta", string(e.Node.Meta)),
						)...,
					)
					d.nodes.Store(e.Node.Name, string(e.Node.Meta))
					d.g.aliveInstance.updateInstance(meta, true)
				} else if e.Event == memberlist.NodeLeave {
					d.g.log.Info("[multiraft] [self-gossip-user] [eventdelegate] [delete]",
						append(fields,
							zap.Int("event", int(e.Event)),
							zap.String("nodename", e.Node.Name),
							zap.String("nodeaddress", e.Node.Address()),
							zap.String("meta", string(e.Node.Meta)),
						)...,
					)
					d.nodes.Delete(e.Node.Name)
					d.g.aliveInstance.updateInstance(meta, false)
				}
			}
		}
	})
}

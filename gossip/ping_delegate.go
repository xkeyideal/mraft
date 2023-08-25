package gossip

import (
	"bytes"
	"time"

	"github.com/xkeyideal/mraft/gossip/coordinate"

	"github.com/hashicorp/go-msgpack/codec"
	"github.com/hashicorp/memberlist"
	"go.uber.org/zap"
)

// pingDelegate is notified when memberlist successfully completes a direct ping
// of a peer node. We use this to update our estimated network coordinate, as
// well as cache the coordinate of the peer.
type pingDelegate struct {
	g *GossipManager
}

const (
	// PingVersion is an internal version for the ping message, above the normal
	// versioning we get from the protocol version. This enables small updates
	// to the ping message without a full protocol bump.
	PingVersion = 1
)

// AckPayload is called to produce a payload to send back in response to a ping
// request.
func (p *pingDelegate) AckPayload() []byte {
	var buf bytes.Buffer

	// The first byte is the version number, forming a simple header.
	version := []byte{PingVersion}
	buf.Write(version)

	// The rest of the message is the serialized coordinate.
	enc := codec.NewEncoder(&buf, &codec.MsgpackHandle{})
	if err := enc.Encode(p.g.coordClient.GetCoordinate()); err != nil {
		p.g.log.Error("[multiraft] [self-gossip-user] [ping-delegate] [AckPayload] [encode]", zap.Error(err))
	}
	return buf.Bytes()
}

// NotifyPingComplete is called when this node successfully completes a direct ping
// of a peer node.
func (p *pingDelegate) NotifyPingComplete(other *memberlist.Node, rtt time.Duration, payload []byte) {
	if payload == nil || len(payload) == 0 {
		return
	}

	// Verify ping version in the header.
	version := payload[0]
	if version != PingVersion {
		p.g.log.Error("[multiraft] [self-gossip-user] [ping-delegate] [NotifyPingComplete] [version]", zap.Uint8("version", version))
		return
	}

	// Process the remainder of the message as a coordinate.
	r := bytes.NewReader(payload[1:])
	dec := codec.NewDecoder(r, &codec.MsgpackHandle{})
	var coord coordinate.Coordinate
	if err := dec.Decode(&coord); err != nil {
		p.g.log.Error("[multiraft] [self-gossip-user] [ping-delegate] [NotifyPingComplete] [decode]", zap.Error(err))
		return
	}

	// Apply the update.
	before := p.g.coordClient.GetCoordinate()
	after, err := p.g.coordClient.Update(other.Name, &coord, rtt)
	if err != nil {
		//metrics.IncrCounter([]string{"serf", "coordinate", "rejected"}, 1)
		p.g.log.Error("[multiraft] [self-gossip-user] [ping-delegate] [NotifyPingComplete] [rejected]", zap.Error(err))
		return
	}

	// Publish some metrics to give us an idea of how much we are
	// adjusting each time we update.
	d := float32(before.DistanceTo(after).Seconds() * 1.0e3)
	//metrics.AddSample([]string{"serf", "coordinate", "adjustment-ms"}, d)
	if d >= 100.0 {
		p.g.log.Warn("[multiraft] [self-gossip-user] [ping-delegate] [NotifyPingComplete] [DistanceTo]",
			zap.String("src", p.g.opts.Name),
			zap.String("dest", other.Name),
			zap.Int64("rtt", int64(rtt)),
			zap.Float32("adjustment-ms", d),
		)
	} else {
		p.g.log.Info("[multiraft] [self-gossip-user] [ping-delegate] [NotifyPingComplete] [DistanceTo]",
			zap.String("src", p.g.opts.Name),
			zap.String("dest", other.Name),
			zap.Int64("rtt", int64(rtt)),
			zap.Float32("adjustment-ms", d),
		)
	}

	// Cache the coordinate for the other node, and add our own
	// to the cache as well since it just got updated. This lets
	// users call GetCachedCoordinate with our node name, which is
	// more friendly.
	p.g.coordCacheLock.Lock()
	p.g.coordCache[other.Name] = &coord
	p.g.coordCache[p.g.opts.Name] = p.g.coordClient.GetCoordinate()
	p.g.coordCacheLock.Unlock()
}

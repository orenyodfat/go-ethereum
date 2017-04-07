package p2p

import (
	"crypto/ecdsa"
	"github.com/ethereum/go-ethereum/p2p/discover"
)

type VirtualPeer struct {
	*Peer
}

// a virtual peer is for use to artificially communicate with the protocol stack
// id - the id to use for this peer
// name - no idea still what this means
// rw - the rw used to pass messages to and read from the peer (it hacks into the transport member of the peer's conn object
// protos - the protocols to instantiate on this peer
func NewVirtualPeer(p *Peer, rw MsgReadWriter, cap Cap) *VirtualPeer {
	
	t := newPipeTransport(rw)
	
	p.rw.transport = t
	p.rw.caps = append(p.rw.caps, cap)
	
	// hacks the passed rw to replace the network connection
	vp := &VirtualPeer{
		Peer: p,
	}
	return vp
}

func (p *VirtualPeer) LinkProtocols(protos []Protocol) {
	p.running = matchProtocols(protos, p.Peer.rw.caps, p.Peer.rw)	
}

func (p *VirtualPeer) Run() DiscReason {
	return p.run()
}

type pipeTransport struct {
	MsgReadWriter
}

func newPipeTransport(rw MsgReadWriter) *pipeTransport {
	p := pipeTransport{
		MsgReadWriter: rw,
	}
	return &p
}

func (p *pipeTransport) close(err error) {
}

func (p *pipeTransport) doEncHandshake(prv *ecdsa.PrivateKey, dialDest *discover.Node) (discover.NodeID, error) {
	return discover.NodeID{}, nil
}

func (p *pipeTransport) doProtoHandshake(our *protoHandshake) (*protoHandshake, error) {
	return nil, nil
}

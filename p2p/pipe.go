package p2p

import (
	"crypto/ecdsa"
	"github.com/ethereum/go-ethereum/logger"
	"github.com/ethereum/go-ethereum/logger/glog"
	"github.com/ethereum/go-ethereum/p2p/discover"
)

type VirtualPeer struct {
	*Peer
}

// a virtual peer is for use to artificially communicate with the protocol stack
// p - the p2p.peer to extend
// rw - the rw used to pass messages to and read from the peer (it hacks into the transport member of the peer's conn object
// cap - the protocol capacities that this peer should have
func NewVirtualPeer(p *Peer, rw MsgReadWriter, cap Cap) *VirtualPeer {

	t := newPipeTransport(rw)
	p.rw.transport = t

	p.rw.caps = append(p.rw.caps, cap)

	// the public p2p.peer constructor NewPeer closes the close channel, so we have to reopen
	p.closed = make(chan struct{})

	// wrap it up with a nice bow on top
	vp := &VirtualPeer{
		Peer: p,
	}

	return vp
}

// manual override of protocol to protorw mappings
// use the transport for rw
func (p *VirtualPeer) LinkProtocols(protos []Protocol) {
	p.running = matchProtocols(protos, p.Peer.rw.caps, p.Peer.rw)
	glog.V(logger.Warn).Infof("vp protocols linked: %v", p.running)
}

// Public accessor to retrieve the ProtoRW for a given protocol this peer is running
// error handling
func (p *VirtualPeer) GetProtoReadWriter(name string) (MsgReadWriter, error) {
	return p.running[name], nil
}

// a minimum implementation of the transport member of the p2p.Server's conn struct
// to enable hijacking the i/o on the p2p level from the outside
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

// public access to the p2p.peer run function
func (p *VirtualPeer) Run() DiscReason {
	return p.run()
}

// pss specific hack, to rebuild correct msgcodes for forwarded messages
func GetBaseProtocolLength() uint64 {
	return baseProtocolLength
}

package network

import (
	"bytes"
	"fmt"
	"time"
	//"reflect"

	"github.com/ethereum/go-ethereum/logger"
	"github.com/ethereum/go-ethereum/logger/glog"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/p2p/adapters"
	"github.com/ethereum/go-ethereum/p2p/protocols"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/pot"
)

const (
	DefaultTTL          = 6000
	TopicLength         = 32
	TopicResolverLength = 8
	PssPeerCapacity     = 256
)

var (
	zeroRW	= PssReadWriter{}
)

type PssMsg struct {
	To   []byte
	Data PssEnvelope
}

func (pm *PssMsg) String() string {
	return fmt.Sprintf("PssMsg: Recipient: %x", pm.To)
}

type PssEnvelope struct {
	Topic PssTopic
	TTL   uint16
	Data  []byte
}

type pssPayload struct {
	SenderOAddr	[]byte
	SenderUAddr []byte
	Code       uint64
	Size       uint32
	Data       []byte
	ReceivedAt time.Time
}

type PssTopic [TopicLength]byte

/*
type PssPeer struct {
	*protocols.Peer
	lastActive time.Time
	insertw    chan p2p.Msg
}

func (psp *PssPeer) Insert(msg p2p.Msg) error {
	glog.V(logger.Detail).Infof("Injecting msg %v from %v", msg, psp)
	// forever, forever we meet the same dead end
	// psp.Peer.m.rw <- msg
	// so meet another ugly as fuck hack:
	psp.insertw <- msg
	return nil
}
*/

// Pss singleton object provides:
// - access to the swarm overlay and routing (kademlia)
// - a collection of remote overlay addresses mapped to MsgReadWriters, representing the virtually connected peers
// - a method to send a message to specific overlayaddr
type Pss struct {
	Overlay // we can get the overlayaddress from this
	NodeId *adapters.NodeId // we need the underlayaddr to make virtual p2p.Peers in the receiving end
	PeerPool map[pot.Address]PssReadWriter // keep track of all virtual p2p.Peers we are currently speaking to
}

func (ps *Pss) isActive(addr pot.Address) bool {
	if ps.PeerPool[addr] == zeroRW {
		return false
	}
	return true
}
/*
func (ps *Pss) GetPssPeer(id adapters.NodeId) PssPeer {
	return ps.PeerPool[id]
}
*/
func NewPss(k Overlay, id *adapters.NodeId) *Pss {
	return &Pss{
		Overlay:   k,
		NodeId: id,
		PeerPool: make(map[pot.Address]PssReadWriter, PssPeerCapacity),
	}
}

func (ps *Pss) Send(to []byte, code uint64, msg interface{}) error {
	// rlp encode msg
	sent := false
	glog.V(logger.Detail).Infof("pss-sending msg code %v: %v", code, msg)
	
	rlpbundle, err := ps.MakeMsg(code, msg)
	if err != nil {
		return err
	}
	
	pssenv := PssEnvelope{
		Topic: MakeTopic("42"),
		TTL:   DefaultTTL,
		Data:  rlpbundle,
	}

	pssmsg := PssMsg{
		To: to,
		Data: pssenv,
	}
	
	// send with kademlia
	// find the closest peer to the recipient and attempt to send
	ps.Overlay.EachLivePeer(to, 255, func(p Peer, po int) bool {
		
		err := p.Send(&pssmsg)
		if err != nil {
			return true
		}
		sent = true
		return false
	})
	if !sent {
		return fmt.Errorf("Was not able to send to any peers")
	}
	return nil
}

type PssReadWriter struct {
	Recipient pot.Address
	LastActive time.Time
	rw        chan p2p.Msg
}

func (prw PssReadWriter) ReadMsg() (p2p.Msg, error) {
	msg := <-prw.rw
	glog.V(logger.Warn).Infof("got %v", msg)
	return msg, nil
}

func (prw PssReadWriter) WriteMsg(msg p2p.Msg) error {
	prw.rw <- msg
	return nil
}

func (prw PssReadWriter) InjectMsg(msg p2p.Msg) error {
	glog.V(logger.Warn).Infof("inject %v on rw %v", msg, prw.rw)
	prw.rw <- msg
	return nil
}

type PssProtocol struct {
	*Pss
	Name            string
	Version         uint
	VirtualProtocol *p2p.Protocol
	ct              *protocols.CodeMap
}

// a new protocol is run using this signature:
// func NewProtocol(protocolname string, protocolversion uint, run func(*Peer) error, na adapters.NodeAdapter, ct *CodeMap, peerInfo func(id discover.NodeID) interface{}, nodeInfo func() interface{}) *p2p.Protocol {
// the run function is the extended run function to the protocol runnning on the peer, before which a new protocols.peer is created with the messenger passed in the nodeadapter passed in the constructor

// the pssprotocol newprotocol function is a REPLACEMENT which implements the following adjustment:
// * it uses the pssmessenger

func NewPssProtocol(pss *Pss, topic string, version uint, run func(*p2p.VirtualPeer) error, ct *protocols.CodeMap) *PssProtocol {
	pp := &PssProtocol{
		Pss:     pss,
		Name:    topic,
		Version: version,
		ct:      ct,
	}

	r := func(p *p2p.Peer, rw p2p.MsgReadWriter) error {
		cap := p2p.Cap{
			Name: topic,
			Version: version,
		}
		v := p2p.NewVirtualPeer(p, rw, cap)
		run(v)
		return nil
	}

	pp.VirtualProtocol = &p2p.Protocol{
		Name:    string(pp.Name),
		Version: pp.Version,
		Length:  ct.Length(),
		Run:     r,
	}

	return pp
}

func (ps *Pss) Add(id pot.Address, rw PssReadWriter) error {
	if ps.PeerPool[id] != zeroRW {
		return fmt.Errorf("Peer pss entry '%v' already exists", id)
	}
	ps.PeerPool[id] = rw
	return nil
}

func (ps *Pss) Remove(id pot.Address) {
	ps.PeerPool[id] = zeroRW
	return
}

func (pp *PssProtocol) handlePss(msg interface{}) error {
	rmsg := &pssPayload{}
	pssmsg := msg.(*PssMsg)
	to := pssmsg.To
	env := pssmsg.Data

	glog.V(logger.Detail).Infof("pss for us, yay! ...: %v", pssmsg)

	//if pp.isSelfRecipient(to) {
	if to != nil {
		err := rlp.DecodeBytes(env.Data, rmsg)
		if err != nil {
			glog.V(logger.Warn).Infof("pss payload encapsulation is corrupt: %v", err)
			return err
		}
	
		nid := adapters.NewNodeId(rmsg.SenderUAddr)
		oaddrhash := pot.NewHashAddressFromBytes(rmsg.SenderOAddr)

		if !pp.isActive(oaddrhash.Address) {
			rw := PssReadWriter{
				Recipient: oaddrhash.Address,
				rw:        make(chan p2p.Msg),
			}
			p := p2p.NewPeer(nid.NodeID, "foo", []p2p.Cap{})

			// might need to have a channel to check if everything is set up properly
			// fake it here with a small wait
			go func() {
				pp.Add(oaddrhash.Address, rw)
				pp.VirtualProtocol.Run(p, rw)
				pp.Remove(oaddrhash.Address)
				close(rw.rw)
				return
			}()
			time.Sleep(time.Millisecond * 250)
		}

		// to be ignored, just making sure things are sane while testing
		/*pmsg, found := pp.ct.GetInterface(rmsg.Code)
		if !found {
			glog.V(logger.Warn).Infof("message code %v not recognized, discarding", rmsg.Code)
			return err
		}
		err = rlp.DecodeBytes(rmsg.Data, &pmsg)
		if err != nil {
			glog.V(logger.Warn).Infof("pss payload data does not fit in interface %v (code %v): %v", pmsg, rmsg.Code, err)
			return err
		}
		glog.V(logger.Detail).Infof("rlp decoded %v", pmsg)*/
		// end ignore

		imsg := p2p.Msg{
			Code:       rmsg.Code,
			Size:       rmsg.Size,
			ReceivedAt: rmsg.ReceivedAt,
			Payload:    bytes.NewBuffer(rmsg.Data),
		}
		
		pp.PeerPool[oaddrhash.Address].InjectMsg(imsg)

	} else {
		pp.EachLivePeer(to, 255, func(p Peer, po int) bool {
			err := p.Send(pssmsg)

			if err != nil {
				return true
			}
			return false
		})
	}

	return nil
}

func (ps *Pss) isSelfRecipient(to []byte) bool {
	glog.V(logger.Detail).Infof("comparing to %v to localaddr %v", to, ps.GetAddr())
	if bytes.Equal(to, ps.GetAddr()) {
		return true
	}
	return false
}

// if too long topic is sent will return only 0s, should be considered error
func MakeTopic(s string) PssTopic {
	t := [TopicLength]byte{}
	if len(s) <= TopicLength {
		copy(t[:len(s)], s)
	}
	return t
}

func (ps *Pss) MakeMsg(code uint64, msg interface{}) ([]byte, error) {

	/*code, found := pp.ct.GetCode(msg)
	if !found {
		return nil, Errorf("Msg code for msg type %v not registered", reflect.TypeOf(msg))
	}*/

	/*data := PssTestPayload{
		Data: "Bar",
	}*/

	rlpdata, err := rlp.EncodeToBytes(msg)
	if err != nil {
		return nil, err
	}

	// previous attempts corrupted nested structs in the payload iself upon deserializing
	// therefore we use two separate []byte fields instead of peerAddr
	// TODO verify that nested structs cannot be used in rlp
	smsg := &pssPayload{
		Code:   code,
		Size:   uint32(len(rlpdata)),
		Data:   rlpdata,
		SenderOAddr: ps.GetAddr(),
		SenderUAddr: ps.NodeId.NodeID[:],
	}

	rlpbundle, err := rlp.EncodeToBytes(smsg)
	if err != nil {
		return nil, err
	}

	return rlpbundle, nil
}

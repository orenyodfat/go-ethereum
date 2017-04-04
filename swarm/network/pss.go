package network

import (
	"bytes"
	"fmt"
	"time"
	"reflect"

	"github.com/ethereum/go-ethereum/logger"
	"github.com/ethereum/go-ethereum/logger/glog"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/p2p/adapters"
	"github.com/ethereum/go-ethereum/p2p/protocols"
	"github.com/ethereum/go-ethereum/rlp"
)

const (
	DefaultTTL          = 6000
	TopicLength         = 32
	TopicResolverLength = 8
	PssPeerCapacity     = 256
)

type pssPayload struct {
	Sender     []byte
	Code       uint64
	Size       uint32
	Data       []byte
	ReceivedAt time.Time
}

type PssTopic [TopicLength]byte

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

// The pss part encapuslates the kademlia for routing of messages, and the protocol peer address of the node we are operating on
// the C channel is used to pass back received message content from the pssmsg handler
type Pss struct {
	Overlay
	LocalAddr []byte
	C         chan interface{}
	//topics	map[PssTopic]string
	PeerPool map[adapters.NodeId]PssPeer
}

func (ps *Pss) isActive(id adapters.NodeId) bool {
	zeropsspeer := PssPeer{}
	if ps.PeerPool[id] == zeropsspeer {
		return false
	}
	return true
}

func (ps *Pss) GetPssPeer(id adapters.NodeId) PssPeer {
	return ps.PeerPool[id]
}

func NewPss(k Overlay, addr []byte) *Pss {
	return &Pss{
		Overlay:   k,
		LocalAddr: addr,
		C:         make(chan interface{}),
		//topics: make(map[PssTopic]string, TopicResolverLength),
		PeerPool: make(map[adapters.NodeId]PssPeer, PssPeerCapacity),
	}
}

type PssMessenger struct {
	Overlay
	Topic  PssTopic
	Sender []byte
	//Recipient []byte
	rw PssReadWriter
}

func (pm PssMessenger) SendMsg(code uint64, msg interface{}) error {
	// rlp encode msg
	glog.V(logger.Detail).Infof("pss-sending msg code %v: %v", code, msg)
	
	rlpbundle, err := MakePss(code, pm.Sender, msg)
	if err != nil {
		return err
	}
	
	pssenv := PssEnvelope{
		Topic: MakeTopic("42"),
		TTL:   DefaultTTL,
		Data:  rlpbundle,
	}

	pssmsg := PssMsg{
		To: pm.rw.Recipient,
		Data: pssenv,
	}
	
	// send with
	pm.EachLivePeer(pm.rw.Recipient, 255, func(p Peer, po int) bool {
		
		err := p.Send(&pssmsg)

		if err != nil {
			glog.V(logger.Detail).Infof("err in iter: %v, have peer %v", err, reflect.TypeOf(p))
			return true
		}
		return false
	})
	
	return nil
}

func (pm PssMessenger) ReadMsg() (p2p.Msg, error) {
	select {
	case msg := <-pm.rw.rw:
		glog.V(logger.Warn).Infof("pssmsgr readmsg got %v", msg)
		return msg, nil
	}

	return p2p.Msg{}, nil
}

func (pm PssMessenger) Close() {
	return
}

type PssReadWriter struct {
	Recipient []byte
	rw        chan p2p.Msg
}

func (prw PssReadWriter) ReadMsg() (p2p.Msg, error) {
	return p2p.Msg{}, nil
}

func (prw PssReadWriter) WriteMsg(p2p.Msg) error {
	return nil
}

type PssEnvelope struct {
	Topic PssTopic
	TTL   uint16
	Data  []byte
}

type PssProtocol struct {
	*Pss
	Name            string
	Version         uint
	VirtualProtocol *p2p.Protocol
	ct              *protocols.CodeMap
	Insert          func(p2p.Msg) error
}

// a new protocol is run using this signature:
// func NewProtocol(protocolname string, protocolversion uint, run func(*Peer) error, na adapters.NodeAdapter, ct *CodeMap, peerInfo func(id discover.NodeID) interface{}, nodeInfo func() interface{}) *p2p.Protocol {
// the run function is the extended run function to the protocol runnning on the peer, before which a new protocols.peer is created with the messenger passed in the nodeadapter passed in the constructor

// the pssprotocol newprotocol function is a REPLACEMENT which implements the following adjustment:
// * it uses the pssmessenger

func NewPssProtocol(pss *Pss, topic string, version uint, run func(*PssPeer) error, ct *protocols.CodeMap) *PssProtocol {
	pp := &PssProtocol{
		Pss:     pss,
		Name:    topic,
		Version: version,
		ct:      ct,
	}

	r := func(p *p2p.Peer, rw p2p.MsgReadWriter) error {
		prw := rw.(PssReadWriter)
		prw.rw = make(chan p2p.Msg)
		id := adapters.NodeId{
			NodeID: p.ID(),
		}
		pp.Add(p, prw)
		peer := pss.PeerPool[id]

		return run(&peer)
	}

	pp.VirtualProtocol = &p2p.Protocol{
		Name:    string(pp.Name),
		Version: pp.Version,
		Length:  ct.Length(),
		Run:     r,
	}

	return pp
}

func (pp *PssProtocol) Add(p *p2p.Peer, rw PssReadWriter) error {
	zeropsspeer := PssPeer{}
	id := adapters.NodeId{
		NodeID: p.ID(),
	}
	// what is the zero value of psspeer?
	if pp.PeerPool[id] == zeropsspeer {
		pp.PeerPool[id] = PssPeer{
			Peer:    protocols.NewPeer(p, pp.ct, pp.Messenger(rw)),
			insertw: rw.rw,
		}
	}
	return nil
}

func (pp *PssProtocol) Messenger(rw PssReadWriter) PssMessenger {
	prw := rw
	t := MakeTopic(string(pp.Name))
	pm := PssMessenger{
		Overlay: pp.Overlay,
		rw:      prw,
		Sender:  pp.LocalAddr,
		Topic:   t,
	}
	return pm
}

func (pp *PssProtocol) handlePss(msg interface{}) error {
	rmsg := &pssPayload{}
	pssmsg := msg.(*PssMsg)
	to := pssmsg.To
	env := pssmsg.Data

	glog.V(logger.Detail).Infof("pss for us, yay! ...: %v", rmsg)

	//if pp.isSelfRecipient(to) {
	if to != nil {
		err := rlp.DecodeBytes(env.Data, rmsg)
		if err != nil {
			glog.V(logger.Warn).Infof("pss payload encapsulation is corrupt: %v", err)
			return err
		}
		sender := rmsg.Sender
		nid := adapters.NewNodeId(sender)

		if !pp.isActive(*nid) {
			rw := PssReadWriter{
				Recipient: sender,
				rw:        make(chan p2p.Msg),
			}
			p := p2p.NewPeer(nid.NodeID, "foo", []p2p.Cap{})

			// might need to have a channel to check if everything is set up properly
			// fake it here with a small wait
			go pp.VirtualProtocol.Run(p, rw)
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

		psp := pp.GetPssPeer(*nid)
		psp.Insert(imsg)

	} else {
		pp.EachLivePeer(to, 255, func(p Peer, po int) bool {
			err := p.Send(pssmsg)

			if err != nil {
				return true
			}
			return false
		})
	}

	// a small breather here too for demo purpose of current state being make sure the message gets inserted
	time.Sleep(time.Millisecond * 250)
	pp.C <- msg
	return nil
}

type PssMsg struct {
	To   []byte
	Data PssEnvelope
}

func (pm *PssMsg) String() string {
	return fmt.Sprintf("PssMsg: Recipient: %x", pm.To)
}

func (ps *Pss) isSelfRecipient(to []byte) bool {
	glog.V(logger.Detail).Infof("comparing to %v to localaddr %v", to, ps.LocalAddr)
	if bytes.Equal(to, ps.LocalAddr) {
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

func MakePss(code uint64, sender []byte, msg interface{}) ([]byte, error) {

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

	smsg := &pssPayload{
		Code:   code,
		Size:   uint32(len(rlpdata)),
		Data:   rlpdata,
		Sender: sender,
	}

	rlpbundle, err := rlp.EncodeToBytes(smsg)
	if err != nil {
		return nil, err
	}

	return rlpbundle, nil
}

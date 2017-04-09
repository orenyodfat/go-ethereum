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
	PeerReverse map[adapters.NodeId]pot.Address // keep track of all virtual p2p.Peers we are currently speaking to
}

func NewPss(k Overlay, id *adapters.NodeId) *Pss {
	return &Pss{
		Overlay:   k,
		NodeId: id,
		PeerPool: make(map[pot.Address]PssReadWriter, PssPeerCapacity),
		PeerReverse: make(map[adapters.NodeId]pot.Address, PssPeerCapacity),
	}
}

func (ps *Pss) Add(id pot.Address, rw PssReadWriter, nid *adapters.NodeId) error {
	if ps.PeerPool[id] != zeroRW {
		return fmt.Errorf("Peer pss entry '%v' already exists", id)
	}
	ps.PeerPool[id] = rw
	ps.PeerReverse[*nid] = id
	return nil
}

func (ps *Pss) Remove(id pot.Address) {
	ps.PeerPool[id] = zeroRW
	return
}

func (ps *Pss) isActive(addr pot.Address) bool {
	if ps.PeerPool[addr] == zeroRW {
		return false
	}
	return true
}

func (ps *Pss) GetPeerFromNodeId(id adapters.NodeId) pot.Address {
	return ps.PeerReverse[id]
}

func (ps *Pss) Send(to []byte, code uint64, msg interface{}) error {
	// rlp encode msg
	sent := false
	
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
		glog.V(logger.Detail).Infof("PSS-sending msg %v TO %v THROUGH %v", msg, to, p.OverlayAddr())
		return false
	})
	if !sent {
		return fmt.Errorf("Was not able to send to any peers")
	}
	return nil
}

type PssReadWriter struct {
	*Pss
	RecipientOAddr pot.Address
	LastActive time.Time
	rw        chan p2p.Msg
}

func (prw PssReadWriter) ReadMsg() (p2p.Msg, error) {
	msg := <-prw.rw
	glog.V(logger.Warn).Infof("pss readmsg %v", msg.Payload)
	msg.Code += p2p.GetBaseProtocolLength()
	return msg, nil
}

func (prw PssReadWriter) WriteMsg(msg p2p.Msg) error {
	//prw.Pss.decodeAndSend(prw.Recipient.Bytes(), msg)
	return nil
}

func (prw PssReadWriter) InjectMsg(msg p2p.Msg) error {
	glog.V(logger.Warn).Infof("pss injectmsg %v", msg, prw.rw)
	prw.rw <- msg
	return nil
}

// this messenger transports replies from the virtual protocol message handlers to the PssReadWriter
// WITHOUT rlpencoding the content, and passes it on to PssReadWriter.WriteMsg
type PssMessenger struct {
	rw p2p.MsgReadWriter
	RecipientUAddr adapters.NodeId
	peerLookup func(adapters.NodeId) pot.Address
	send func([]byte, uint64, interface{}) error
}

func (self *PssMessenger) SendMsg(code uint64, msg interface{}) error {
	oaddr := self.peerLookup(self.RecipientUAddr)
	return self.send(oaddr.Bytes(), code, msg)
}

func (self *PssMessenger) ReadMsg() (p2p.Msg, error) {
	return self.rw.ReadMsg()
}

// this is not in use
func (self *PssMessenger) Close() {
}


type PssProtocol struct {
	*Pss
	Name            string
	Version         uint
	VirtualProtocol *p2p.Protocol
	ct              *protocols.CodeMap
}

// PssProtocol holds the virtual protocol, which will handle the pss payload
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
				Pss: pp.Pss,
				RecipientOAddr: oaddrhash.Address,
				rw:        make(chan p2p.Msg),
			}
			p := p2p.NewPeer(nid.NodeID, "foo", []p2p.Cap{})
			
			go func() {
				pp.Add(oaddrhash.Address, rw, nid)
				pp.VirtualProtocol.Run(p, rw)
				pp.Remove(oaddrhash.Address)
				close(rw.rw)
				return
			}()
		}
		
		// wait for the virtualpeer to get into the readloop
		time.Sleep(time.Millisecond * 250)
		
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

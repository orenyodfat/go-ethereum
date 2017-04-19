package network

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/logger"
	"github.com/ethereum/go-ethereum/logger/glog"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/p2p/adapters"
	"github.com/ethereum/go-ethereum/p2p/protocols"
	"github.com/ethereum/go-ethereum/pot"
	"github.com/ethereum/go-ethereum/rlp"
)

const (
	DefaultTTL                  = 6000
	TopicLength                 = 32
	TopicResolverLength         = 8
	PssPeerCapacity             = 256
	PssPeerTopicDefaultCapacity = 8
)

var (
	zeroRW    = PssReadWriter{}
	zeroTopic = PssTopic{}
)

type PssParams struct {
}

func NewPssParams() *PssParams {
	return &PssParams{}
}

type PssMsg struct {
	To      []byte
	Payload PssEnvelope
}

func (pm *PssMsg) String() string {
	return fmt.Sprintf("PssMsg: Recipient: %x", pm.To)
}

// Pre-Whisper placeholder
type PssEnvelope struct {
	Topic       PssTopic
	TTL         uint16
	Payload     []byte
	SenderOAddr []byte
	SenderUAddr []byte
}

// Pre-Whisper placeholder
type pssPayload struct {
	Code       uint64
	Size       uint32
	Data       []byte
	ReceivedAt time.Time
}

type PssTopic [TopicLength]byte

// Pss master object provides:
// - access to the swarm overlay and routing (kademlia)
// - a collection of remote overlay addresses mapped to MsgReadWriters, representing the virtually connected peers
// - a collection of remote underlay address, mapped to the overlay addresses above
// - a method to send a message to specific overlayaddr
type Pss struct {
	Overlay                                              // we can get the overlayaddress from this
	NodeId   *adapters.NodeId                            // we need the underlayaddr to make virtual p2p.Peers in the receiving end
	PeerPool map[pot.Address]map[PssTopic]*PssReadWriter // keep track of all virtual p2p.Peers we are currently speaking to
	//PeerReverse map[adapters.NodeId]pot.Address // keep track of all virtual p2p.Peers we are currently speaking to
	handlers map[PssTopic]func([]byte, *p2p.Peer, []byte) error // topic and version based pss payload handlers
}

// todo error check overlay integrity
func NewPss(k Overlay) *Pss {
	return &Pss{
		Overlay:  k,
		NodeId:   adapters.NewNodeId(k.GetAddr().UnderlayAddr()),
		PeerPool: make(map[pot.Address]map[PssTopic]*PssReadWriter, PssPeerCapacity),
		//PeerReverse: make(map[adapters.NodeId]pot.Address, PssPeerCapacity),
		handlers: make(map[PssTopic]func([]byte, *p2p.Peer, []byte) error),
	}
}

func (self *Pss) Register(name string, version int, handler func(msg []byte, p *p2p.Peer, from []byte) error) error {
	t, err := MakeTopic(name, version)
	if err != nil {
		return err
	}
	self.handlers[t] = handler
	return nil
}

func (self *Pss) GetHandler(name string, version int) func([]byte, *p2p.Peer, []byte) error {
	t, err := MakeTopic(name, version) // if deterministic, maybe its not after we have topic obfuscation
	if err != nil {
		return nil
	}
	return self.handlers[t]
}

func (self *Pss) AddPeerTopic(id pot.Address, topic PssTopic, rw *PssReadWriter) error {
	if self.PeerPool[id][topic] == nil {
		self.PeerPool[id] = make(map[PssTopic]*PssReadWriter, PssPeerTopicDefaultCapacity)
	} else {
		glog.V(logger.Detail).Infof("replacing pss peerpool entry peer '%v' topic '%v'", id, topic)
	}

	self.PeerPool[id][topic] = rw
	//self.PeerReverse[*nid] = id
	return nil
}

// TODO also remove the reverse
func (self *Pss) RemovePeerTopic(id pot.Address, topic PssTopic) {
	self.PeerPool[id][topic] = nil
	return
}

func (self *Pss) RemovePeer(id pot.Address) {
	self.PeerPool[id] = nil
	return
}

func (self *Pss) isActive(id pot.Address, topic PssTopic) bool {
	if self.PeerPool[id][topic] == nil {
		return false
	}
	return true
}


/*
func (self *Pss) GetPeerFromNodeId(id adapters.NodeId) pot.Address {
	return self.PeerReverse[id]
}
*/

// we have the send on the self., because if we should be able to initiate a send without worring about the rest of the stack
// the reply to incoming self.msgs also ends up here in the end
func (self *Pss) Send(to []byte, code uint64, msg interface{}) error {

	rlpbundle, err := MakeMsg(code, msg)
	if err != nil {
		return err
	}

	t, err := MakeTopic("foo", 42)

	pssenv := PssEnvelope{
		Topic:   t,
		TTL:     DefaultTTL,
		Payload: rlpbundle,
	}

	pssmsg := &PssMsg{
		To:      to,
		Payload: pssenv,
	}

	return self.Forward(pssmsg)
}

func (self *Pss) Forward(msg *PssMsg) error {

	sent := false
	
	// send with kademlia
	// find the closest peer to the recipient and attempt to send
	self.Overlay.EachLivePeer(msg.To, 255, func(p Peer, po int) bool {
		glog.V(logger.Debug).Infof("Attempting PSS-relay msg %v TO %x THROUGH %x", msg, msg.To, p.OverlayAddr())
		err := p.Send(msg)
		if err != nil {
			glog.V(logger.Warn).Infof("Attempting PSS-relay msg %v TO %x THROUGH %x FAILED: %v", msg, msg.To, p.OverlayAddr(), err)
			return true
		}
		sent = true
		return false
	})
	if !sent {
		return fmt.Errorf("PSS Was not able to send to any peers")
	}
	
	return nil
}

// we need the pss pointer here to access the final send
type PssReadWriter struct {
	*Pss
	RecipientOAddr pot.Address
	LastActive     time.Time
	rw             chan p2p.Msg
	ct             *protocols.CodeMap
}

// get incoming msg
func (prw PssReadWriter) ReadMsg() (p2p.Msg, error) {
	msg := <-prw.rw
	glog.V(logger.Detail).Infof("pss readmsg %v", msg)

	// p2p.ProtoRW.ReadMsg() offset deducts this later, we need to add to get past the baseProtocolLength check in p2p.handle()
	//msg.Code += p2p.GetBaseProtocolLength()
	return msg, nil
}

// currently not in use (because we can't access the original msg struct to pass on to Pss.Send)
func (prw PssReadWriter) WriteMsg(msg p2p.Msg) error {
	glog.V(logger.Detail).Infof("pss writemsg %v", msg)
	ifc, found := prw.ct.GetInterface(msg.Code)
	if !found {
		return fmt.Errorf("Writemsg couldn't find matching interface for code %d", msg.Code)
	}
	rlp.Decode(msg.Payload, ifc)
	to := prw.RecipientOAddr.Bytes()

	return prw.Pss.Send(to, msg.Code, ifc)
}

// insert a incoming message on the p2p.VirtualPeer hack
func (prw PssReadWriter) injectMsg(msg p2p.Msg) error {
	glog.V(logger.Detail).Infof("pss injectmsg %v", msg)
	prw.rw <- msg
	return nil
}

// encapsulates all the complexity of the bridging of real-to-virtual protocol instantiations
// TODO: needs review to see if it can be made leaner
type PssProtocol struct {
	*Pss
	Topic           *PssTopic
	VirtualProtocol *p2p.Protocol
	ct              *protocols.CodeMap
}

func NewPssProtocol(pss *Pss, topic *PssTopic, ct *protocols.CodeMap, targetprotocol *p2p.Protocol) *PssProtocol {
	pp := &PssProtocol{
		Pss:             pss,
		Topic:           topic,
		VirtualProtocol: targetprotocol,
		ct:              ct,
	}
	return pp
}

func (self *PssProtocol) GetHandler() func([]byte, *p2p.Peer, []byte) error {
	return self.handle
}

func (self *PssProtocol) handle(msg []byte, p *p2p.Peer, senderAddr []byte) error {
	hashoaddr := pot.NewHashAddressFromBytes(senderAddr).Address
	if !self.isActive(hashoaddr, *self.Topic) {
		rw := &PssReadWriter{
			Pss:            self.Pss,
			RecipientOAddr: hashoaddr,
			rw:             make(chan p2p.Msg),
			ct:             self.ct,
		}
		self.Pss.AddPeerTopic(hashoaddr, *self.Topic, rw)
		go func() {
			err := self.VirtualProtocol.Run(p, rw)
			glog.V(logger.Detail).Infof("vprotocol on addr %v topic %v quit: %v", hashoaddr, self.Topic, err)
		}()
		// we need a protocol ready reporting channel here
	}

	payload := &pssPayload{}
	rlp.DecodeBytes(msg, payload)

	pmsg := p2p.Msg{
		Code:       payload.Code,
		Size:       uint32(len(payload.Data)),
		ReceivedAt: time.Now(),
		Payload:    bytes.NewBuffer(payload.Data),
	}

	self.Pss.PeerPool[hashoaddr][*self.Topic].injectMsg(pmsg)

	return nil
}

func (ps *Pss) isSelfRecipient(msg *PssMsg) bool {
	glog.V(logger.Detail).Infof("comparing to %v to localaddr %v", msg.To, ps.GetAddr().OverlayAddr())
	if bytes.Equal(ps.GetMsgRecipient(msg), ps.Overlay.GetAddr().OverlayAddr()) {
		return true
	}
	return false
}

func (ps *Pss) GetMsgRecipient(msg *PssMsg) []byte {
	return msg.To
}


// references the p2p.Msg structure, but with non-buffered byte payload, and with sender address
// Pre-Whisper placeholder
func MakeMsg(code uint64, msg interface{}) ([]byte, error) {

	rlpdata, err := rlp.EncodeToBytes(msg)
	if err != nil {
		return nil, err
	}

	// previous attempts corrupted nested structs in the payload iself upon deserializing
	// therefore we use two separate []byte fields instead of peerAddr
	// TODO verify that nested structs cannot be used in rlp
	smsg := &pssPayload{
		Code: code,
		Size: uint32(len(rlpdata)),
		Data: rlpdata,
	}

	rlpbundle, err := rlp.EncodeToBytes(smsg)
	if err != nil {
		return nil, err
	}

	return rlpbundle, nil
}

// if too long topic is sent will return only 0s, should be considered error
// Pre-Whisper placeholder
func MakeTopic(s string, v int) (PssTopic, error) {
	t := [TopicLength]byte{}
	if len(s)+4 <= TopicLength {
		copy(t[4:len(s)+4], s)
	} else {
		return t, fmt.Errorf("topic '%t' too long", s)
	}
	binary.PutVarint(t[:4], int64(v))
	return t, nil
}

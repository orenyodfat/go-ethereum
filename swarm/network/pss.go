package network

import (
	"bytes"
	"fmt"
	"time"
	"encoding/binary"

	"github.com/ethereum/go-ethereum/logger"
	"github.com/ethereum/go-ethereum/logger/glog"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/p2p/adapters"
	"github.com/ethereum/go-ethereum/p2p/protocols"
	"github.com/ethereum/go-ethereum/pot"
	"github.com/ethereum/go-ethereum/rlp"
)

const (
	DefaultTTL          = 6000
	TopicLength         = 32
	TopicResolverLength = 8
	PssPeerCapacity     = 256
	PssPeerTopicDefaultCapacity = 8
)

var (
	zeroRW = PssReadWriter{}
	zeroTopic = PssTopic{}
)

type PssParams struct {	
}

func NewPssParams() *PssParams {
	return &PssParams{}
}

type PssMsg struct {
	To   []byte
	Payload PssEnvelope
}

func (pm *PssMsg) String() string {
	return fmt.Sprintf("PssMsg: Recipient: %x", pm.To)
}

// Pre-Whisper placeholder
type PssEnvelope struct {
	Topic PssTopic
	TTL   uint16
	Payload  []byte
	SenderOAddr []byte
	SenderUAddr []byte
}

// Pre-Whisper placeholder
type pssPayload struct {
	SenderOAddr []byte
	SenderUAddr []byte
	Code        uint64
	Size        uint32
	Data        []byte
	ReceivedAt  time.Time
}

type PssTopic [TopicLength]byte

// Pss master object provides:
// - access to the swarm overlay and routing (kademlia)
// - a collection of remote overlay addresses mapped to MsgReadWriters, representing the virtually connected peers
// - a collection of remote underlay address, mapped to the overlay addresses above
// - a method to send a message to specific overlayaddr
type Pss struct {
	Overlay                                     // we can get the overlayaddress from this
	NodeId      *adapters.NodeId                // we need the underlayaddr to make virtual p2p.Peers in the receiving end
	PeerPool    map[pot.Address]map[PssTopic]*PssReadWriter   // keep track of all virtual p2p.Peers we are currently speaking to
	//PeerReverse map[adapters.NodeId]pot.Address // keep track of all virtual p2p.Peers we are currently speaking to
	handlers	map[PssTopic]func([]byte, *p2p.Peer, []byte) error // topic and version based pss payload handlers
}

func NewPss(k Overlay, id *adapters.NodeId) *Pss {
	return &Pss{
		Overlay:     k,
		NodeId:      id,
		PeerPool:    make(map[pot.Address]map[PssTopic]*PssReadWriter, PssPeerCapacity),
		//PeerReverse: make(map[adapters.NodeId]pot.Address, PssPeerCapacity),
		handlers:	 make(map[PssTopic]func([]byte, *p2p.Peer, []byte) error),
	}
}

func (self *Pss) Register(name string, version int, handler func(msg []byte, p *p2p.Peer, from []byte) error) error {
	t, err := self.MakeTopic(name, version)
	if err != nil {
		return err
	}
	self.handlers[t] = handler
	return nil
}

func (self *Pss) GetHandler (name string, version int) func([]byte, *p2p.Peer, []byte) error {
	t, err := self.MakeTopic(name, version) // if deterministic, maybe its not after we have topic obfuscation
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

// if too long topic is sent will return only 0s, should be considered error
// Pre-Whisper placeholder
func (self *Pss) MakeTopic(s string, v int) (PssTopic, error) {
	t := [TopicLength]byte{}
	if len(s) + 4 <= TopicLength {
		copy(t[4:len(s) + 4], s)
	} else {
		return t, fmt.Errorf("topic '%t' too long", s)
	}
	binary.PutVarint(t[:4], int64(v))
	return t, nil
}

// references the p2p.Msg structure, but with non-buffered byte payload, and with sender address
// Pre-Whisper placeholder
func (ps *Pss) MakeMsg(code uint64, msg interface{}) ([]byte, error) {

	rlpdata, err := rlp.EncodeToBytes(msg)
	if err != nil {
		return nil, err
	}

	// previous attempts corrupted nested structs in the payload iself upon deserializing
	// therefore we use two separate []byte fields instead of peerAddr
	// TODO verify that nested structs cannot be used in rlp
	smsg := &pssPayload{
		Code:        code,
		Size:        uint32(len(rlpdata)),
		Data:        rlpdata,
		SenderOAddr: ps.GetAddr(),
		SenderUAddr: ps.NodeId.NodeID[:],
	}

	rlpbundle, err := rlp.EncodeToBytes(smsg)
	if err != nil {
		return nil, err
	}

	return rlpbundle, nil
}

/*
func (self *Pss) GetPeerFromNodeId(id adapters.NodeId) pot.Address {
	return self.PeerReverse[id]
}
*/

// we have the send on the self., because if we should be able to initiate a send without worring about the rest of the stack
// the reply to incoming self.msgs also ends up here in the end
func (self *Pss) Send(to []byte, code uint64, msg interface{}) error {

	sent := false

	rlpbundle, err := self.MakeMsg(code, msg)
	if err != nil {
		return err
	}

	t, err := self.MakeTopic("foo", 42)
	 
	pssenv := PssEnvelope{
		Topic: t,
 		TTL:   DefaultTTL,
		Payload:  rlpbundle,
	}

	pssmsg := PssMsg{
		To:   to,
		Payload: pssenv,
	}

	// send with kademlia
	// find the closest peer to the recipient and attempt to send
	self.Overlay.EachLivePeer(to, 255, func(p Peer, po int) bool {
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

// we need the pss pointer here to access the final send
type PssReadWriter struct {
	*Pss
	RecipientOAddr pot.Address
	LastActive     time.Time
	rw             chan p2p.Msg
	ct			   *protocols.CodeMap
}

// get incoming msg
func (prw PssReadWriter) ReadMsg() (p2p.Msg, error) {
	msg := <-prw.rw
	glog.V(logger.Detail).Infof("pss readmsg %v", msg)

	// p2p.ProtoRW.ReadMsg() offset deducts this later, we need to add to get past the baseProtocolLength check in p2p.handle()
	msg.Code += p2p.GetBaseProtocolLength()
	return msg, nil
}

// currently not in use (because we can't access the original msg struct to pass on to Pss.Send)
func (prw PssReadWriter) WriteMsg(msg p2p.Msg) error {
	return nil
}

// insert a incoming message on the p2p.VirtualPeer hack
func (prw PssReadWriter) injectMsg(msg p2p.Msg) error {
	glog.V(logger.Detail).Infof("pss injectmsg %v", msg)
	prw.rw <- msg
	return nil
}

// this messenger transports replies from the virtual protocol message handlers to the PssReadWriter
// WITHOUT rlpencoding the content
// we need it because we're using protocols.Peer too
type PssMessenger struct {
	rw             p2p.MsgReadWriter
	RecipientUAddr adapters.NodeId
	peerLookup     func(adapters.NodeId) pot.Address
	send           func([]byte, uint64, interface{}) error
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

// encapsulates all the complexity of the bridging of real-to-virtual protocol instantiations
// TODO: needs review to see if it can be made leaner
type PssProtocol struct {
	*Pss
	Topic			*PssTopic
	VirtualProtocol *p2p.Protocol
	ct              *protocols.CodeMap
}

func NewPssProtocol(pss *Pss, topic *PssTopic, ct *protocols.CodeMap, targetprotocol *p2p.Protocol) *PssProtocol {
	pp := &PssProtocol{
		Pss: pss,
		Topic:	topic,
		VirtualProtocol: targetprotocol,
		ct: ct,
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
			ct:				self.ct,
		}
		self.Pss.AddPeerTopic(hashoaddr, *self.Topic, rw)
		self.VirtualProtocol.Run(p, rw)
		// we need a protocol ready reporting channel here
	}
	
	payload := pssPayload{}
	rlp.DecodeBytes(msg, payload)
	
	pmsg := p2p.Msg{
		Code: payload.Code,
		Size: uint32(len(payload.Data)),
		ReceivedAt: time.Now(),
		Payload: bytes.NewBuffer(payload.Data),
	}
	
	self.Pss.PeerPool[hashoaddr][*self.Topic].injectMsg(pmsg)
	
	return nil
}

/*
func NewOldPssProtocol(pss *Pss, name string, version uint, run func(*p2p.VirtualPeer) error, ct *protocols.CodeMap) *PssProtocol {
	pp := &PssProtocol{
		Pss:     pss,
		Topic:	pss.MakeTopic(name, version)
		ct:      ct,
	}

	r := func(p *p2p.Peer, rw p2p.MsgReadWriter) error {
		cap := p2p.Cap{
			Name:    topic,
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
*/

// get pssmsg, find out which protocol (and who it's from), fire up protocol, pass it to protocol
// if it's not for us, pass it on
/*
func (pp *PssProtocol) handlePss(msg interface{}) error {

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
				Pss:            pp.Pss,
				RecipientOAddr: oaddrhash.Address,
				rw:             make(chan p2p.Msg),
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
		// TODO: find out how to hook into the writestart channel of the virtualpeer, so we know when we can proceed
		time.Sleep(time.Millisecond * 250)

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
*/
func (ps *Pss) isSelfRecipient(msg PssMsg) bool {
	glog.V(logger.Detail).Infof("comparing to %v to localaddr %v", msg.To, ps.GetAddr())
	if bytes.Equal(ps.GetMsgRecipient(msg), ps.GetAddr()) {
		return true
	}
	return false
}

func (ps *Pss) GetMsgRecipient(msg PssMsg) []byte {
	return msg.To
}

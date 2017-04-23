package network

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/logger"
	"github.com/ethereum/go-ethereum/logger/glog"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/p2p/adapters"
	"github.com/ethereum/go-ethereum/p2p/protocols"
	"github.com/ethereum/go-ethereum/pot"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/swarm/storage"
)

const (
	DefaultTTL                  = 6000
	TopicLength                 = 32
	TopicResolverLength         = 8
	PssPeerCapacity             = 256
	PssPeerTopicDefaultCapacity = 8
	digestLength                = 64
	digestCapacity              = 256
	defaultDigestCacheTTL       = time.Second
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
	return fmt.Sprintf("PssMsg: Recipient: %x", pm.To[:8])
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

type pssDigest uint32

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
	fwdcache map[pssDigest]time.Time                            // checksum of unique fields from pssmsg mapped to expiry, cache to determine whether to drop msg
	cachettl time.Duration                                      // how long to keep messages in fwdcache
	hasher   hash.Hash                                          // hasher to digest message to cache
}

// 32bit checks should be enough for this use
func (self *Pss) hashMsg(msg *PssMsg) pssDigest {
	self.hasher.Reset()
	self.hasher.Write(msg.To)
	self.hasher.Write(msg.Payload.SenderUAddr)
	self.hasher.Write(msg.Payload.SenderOAddr)
	self.hasher.Write(msg.Payload.Topic[:])
	self.hasher.Write(msg.Payload.Payload)
	b := self.hasher.Sum([]byte{})
	return pssDigest(binary.BigEndian.Uint32(b))
}

// todo error check overlay integrity
func NewPss(k Overlay, cachettl time.Duration) *Pss {
	if cachettl == 0 {
		cachettl = defaultDigestCacheTTL
	}
	return &Pss{
		Overlay:  k,
		NodeId:   adapters.NewNodeId(k.GetAddr().UnderlayAddr()),
		PeerPool: make(map[pot.Address]map[PssTopic]*PssReadWriter, PssPeerCapacity),
		//PeerReverse: make(map[adapters.NodeId]pot.Address, PssPeerCapacity),
		handlers: make(map[PssTopic]func([]byte, *p2p.Peer, []byte) error),
		fwdcache: make(map[pssDigest]time.Time),
		cachettl: cachettl,
		hasher:   storage.MakeHashFunc("SHA3")(),
	}
}

// when full oldest out
// periodically clean cache
func (self *Pss) addToFwdCache(digest pssDigest) error {
	self.fwdcache[digest] = time.Now().Add(self.cachettl)
	return nil
}

// returns true if in cache AND not expired
func (self *Pss) checkCache(digest pssDigest) bool {
	entry, ok := self.fwdcache[digest]
	if !ok || entry.Before(time.Now()) {
		return false
	}
	return true
}

func (self *Pss) Register(name string, version int, handler func(msg []byte, p *p2p.Peer, from []byte) error) error {
	t, err := MakeTopic(name, version)
	if err != nil {
		return err
	}
	self.handlers[t] = handler
	return nil
}

func (self *Pss) GetHandler(topic PssTopic) func([]byte, *p2p.Peer, []byte) error {
	return self.handlers[topic]
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

// we have the send on the self., because if we should be able to initiate a send without worring about the rest of the stack
// the reply to incoming self.msgs also ends up here in the end
func (self *Pss) Send(to []byte, topic PssTopic, msg []byte) error {

	pssenv := PssEnvelope{
		SenderOAddr: self.Overlay.GetAddr().OverlayAddr(),
		SenderUAddr: self.Overlay.GetAddr().UnderlayAddr(),
		Topic:       topic,
		TTL:         DefaultTTL,
		Payload:     msg,
	}

	pssmsg := &PssMsg{
		To:      to,
		Payload: pssenv,
	}

	return self.Forward(pssmsg)
}

func (self *Pss) Forward(msg *PssMsg) error {

	if self.isSelfRecipient(msg) {
		return newPssError(pssError_ForwardToSelf)
	}

	digest := self.hashMsg(msg)

	if self.checkCache(digest) {
		glog.V(logger.Detail).Infof("Found in block-cache: PSS-relay msg FROM %x TO %x", common.ByteLabel(self.Overlay.GetAddr().OverlayAddr()), common.ByteLabel(msg.To))
		return nil
		//return newPssError(pssError_BlockByCache)
	}

	self.addToFwdCache(digest)

	// TODO:check integrity of message

	sent := false

	// send with kademlia
	// find the closest peer to the recipient and attempt to send
	self.Overlay.EachLivePeer(msg.To, 256, func(p Peer, po int) bool {
		glog.V(logger.Debug).Infof("Attempting PSS-relay msg FROM %x TO %x THROUGH %x (%x)", common.ByteLabel(self.Overlay.GetAddr().OverlayAddr()), common.ByteLabel(msg.To), common.ByteLabel(p.OverlayAddr()), common.ByteLabel(p.UnderlayAddr()))
		err := p.Send(msg)
		if err != nil {
			glog.V(logger.Warn).Infof("Attempting PSS-relay msg FROM %x TO %x THROUGH %x (%x) FAILED: %v", common.ByteLabel(self.Overlay.GetAddr().OverlayAddr()), common.ByteLabel(msg.To), common.ByteLabel(p.OverlayAddr()), common.ByteLabel(p.UnderlayAddr()), err)
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
	topic          *PssTopic
}

// get incoming msg
func (prw PssReadWriter) ReadMsg() (p2p.Msg, error) {
	msg := <-prw.rw

	glog.V(logger.Detail).Infof("pssrw readmsg %v", msg)

	return msg, nil
}

func (prw PssReadWriter) WriteMsg(msg p2p.Msg) error {
	glog.V(logger.Detail).Infof("pssrw writemsg %v", msg)
	ifc, found := prw.ct.GetInterface(msg.Code)
	if !found {
		return fmt.Errorf("Writemsg couldn't find matching interface for code %d", msg.Code)
	}
	msg.Decode(ifc)

	to := prw.RecipientOAddr.Bytes()

	glog.V(logger.Detail).Infof("pssrw writemsg %v", msg)

	pmsg, _ := MakeMsg(msg.Code, ifc)

	return prw.Pss.Send(to, *prw.topic, pmsg)
}

// insert a incoming message on the p2p.VirtualPeer hack
func (prw PssReadWriter) injectMsg(msg p2p.Msg) error {
	glog.V(logger.Detail).Infof("pssrw injectmsg %v", msg)
	prw.rw <- msg
	return nil
}

// encapsulates all the complexity of the bridging of real-to-virtual protocol instantiations
// TODO: needs review to see if it can be made leaner
type PssProtocol struct {
	*Pss
	virtualProtocol *p2p.Protocol
	topic           *PssTopic
	ct              *protocols.CodeMap
}

func NewPssProtocol(pss *Pss, topic *PssTopic, ct *protocols.CodeMap, targetprotocol *p2p.Protocol) *PssProtocol {
	pp := &PssProtocol{
		Pss:             pss,
		virtualProtocol: targetprotocol,
		topic:           topic,
		ct:              ct,
	}
	return pp
}

func (self *PssProtocol) GetHandler() func([]byte, *p2p.Peer, []byte) error {
	return self.handle
}

func (self *PssProtocol) handle(msg []byte, p *p2p.Peer, senderAddr []byte) error {
	hashoaddr := pot.NewHashAddressFromBytes(senderAddr).Address
	if !self.isActive(hashoaddr, *self.topic) {
		rw := &PssReadWriter{
			Pss:            self.Pss,
			RecipientOAddr: hashoaddr,
			rw:             make(chan p2p.Msg),
			ct:             self.ct,
			topic:          self.topic,
		}
		self.Pss.AddPeerTopic(hashoaddr, *self.topic, rw)
		go func() {
			err := self.virtualProtocol.Run(p, rw)
			glog.V(logger.Detail).Infof("vprotocol on addr %v topic %v quit: %v", hashoaddr, self.topic, err)
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

	self.Pss.PeerPool[hashoaddr][*self.topic].injectMsg(pmsg)

	return nil
}

func (ps *Pss) isSelfRecipient(msg *PssMsg) bool {
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

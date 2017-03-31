package network

import (
	"bytes"
	"encoding/gob"
	
	"github.com/ethereum/go-ethereum/p2p/adapters"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/p2p/protocols"
	"github.com/ethereum/go-ethereum/logger"
	"github.com/ethereum/go-ethereum/logger/glog"
)

const (
	DefaultTTL	= 6000
	TopicLength = 32
)

type PssTopic [TopicLength]byte

// The pss part encapuslates the kademlia for routing of messages, and the protocol peer address of the node we are operating on
// the C channel is used to pass back received message content from the pssmsg handler
type Pss struct {
	Overlay
	LocalAddr	[]byte
	C	chan PssEnvelope
}

func NewPss(k Overlay, addr []byte) *Pss {
	return &Pss{
		Overlay: k,
		LocalAddr: addr,
		C: make(chan PssEnvelope),
	}
}

// sender
// packaged message
// 

type PssMessenger struct {
	Overlay
	Topic PssTopic
	Sender []byte
	Recipient []byte
}

func (pm *PssMessenger) SendMsg(code uint64, msg interface{}) error{
	// rlp encode msg

	// wrap data 
	pe := PssEnvelope{
		Topic: pm.Topic,
		TTL: DefaultTTL,
		Data: tmpSerialize(msg),
	}
	
	pssmsg := PssMsg{
		To: pm.Recipient,
		Data: pe,
	}
	
	// send with 
	pm.EachLivePeer(pm.Recipient, 255, func(p Peer, po int) bool {
		err := p.Send(pssmsg)
		
		if err != nil {
			return true
		}
		return false
	})
	return nil
}

func (pm *PssMessenger) ReadMsg() (p2p.Msg, error) {
	return p2p.Msg{}, nil
}

func (pm *PssMessenger) Close() {
	return
}

type PssReadWriter struct {
	Recipient []byte
}

func (prw PssReadWriter) ReadMsg() (p2p.Msg, error) {
	return p2p.Msg{}, nil
}

func (prw PssReadWriter) WriteMsg(p2p.Msg) error {
	return nil
}

type PssEnvelope struct {
	Topic PssTopic
	TTL uint16
	Data []byte
}

type PssProtocol struct {
	*Pss
	Name []byte
	Version uint
}

func (pp *PssProtocol) NewProtocol(run func(*protocols.Peer) error, ct *protocols.CodeMap) *p2p.Protocol {

	r := func(p *p2p.Peer, rw p2p.MsgReadWriter) error {

		m := pp.Messenger(rw.(PssReadWriter))

		peer := protocols.NewPeer(p, ct, m)
		return run(peer)

	}
	
	return &p2p.Protocol{
		Name:     string(pp.Name),
		Version:  pp.Version,
		Length:   ct.Length(),
		Run:      r,
	}
}

//func (pp *PssProtocol) Messenger(rw p2p.MsgReadWriter) adapters.Messenger {
func (pp *PssProtocol) Messenger(rw PssReadWriter) adapters.Messenger {
	prw := rw
	t := makeTopic(pp.Name)
	pm := &PssMessenger{
		Overlay: pp.Overlay,
		Recipient: prw.Recipient,
		Sender: pp.LocalAddr,
		Topic: t,
	}
	return adapters.Messenger(pm)
}

type PssMsg struct {
	To   []byte
	Data	PssEnvelope
}

func (pm *PssMsg) String() string {
	return fmt.Sprintf("PssMsg: Recipient: %x", pm.To)
}

func (ps *Pss) HandlePssMsg(msg interface{}) error {
	pssmsg := msg.(*PssMsg)
	to := pssmsg.To
	env := pssmsg.Data
	if ps.isSelfRecipient(to) {
		glog.V(logger.Detail).Infof("Pss to us, yay!", to)
		ps.C <- env
		return nil
	}
	
	ps.EachLivePeer(to, 255, func(p Peer, po int) bool {
		err := p.Send(pssmsg)
		
		if err != nil {
			return true
		}
		return false
	})

	return nil
}

func (ps *Pss) isSelfRecipient(to []byte) bool {
	if bytes.Equal(to, ps.LocalAddr) {
		return true
	}
	return false
}

// if too long topic is sent will return only 0s, should be considered error
func makeTopic(b []byte) PssTopic {
	t := [TopicLength]byte{}
	if len(b) <= TopicLength {
		copy(t[:len(b)], b[:len(b)])
	}
	return t
}

func tmpSerialize(msg interface{}) []byte {
	var smsg bytes.Buffer
	enc := gob.NewEncoder(&smsg)
	err := enc.Encode(msg)
	if err != nil {
		return []byte{}
	}
	return smsg.Bytes()
}

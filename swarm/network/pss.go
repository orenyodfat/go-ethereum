package network

import (
	"bytes"
	
	"github.com/ethereum/go-ethereum/p2p/adapters"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/p2p/protocols"
	"github.com/ethereum/go-ethereum/logger"
	"github.com/ethereum/go-ethereum/logger/glog"
	"github.com/ethereum/go-ethereum/rlp"
)

const (
	DefaultTTL	= 6000
	TopicLength = 32
	TopicResolverLength = 8
)

type PssTopic [TopicLength]byte

// The pss part encapuslates the kademlia for routing of messages, and the protocol peer address of the node we are operating on
// the C channel is used to pass back received message content from the pssmsg handler
type Pss struct {
	Overlay
	LocalAddr	[]byte
	C	chan interface{}
	topics	map[PssTopic]string
}

func NewPss(k Overlay, addr []byte) *Pss {
	return &Pss{
		Overlay: k,
		LocalAddr: addr,
		C: make(chan interface{}),
		topics: make(map[PssTopic]string, TopicResolverLength),
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
	/*pe := PssEnvelope{
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
	})*/
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
	Name string
	Version uint
	Peer Peer
	VirtualProtocol *p2p.Protocol
	ct *protocols.CodeMap
}

func (pp *PssProtocol) setPeer(p Peer) {
	pp.Peer = p
}
/*
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
*/
//func (pp *PssProtocol) Messenger(rw p2p.MsgReadWriter) adapters.Messenger {
func (pp *PssProtocol) Messenger(rw PssReadWriter) adapters.Messenger {
	prw := rw
	t := pp.MakeTopic(string(pp.Name))
	pm := &PssMessenger{
		Overlay: pp.Overlay,
		Recipient: prw.Recipient,
		Sender: pp.LocalAddr,
		Topic: t,
	}
	return adapters.Messenger(pm)
}

func (pp *PssProtocol) HandlePssMsg(msg interface{}) error {
	rmsg := &pssPayload{}
	pssmsg := msg.(*PssMsg)
	to := pssmsg.To
	env := pssmsg.Data
	if pp.isSelfRecipient(to) {
		glog.V(logger.Detail).Infof("pssmsg wcontent: %v", env.Data)
		err := rlp.DecodeBytes(env.Data, rmsg)
		if err != nil {
			glog.V(logger.Warn).Infof("pss payload encapsulation is corrupt: %v", err)
			return err
		}
		
		pmsg, found := pp.ct.GetInterface(rmsg.Code)
		if !found {
			glog.V(logger.Warn).Infof("message code %v not recognized, discarding", rmsg.Code)
			return err
		}
		err = rlp.DecodeBytes(rmsg.Data, &pmsg)
		if err != nil {
			glog.V(logger.Warn).Infof("pss payload data does not fit in interface %v (code %v): %v", pmsg, rmsg.Code, err)
			return err
		}
		//size := uint8(env.Data[0])
		//submsgtype := string(env.Data[1:size+1])
		//submsg := env.Data[size+1:]
		glog.V(logger.Detail).Infof("rlp decoded %v", pmsg)
		//submsg := tmpDeserialize(, p.Codfe)
		// resolve topic to protocol
		// find messagetype from codemap
		pp.C <- env
		return nil
	}
	
	pp.EachLivePeer(to, 255, func(p Peer, po int) bool {
		err := p.Send(pssmsg)
		
		if err != nil {
			return true
		}
		return false
	})

	return nil
}

type PssMsg struct {
	To   []byte
	Data	PssEnvelope
}

func (pm *PssMsg) String() string {
	return fmt.Sprintf("PssMsg: Recipient: %x", pm.To)
}


func (ps *Pss) isSelfRecipient(to []byte) bool {
	if bytes.Equal(to, ps.LocalAddr) {
		return true
	}
	return false
}

// if too long topic is sent will return only 0s, should be considered error
func (ps *Pss) MakeTopic(s string) PssTopic {
	t := [TopicLength]byte{}
	if len(s) <= TopicLength {
		copy(t[:len(s)], s)
	}
	ps.topics[t] = s
	return t
}

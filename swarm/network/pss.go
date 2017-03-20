package network

import (
	"bytes"
	
	"github.com/ethereum/go-ethereum/p2p/adapters"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/p2p/protocols"
	"github.com/ethereum/go-ethereum/logger"
	"github.com/ethereum/go-ethereum/logger/glog"
)

type Pss struct {
	Overlay
	LocalAddr []byte
	C         chan []byte
}

// sender
// packaged message
// 

type PssMessenger struct {
	Topic []byte
	Sender []byte
	Recipient []byte
}

func (pm *PssMessenger) SendMsg(code uint64, msg interface{}) error{
	// rlp encode msg
	
	// wrap data 
	
	// send with 
	return nil
}

func (pm *PssMessenger) ReadMsg() (p2p.Msg, error) {
	return p2p.Msg{}, nil
}

func (pm *PssMessenger) Close() {
	return
}

type PssEnvelope struct {
	Topic []byte
	Sender []byte
	Data []byte
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

type PssProtocol struct {
	*Pss
	Name []byte
	Version uint
}

func (pp *PssProtocol) NewProtocol(run func(*protocols.Peer) error, ct *protocols.CodeMap) *p2p.Protocol {

	r := func(p *p2p.Peer, rw p2p.MsgReadWriter) error {

		m := pp.Messenger(rw)

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


func (pp *PssProtocol) Connect(protocol *p2p.Protocol) {
	
}

func (pp *PssProtocol) Messenger(rw p2p.MsgReadWriter) adapters.Messenger {
	prw := rw.(PssReadWriter)
	pm := &PssMessenger{
		Recipient: prw.Recipient,
		Sender: pp.LocalAddr,
		Topic: pp.Name,
	}
	return adapters.Messenger(pm)
}

func NewPss(k Overlay, addr []byte) *Pss {
	return &Pss{
		Overlay:   k,
		LocalAddr: addr,
		C:         make(chan []byte),
	}
}

type PssMsg struct {
	To   []byte
	Data []byte
}

func (pm *PssMsg) String() string {
	return fmt.Sprintf("PssMsg: Recipient: %v", pm.To)
}

func (ps *Pss) HandlePssMsg(msg interface{}) error {
	pssmsg := msg.(*PssMsg)
	to := pssmsg.To
	if bytes.Equal(to, ps.LocalAddr) {
		log.Trace(fmt.Sprintf("Pss to us, yay! %v", to))
		ps.C <- pssmsg.Data
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

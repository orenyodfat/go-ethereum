package network

import (
	"bytes"
	"testing"
	"time"
	"reflect"

	"github.com/ethereum/go-ethereum/logger"
	"github.com/ethereum/go-ethereum/logger/glog"
	//"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/p2p/adapters"
	//"github.com/ethereum/go-ethereum/p2p/simulations"
	"github.com/ethereum/go-ethereum/p2p/protocols"
	p2ptest "github.com/ethereum/go-ethereum/p2p/testing"
)

func init() {
	glog.SetV(logger.Detail)
	glog.SetToStderr(true)
}

type PssTestPayload struct {
	Data string
}

type pssTester struct {
	*p2ptest.ProtocolTester
	ct *protocols.CodeMap
	*PssProtocol
}

type pssProtocolTester struct {
	*pssTester
	//VirtualProtocol *p2p.Protocol
}

//func TestPssProtocolStart(t *testing.T) {
func TestPssVirtualProtocolIO(t *testing.T) {

	//var sdata []byte

	addr := RandomAddr()
	pt := newPssProtocolTester(t, 2, "foo", 42, addr)

	payload := PssEnvelope{
		Topic: MakeTopic("42"),
		TTL:   DefaultTTL,
		Data:  makeFakeMsg(t, pt.pssTester, pt.Ids[1].Bytes()),
	}

	subpeermsgcode, found := pt.ct.GetCode(&SubPeersMsg{})
	if !found {
		t.Fatalf("subpeersMsg not defined")
	}

	pssmsgcode, found := pt.ct.GetCode(&PssMsg{})
	if !found {
		t.Fatalf("PssMsg not defined")
	}

	hs_pivot := correctBzzHandshake(addr)

	for _, id := range pt.Ids {
		hs_sim := correctBzzHandshake(NewPeerAddrFromNodeId(id))
		<-pt.GetPeer(id).Connc
		err := pt.TestExchanges(bzzHandshakeExchange(hs_pivot, hs_sim, id)...)
		if err != nil {
			t.Fatalf("Handshake fail: %v", err)
		}

		err = pt.TestExchanges(
			p2ptest.Exchange{
				Expects: []p2ptest.Expect{
					p2ptest.Expect{
						Code: subpeermsgcode,
						Msg:  &SubPeersMsg{},
						Peer: id,
					},
				},
				/*Triggers: []p2ptest.Trigger{
					p2ptest.Trigger{
						Code: peersmsgcode,
						Msg:  &peersMsg{},
						Peer: id,
					},
				},*/
			},
		)
		if err != nil {
			t.Fatalf("Subpeersmsg to peer %v fail: %v", id, err)
		}
	}

	addr_sim := NewPeerAddrFromNodeId(pt.Ids[1])

	err := pt.TestExchanges(
		p2ptest.Exchange{
			Triggers: []p2ptest.Trigger{
				p2ptest.Trigger{
					Code: pssmsgcode,
					Msg: &PssMsg{
						To:   addr.OverlayAddr(),
						Data: payload,
					},
					Peer: pt.Ids[0],
				},
			},
			Expects: []p2ptest.Expect{
				p2ptest.Expect{
					Code: pssmsgcode,
					Msg:  &PssMsg{
						To:   addr_sim.OverlayAddr(),
						Data: payload,
					},
					Peer: pt.Ids[0],
				},
			},
		},
	)
	if err != nil {
		t.Fatalf("PssMsg sending %v to %v (pivot) fail: %v", pt.Ids[0], addr.OverlayAddr(), err)
	}

	<-pt.C

}

// simple tests of merely receiving and relaying a skeleton pss msg

func TestPssTwoToSelf(t *testing.T) {
	addr := RandomAddr()
	pt := newPssTester(t, 2, addr)

	payload := PssEnvelope{
		Topic: MakeTopic("42"),
		TTL:   DefaultTTL,
		Data:  makeFakeMsg(t, pt, pt.Ids[1].Bytes()),
	}

	subpeermsgcode, found := pt.ct.GetCode(&SubPeersMsg{})
	if !found {
		t.Fatalf("peerMsg not defined")
	}

	pssmsgcode, found := pt.ct.GetCode(&PssMsg{})
	if !found {
		t.Fatalf("PssMsg not defined")
	}

	hs_pivot := correctBzzHandshake(addr)

	for _, id := range pt.Ids {
		hs_sim := correctBzzHandshake(NewPeerAddrFromNodeId(id))
		<-pt.GetPeer(id).Connc
		err := pt.TestExchanges(bzzHandshakeExchange(hs_pivot, hs_sim, id)...)
		if err != nil {
			t.Fatalf("Handshake fail: %v", err)
		}

		err = pt.TestExchanges(
			p2ptest.Exchange{
				Expects: []p2ptest.Expect{
					p2ptest.Expect{
						Code: subpeermsgcode,
						Msg:  &SubPeersMsg{},
						Peer: id,
					},
				},
				/*Triggers: []p2ptest.Trigger{
					p2ptest.Trigger{
						Code: peersmsgcode,
						Msg:  &peersMsg{},
						Peer: id,
					},
				},*/
			},
		)
		if err != nil {
			t.Fatalf("Subpeersmsg to peer %v fail: %v", id, err)
		}
	}

	err := pt.TestExchanges(
		p2ptest.Exchange{
			Triggers: []p2ptest.Trigger{
				p2ptest.Trigger{
					Code: pssmsgcode,
					Msg: &PssMsg{
						To:   addr.OverlayAddr(),
						Data: payload,
					},
					Peer: pt.Ids[0],
				},
			},
		},
	)
	if err != nil {
		t.Fatalf("PssMsg sending %v to %v (pivot) fail: %v", pt.Ids[0], addr.OverlayAddr(), err)
	}

	alarm := time.NewTimer(1000 * time.Millisecond)
	select {
	case data := <-pt.C:
		//p := payload.Data.(pssTestPayload)
		//if !isEqualEnvelope(t, payload, data) {
		if !bytes.Equal(data.(PssEnvelope).Data, payload.Data) {
			t.Fatalf("Data transfer failed, expected: %v, got: %v", payload.Data, data.(PssEnvelope).Data)
		}
		t.Logf("Data transfer success, expected: %v, got: %v", payload.Data, data.(PssEnvelope).Data)
	case <-alarm.C:
		t.Fatalf("Pivot receive of PssMsg from %v timeout", pt.Ids[0])
	}
}

func TestPssTwoRelaySelf(t *testing.T) {
	addr := RandomAddr()
	pt := newPssTester(t, 2, addr)

	subpeermsgcode, found := pt.ct.GetCode(&SubPeersMsg{})
	if !found {
		t.Fatalf("peerMsg not defined")
	}
	/*
		peersmsgcode, found := pt.ct.GetCode(&peersMsg{})
		if !found {
			t.Fatalf("PssMsg not defined")
		}
	*/
	pssmsgcode, found := pt.ct.GetCode(&PssMsg{})
	if !found {
		t.Fatalf("PssMsg not defined")
	}

	payload := PssEnvelope{
		Topic: MakeTopic("42"),
		TTL:   DefaultTTL,
		Data:  makeFakeMsg(t, pt, pt.Ids[1].Bytes()),
	}

	hs_pivot := correctBzzHandshake(addr)

	for _, id := range pt.Ids {
		hs_sim := correctBzzHandshake(NewPeerAddrFromNodeId(id))
		<-pt.GetPeer(id).Connc
		err := pt.TestExchanges(bzzHandshakeExchange(hs_pivot, hs_sim, id)...)
		if err != nil {
			t.Fatalf("Handshake fail: %v", err)
		}

		err = pt.TestExchanges(
			p2ptest.Exchange{
				Expects: []p2ptest.Expect{
					p2ptest.Expect{
						Code: subpeermsgcode,
						Msg:  &SubPeersMsg{},
						Peer: id,
					},
				},
				/*Triggers: []p2ptest.Trigger{
					p2ptest.Trigger{
						Code: peersmsgcode,
						Msg:  &peersMsg{},
						Peer: id,
					},
				},*/
			},
		)
		if err != nil {
			t.Fatalf("Subpeersmsg to peer %v fail: %v", id, err)
		}
	}

	err := pt.TestExchanges(
		p2ptest.Exchange{
			Expects: []p2ptest.Expect{
				p2ptest.Expect{
					Code: pssmsgcode,
					Msg: &PssMsg{
						To:   pt.Ids[0].Bytes(),
						Data: payload,
					},
					Peer: pt.Ids[0],
				},
			},
			Triggers: []p2ptest.Trigger{
				p2ptest.Trigger{
					Code: pssmsgcode,
					Msg: &PssMsg{
						To:   pt.Ids[0].Bytes(),
						Data: payload,
					},
					Peer: pt.Ids[1],
				},
			},
		},
	)
	if err != nil {
		t.Fatalf("PssMsg routing from %v to %v fail: %v", pt.Ids[0], pt.Ids[1], err)
	}
}

// the protocoltester tests ONE SINGLE p2p.protocol to pss protocol mapping
// any handling of parallell p2p.protocols, and the selection of protocol (and thus msghandler batches) via topic and version is out of scope here

func newPssProtocolTester(t *testing.T, n int, topic string, version uint, addr *peerAddr) *pssProtocolTester {

	ps, ct := newPssBaseTester(t, topic, version, n, addr)

	h := NewHive(NewHiveParams(), ps.Overlay)

	run := func(p *PssPeer) error {
		glog.V(logger.Detail).Infof("added and using psspeer: %v", p)
		p.Register(&PssTestPayload{}, p.SimpleHandlePssPayload)
		err := p.Run()
		glog.V(logger.Detail).Infof("psspeer died: %v", err)
		return nil
	}

	pp := NewPssProtocol(ps, topic, version, run, ct)

	srv := func(p Peer) error {
		glog.V(logger.Detail).Infof("Peer %v has protocol map %v", reflect.TypeOf(p))
		p.Register(&PssMsg{}, pp.handlePss)
		h.Add(p)
		p.DisconnectHook(func(err error) {
			h.Remove(p)
		})
		return nil
	}

	protocall := func(na adapters.NodeAdapter) adapters.ProtoCall {
		protocol := Bzz(addr.OverlayAddr(), na, ct, srv, nil, nil)
		return protocol.Run
	}

	ptt := p2ptest.NewProtocolTester(t, NodeId(addr), n, protocall)

	pt := &pssProtocolTester{
		pssTester: &pssTester{
			ProtocolTester: ptt,
			ct:             ct,
			PssProtocol:    pp,
		},
	}

	return pt
}

func newPssTester(t *testing.T, n int, addr *peerAddr) *pssTester {

	topic := "foo"
	version := uint(42)
	ps, ct := newPssBaseTester(t, topic, version, n, addr)

	pp := NewHive(NewHiveParams(), ps.Overlay)

	run := func(p *PssPeer) error {
		return nil
	}

	psp := NewPssProtocol(ps, topic, version, run, ct)

	srv := func(p Peer) error {
		p.Register(&PssMsg{}, ps.SimpleHandlePssMsg)
		pp.Add(p)
		p.DisconnectHook(func(err error) {
			pp.Remove(p)
		})
		return nil
	}

	protocall := func(na adapters.NodeAdapter) adapters.ProtoCall {
		protocol := Bzz(addr.OverlayAddr(), na, ct, srv, nil, nil)
		return protocol.Run
	}

	ptt := p2ptest.NewProtocolTester(t, NodeId(addr), n, protocall)

	return &pssTester{
		ProtocolTester: ptt,
		ct:             ct,
		PssProtocol:    psp,
	}
}

func newPssBaseTester(t *testing.T, topic string, version uint, n int, laddr *peerAddr) (*Pss, *protocols.CodeMap) {

	var lto Overlay
	var lps *Pss
	ct := BzzCodeMap()
	ct.Register(&PssMsg{})
	ct.Register(&peersMsg{})
	ct.Register(&getPeersMsg{})
	ct.Register(&SubPeersMsg{})
	ct.Register(&PssTestPayload{})

	kp := NewKadParams()
	kp.MinProxBinSize = 3
	lto = NewKademlia(laddr.OverlayAddr(), kp)
	lps = NewPss(lto, laddr.OverlayAddr())

	return lps, ct
}

func makeFakeMsg(t *testing.T, pt *pssTester, sender []byte) []byte {
	
	code, found := pt.ct.GetCode(&PssTestPayload{})
	if !found {
		t.Fatalf("pssTestPayload type not registered")
	}

	data := PssTestPayload{
		Data: "Bar",
	}
	
	rlpbundle, err := MakePss(code, sender, data)
	if err != nil {
		return nil
	}
/*
	rlpdata, err := rlp.EncodeToBytes(data)
	if err != nil {
		t.Fatalf("rlp encoding of data fail: %v", err)
	}

	smsg := &pssPayload{
		Code:   code,
		Size:   uint32(len(rlpdata)),
		Data:   rlpdata,
		Sender: sender,
	}

	rlpbundle, err := rlp.EncodeToBytes(smsg)
	if err != nil {
		t.Fatalf("rlp encoding of msg fail: %v", err)
	}
*/
	return rlpbundle
}

// poc for just passing empty pssmsg shells
func (pp *Pss) SimpleHandlePssMsg(msg interface{}) error {
	pssmsg := msg.(*PssMsg)
	to := pssmsg.To
	env := pssmsg.Data
	if pp.isSelfRecipient(to) {
		glog.V(logger.Detail).Infof("Pss to us, yay!", to)
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

// poc for handling incoming unpacked message
func (p *PssPeer) SimpleHandlePssPayload(msg interface{}) error {
	pmsg := msg.(*PssTestPayload)
	glog.V(logger.Detail).Infof("PssTestPayloadhandler got message %v", pmsg)
	p.Send(pmsg)
	return nil
}

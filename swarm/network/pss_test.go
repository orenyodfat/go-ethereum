package network

import (
	"testing"
	"time"
	"bytes"
	//"reflect"
	
	"github.com/ethereum/go-ethereum/logger"
	"github.com/ethereum/go-ethereum/logger/glog"
	//"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/p2p/adapters"
	//"github.com/ethereum/go-ethereum/p2p/simulations"
	"github.com/ethereum/go-ethereum/p2p/protocols"
	p2ptest "github.com/ethereum/go-ethereum/p2p/testing"
	"github.com/ethereum/go-ethereum/rlp"
)


func init() {
	glog.SetV(logger.Detail)
	glog.SetToStderr(true)
}

type pssPayload struct {
	Code uint64
	Size uint32
	Data []byte
	ReceivedAt time.Time
}

type pssTestPayload struct {
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

func TestPssProtocolStart(t *testing.T) {
	
	//var sdata []byte
	
	addr := RandomAddr()
	pt := newPssProtocolTester(t, 2, "foo", 42, addr)
	glog.V(logger.Detail).Infof("made protocoltester %v", pt.Name)
	
	payload := PssEnvelope{
		Topic: pt.MakeTopic("42"),
		TTL: DefaultTTL,
		Data: makeFakeMsg(t, pt.ct),
	}

	rw := PssReadWriter{
		Recipient: pt.Ids[len(pt.Ids) - 1].Bytes(),
	}
	m := pt.Messenger(rw)
	glog.V(logger.Detail).Infof("made messenger %v", m)
	
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
	
	err := pt.TestExchanges (
		p2ptest.Exchange{
			Triggers: []p2ptest.Trigger{
				p2ptest.Trigger{
					Code: pssmsgcode,
					Msg:  &PssMsg{
						To: addr.OverlayAddr(),
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
	
}

// simple tests of merely receiving and relaying a skeleton pss msg

func TestPssTwoToSelf(t *testing.T) {
	addr := RandomAddr()
	pt := newPssTester(t, 2, addr)
	
	payload := PssEnvelope{
		Topic: pt.MakeTopic("42"),
		TTL: DefaultTTL,
		Data: makeFakeMsg(t, pt.ct),
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
	
	err := pt.TestExchanges (
		p2ptest.Exchange{
			Triggers: []p2ptest.Trigger{
				p2ptest.Trigger{
					Code: pssmsgcode,
					Msg:  &PssMsg{
						To: addr.OverlayAddr(),
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
		Topic: pt.MakeTopic("42"),
		TTL: DefaultTTL,
		Data: makeFakeMsg(t, pt.ct),
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
	
	err := pt.TestExchanges (
		p2ptest.Exchange{
			Expects: []p2ptest.Expect{
				p2ptest.Expect{
					Code: pssmsgcode,
					Msg:  &PssMsg{
						To: pt.Ids[0].Bytes(),
						Data: payload,
					},
					Peer: pt.Ids[0],
				},
			},
			Triggers: []p2ptest.Trigger{
				p2ptest.Trigger{
					Code: pssmsgcode,
					Msg:  &PssMsg{
						To: pt.Ids[0].Bytes(),
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


func newPssProtocolTester(t *testing.T, n int, topic string, version uint, addr *peerAddr) *pssProtocolTester {
	
	psp, to, ct := newPssBaseTester(t, n, addr)
	
	pp := NewHive(NewHiveParams(), to)

	srv := func(p Peer) error {
		p.Register(&PssMsg{}, psp.HandlePssMsg)
		psp.setPeer(p)
		psp.ct = ct
		pp.Add(p)
		p.DisconnectHook(func(err error) {
			pp.Remove(p)
		})
		return nil
	}
	
	protocall := func(na adapters.NodeAdapter) adapters.ProtoCall {
		psp.VirtualProtocol = Bzz(addr.OverlayAddr(), na, ct, srv, nil, nil)
		return psp.VirtualProtocol.Run
	}

	ptt := p2ptest.NewProtocolTester(t, NodeId(addr), n, protocall)
	
	/*run := func(p *protocols.Peer) error {
		glog.V(logger.Detail).Infof("inside virtual run with peer %v", p)
		return nil
	}*/
	
	pt := &pssProtocolTester{
		pssTester: &pssTester{
			ProtocolTester: ptt,
			ct: ct,
			PssProtocol: psp,
		},
	}
	
	//vct := protocols.NewCodeMap(topic, version, 65535, nil)
	//pt.VirtualProtocol = pt.NewProtocol(run, vct)
	pt.Name = topic
	pt.Version = version
		
	return pt
}


func newPssTester(t *testing.T, n int, addr *peerAddr) *pssTester {
	psp, to, ct := newPssBaseTester(t, n, addr)
	
	pp := NewHive(NewHiveParams(), to)

	/*net := simulations.NewNetwork(&simulations.NetworkConfig{})
	naf := func(conf *simulations.NodeConfig) adapters.NodeAdapter {
		na := adapters.NewSimNode(conf.Id, net, simPipe)
		return na
	}
	net.SetNaf(naf)*/
	
	srv := func(p Peer) error {
		p.Register(&PssMsg{}, psp.SimpleHandlePssMsg)
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
	
	return &pssTester {
		ProtocolTester: ptt,
		ct: ct,
		PssProtocol: psp,
	}
}

func newPssBaseTester(t *testing.T, n int, laddr *peerAddr) (*PssProtocol, Overlay, *protocols.CodeMap) {
	
	var lto Overlay
	var lps *Pss
	ct := BzzCodeMap()
	ct.Register(&PssMsg{})
	ct.Register(&peersMsg{})
	ct.Register(&getPeersMsg{})
	ct.Register(&SubPeersMsg{})
	ct.Register(&pssTestPayload{}) 
	
	//simPipe := adapters.NewSimPipe
	
	kp := NewKadParams()
	kp.MinProxBinSize = 3
	lto = NewKademlia(laddr.OverlayAddr(), kp)
	lps = NewPss(lto, laddr.OverlayAddr())
	lpsp := &PssProtocol{
		Pss: lps,
	}

	return lpsp, lto, ct
}

// poc for just passing empty pssmsg shells
func (pp *PssProtocol) SimpleHandlePssMsg(msg interface{}) error {
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
/*
func TestMakePssPeer(t *testing.T) {
	
	name := "foo"
	version := uint(42)
	
	id := adapters.RandomNodeId()
	addr := NewPeerAddrFromNodeId(id)
	
	pt, _, ct := newPssBaseTester(t, 0, addr)
	
	p := p2p.NewPeer(id.NodeID, "foo", []p2p.Cap{
			p2p.Cap {
				Name: name,
				Version: version,
			},
		},
	)
	
	rw := PssReadWriter{
		Recipient: addr.UnderlayAddr(),
	}
	
	m := pt.Messenger(rw)
	
	pp := protocols.NewPeer(p, ct, m)
	
	glog.V(logger.Detail).Infof("made psspeer %v\n\tid: %v\n\toaddr: %x,\n\tuaddr: %x", pp, id, addr.OverlayAddr(), addr.UnderlayAddr())
}
*/

func makeFakeMsg(t *testing.T, ct *protocols.CodeMap) []byte {
	
	code, found := ct.GetCode(&pssTestPayload{})
	if !found {
		t.Fatalf("pssTestPayload type not registered")
	}
	
	data := pssTestPayload{
		Data: "Bar",
	}
	
	rlpdata,err := rlp.EncodeToBytes(data)
	if err != nil {
		t.Fatalf("rlp encoding of data fail: %v", err)
	}
	
	smsg := &pssPayload{
		Code: code,
		Size: uint32(len(rlpdata)),
		Data: rlpdata,
	}
	
	rlpbundle, err := rlp.EncodeToBytes(smsg)
	if err != nil {
		t.Fatalf("rlp encoding of msg fail: %v", err)
	}
	
	return rlpbundle
}

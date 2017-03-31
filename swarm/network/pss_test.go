package network

import (
	"testing"
	"time"
	"bytes"
	
	"github.com/ethereum/go-ethereum/logger"
	"github.com/ethereum/go-ethereum/logger/glog"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/p2p/adapters"
	"github.com/ethereum/go-ethereum/p2p/simulations"
	"github.com/ethereum/go-ethereum/p2p/protocols"
	p2ptest "github.com/ethereum/go-ethereum/p2p/testing"
)


func init() {
	glog.SetV(logger.Detail)
	glog.SetToStderr(true)
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
	VirtualProtocol *p2p.Protocol
}

func TestPssProtocolStart(t *testing.T) {
	addr := RandomAddr()
	pt := newPssProtocolTester(t, addr, 3, []byte("foo"), 42)
	glog.V(logger.Detail).Infof("made protocoltester %v", pt.Name)
	
	rw := PssReadWriter{
		Recipient: pt.Ids[2].Bytes(),
	}
	m := pt.Messenger(rw)
	glog.V(logger.Detail).Infof("made messenger %v", m)
}

func newPssProtocolTester(t *testing.T, addr *peerAddr, n int, topic []byte, version uint) *pssProtocolTester {
	bt := newPssBaseTester(t, addr, n)
	bt.Name = topic
	bt.Version = version
	
	pt := &pssProtocolTester{
		pssTester: bt,
	}
	pt.ct = protocols.NewCodeMap(string(topic), version, 65535, nil)
	
	run := func(p *protocols.Peer) error {
		glog.V(logger.Detail).Infof("inside run with peer %v", p)
		return nil
	}
	pt.VirtualProtocol = pt.NewProtocol(run, pt.ct)
	
	return pt
}

// simple tests of merely receiving and relaying a skeleton pss msg

func TestPssTwoToSelf(t *testing.T) {
	addr := RandomAddr()
	pt := newPssTester(t, addr, 2)
	data := pssTestPayload{Data: "foo"}
	
	payload := PssEnvelope{
		Topic: makeTopic([]byte("42")),
		TTL: DefaultTTL,
		Data: tmpSerialize(data),
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
			if !bytes.Equal(data.Data, payload.Data) {
				t.Fatalf("Data transfer failed, expected: %v, got: %v", payload.Data, data.Data)
			}
			t.Logf("Data transfer success, expected: %v, got: %v", payload.Data, data.Data)
		case <-alarm.C:
			t.Fatalf("Pivot receive of PssMsg from %v timeout", pt.Ids[0])
	}
}


func TestPssTwoRelaySelf(t *testing.T) {
	addr := RandomAddr()
	pt := newPssTester(t, addr, 2)
	
	
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
	
	data := pssTestPayload{Data: "foo"}
	payload := PssEnvelope{
		Topic: makeTopic([]byte("42")),
		TTL: DefaultTTL,
		Data: tmpSerialize(data),
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

func newPssTester(t *testing.T, addr *peerAddr, n int) *pssTester {
	pt := newPssBaseTester(t, addr, n)
	return pt
}

func newPssBaseTester(t *testing.T, addr *peerAddr, n int) *pssTester {
	ct := BzzCodeMap()
	ct.Register(&PssMsg{})
	ct.Register(&peersMsg{})
	ct.Register(&getPeersMsg{})
	ct.Register(&SubPeersMsg{}) // why is this public? 
	
	
	simPipe := adapters.NewSimPipe
	kp := NewKadParams()
	kp.MinProxBinSize = 3
	to := NewKademlia(addr.OverlayAddr(), kp)
	pp := NewHive(NewHiveParams(), to)
	ps := NewPss(to, addr.OverlayAddr())
	net := simulations.NewNetwork(&simulations.NetworkConfig{})
	naf := func(conf *simulations.NodeConfig) adapters.NodeAdapter {
		na := adapters.NewSimNode(conf.Id, net, simPipe)
		return na
	}
	net.SetNaf(naf)

	srv := func(p Peer) error {
		p.Register(&PssMsg{}, ps.HandlePssMsg)
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
	
	psp := &PssProtocol{
		Pss: ps,
	}
	
	return &pssTester {
		ProtocolTester: ptt,
		ct: ct,
		PssProtocol: psp,
	}
}

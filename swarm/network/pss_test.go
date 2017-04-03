package network

import (
	"testing"
	"time"
	"bytes"
	//"reflect"
	
	"github.com/ethereum/go-ethereum/logger"
	"github.com/ethereum/go-ethereum/logger/glog"
	"github.com/ethereum/go-ethereum/p2p"
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

	/*rw := PssReadWriter{
		Recipient: pt.Ids[len(pt.Ids) - 1].Bytes(),
	}
	m := pt.Messenger(rw)
	glog.V(logger.Detail).Infof("made messenger %v", m)
	*/
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
	
	time.Sleep(time.Second * 1)
	
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

	// now the pssprotocol Insert function designation is overwritten on every new peer connect
	// we need a new pssprotocol for every peer connect, too
	// or we need to use it as a dispatcher with a pool of rws/messengers to match the peer
	// problem is, that we can't tell the peer from inside the msg handler function
	// thus we need to instantiate and maintain (pss)peers with m/rws for every new pssmsg that comes in, with the "sender" field as the nodeid
	// question again (and again and again) is how to we then reach the right messenger/rw to pass messages in to and out from this peer
	
	srv := func(p Peer) error {
		p.Register(&PssMsg{}, psp.HandlePssMsg)
		psp.ct = ct
		pp.Add(p)
		p.DisconnectHook(func(err error) {
			pp.Remove(p)
		})
		return nil
	}
	
	// I guess this is where the handler registering goes
	// still to figure out is how to tap into and register
	// the batches of handlers that are already registered on the existing p2p protocols
	// 
	run := func(p *protocols.Peer) error {
		p.Register(&PssTestPayload{}, psp.SimpleHandlePssPayload)
		glog.V(logger.Detail).Infof("inside virtual run with peer %v", p)
		err := p.Run()
		glog.V(logger.Detail).Infof("virtual run peer died: %v", err)
		return nil
	}
	
	psp.VirtualProtocol = psp.NewProtocol(run, ct)
	psp.Name = topic
	psp.Version = version
	
	protocall := func(na adapters.NodeAdapter) adapters.ProtoCall {
		protocol := Bzz(addr.OverlayAddr(), na, ct, srv, nil, nil)
		return func (p *p2p.Peer, rw p2p.MsgReadWriter) error {
			prw := PssReadWriter{
				rw: make(chan p2p.Msg),
			}
			go psp.VirtualProtocol.Run(p, prw)
			protocol.Run(p, rw)
			return nil
		}
	}

	ptt := p2ptest.NewProtocolTester(t, NodeId(addr), n, protocall)
	
	pt := &pssProtocolTester{
		pssTester: &pssTester{
			ProtocolTester: ptt,
			ct: ct,
			PssProtocol: psp,
		},
	}
	
	return pt
}


// sets up hive

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

// laddr (peerAddr) contains both underlying nodeid and the swarm overlay address of the PIVOT NODE

// sets up codemap
// sets up kademlia routing
// sets up pss base object with;
// * kademlia (for message forwarding)
// * the swarm overlay addr: keccak256 hash of the discovery.nodeid, byte slice) 
// instantiates the pssProtocol, which should be created one to one for every protocol wished to offer over pss
// * dispatching of incoming messages
// * messenger instantiation for 

// the protocol is identified over pss as a topic and version pair, analogous to 
// however this topic and version pair should be obfuscated, and a topic and version alias could be established through a handshake phase
// ("handshake" being a topic in itself)
// where the protocol name/version is asked for by the local peer in a public key encrypted message payload, and a topic/version is given by the pss-peer in return
// through an active session the remote peer will remember this pairing, until
// * termination notification is received by the pss-peer
// * is flushed from the topic lookup table by new requests obsoleting the current one

// so far, in this simple test case, the pssProtocol is only created for an imagined extended bzz protocol scenario
// these should however be two protocols running, one for the pss/bzz, and one separate for the actual protocol whose implementation is desired

func newPssBaseTester(t *testing.T, n int, laddr *peerAddr) (*PssProtocol, Overlay, *protocols.CodeMap) {
	
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
	lpsp := &PssProtocol{
		Pss: lps,
	}

	return lpsp, lto, ct
}


func makeFakeMsg(t *testing.T, ct *protocols.CodeMap) []byte {
	
	code, found := ct.GetCode(&PssTestPayload{})
	if !found {
		t.Fatalf("pssTestPayload type not registered")
	}
	
	data := PssTestPayload{
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

// hands-on exercise to clear up the relationship with messenger, rw,
// how to create and start a stripped-down protocol manually, and how to invoke the peer incoming handle loop

// see above for description of what the base tester sets up

// currently we're using write directly to the rw as a hack

func TestMakePssPeer(t *testing.T) {
	
	topic := "foo"
	version := uint(42)
	
	id := adapters.RandomNodeId()
	addr := NewPeerAddrFromNodeId(id)
		
	pp, _, _ := newPssBaseTester(t, 1, addr)
	
	// make a fake peer to run the protocol on
	
	pid := adapters.RandomNodeId()
	p := p2p.NewPeer(pid.NodeID, "foo", []p2p.Cap{
			p2p.Cap {
				Name: topic,
				Version: version,
			},
		},
	)
	
	// the rw will secure two-way writing
	rw := PssReadWriter{
		Recipient: addr.UnderlayAddr(),
		rw: make(chan p2p.Msg),
	}
	
	run := func(p *protocols.Peer) error {
		glog.V(logger.Detail).Infof("inside virtual run with peer %v", p)
		go p.Run()
		return nil
	}
	vct := protocols.NewCodeMap(topic, version, 65535, nil)
	vct.Register(&PssTestPayload{}) 
	
	pp.VirtualProtocol = pp.NewProtocol(run, vct)

	pp.VirtualProtocol.Run(p, rw)
	
	// fake a message send to the pipe
	bmsg := makeFakeMsg(t, vct)
	b := bytes.NewBuffer(bmsg)
	glog.V(logger.Detail).Infof("protocoltester: %v", pp)
		
	msg := p2p.Msg{
		Code: uint64(pp.VirtualProtocol.Length - 1),
		Size: uint32(len(bmsg)),
		Payload: b,
		ReceivedAt: time.Now(),
	}


	rw.rw <- msg
	close(rw.rw)
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

// poc for handling incoming unpacked message
func (pp *PssProtocol) SimpleHandlePssPayload(msg interface{}) error {
	pmsg := msg.(*PssTestPayload)
	glog.V(logger.Detail).Infof("PssTestPayloadhandler got message %v", pmsg)
	return nil
}

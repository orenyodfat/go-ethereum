package network

import (
	"fmt"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/logger"
	"github.com/ethereum/go-ethereum/logger/glog"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/p2p/adapters"
	"github.com/ethereum/go-ethereum/p2p/protocols"
	p2ptest "github.com/ethereum/go-ethereum/p2p/testing"
)

func init() {
	glog.SetV(logger.Detail)
	glog.SetToStderr(true)
}

// example protocol implementation peer
// message handlers are methods of this
// goal is that we can use the same for "normal" p2p.protocols operations aswell as pss
type PssTestPeer struct {
	*protocols.Peer
}

// the content of the msgs we're sending in the tests
type PssTestPayload struct {
	Data string
}

// all we need for the testing
// TODO: these might be leaner?
type pssTester struct {
	*p2ptest.ProtocolTester
	ct  *protocols.CodeMap
	vct *protocols.CodeMap
	*PssProtocol
}

type pssProtocolTester struct {
	*pssTester
}

func TestPssRegisterHandler(t *testing.T) {
	var err error
	addr := RandomAddr()
	ps := makePss(addr)

	err = ps.Register("foo", 42, func(msg []byte, p *p2p.Peer, sender []byte) error { return nil })
	if err != nil {
		t.Fatalf("couldnt register protocol 'foo' v 42: %v", err)
	}
	err = ps.Register("abcdefghiljklmnopqrstuvxyz0123456789", 65536, func(msg []byte, p *p2p.Peer, sender []byte) error { return nil })
	if err == nil {
		t.Fatalf("register protocol 'abc..xyz' v 65536 should have failed")
	}
}

func TestPssAddSingleHandler(t *testing.T) { 
	//var err error
	name := "foo"
	version := 42
	
	addr := RandomAddr()

	ps := newPssBase(t, name, version, addr)
	vct := protocols.NewCodeMap(name, uint(version), 65535, &PssTestPayload{})
	
	// topic will be the mapping in pss used to dispatch to the proper handler
	// the dispatcher is protocol agnostic
	topic, _ := ps.MakeTopic(name, version)
	
	// this is the protocols.Protocol that we want to be made accessible through Pss
	// set up the protocol mapping to pss, and register it for this topic
	// this is an optional step, we are not forcing to use protocols in the handling of pss, it might be anything
	targetprotocol := makeCustomProtocol(name, version, vct, ps.NodeId)
	pssprotocol := NewPssProtocol(ps, &topic, vct, targetprotocol)
	ps.Register(name, version, pssprotocol.GetHandler())
	
	handlefunc := makePssHandleForward(ps)
	
	newPssTester(t, ps, addr, 0, handlefunc)
}

func TestPssSimpleRelay(t *testing.T) {
	//var err error
	name := "foo"
	version := 42
	
	addr := RandomAddr()

	ps := newPssBase(t, name, version, addr)
	vct := protocols.NewCodeMap(name, uint(version), 65535, &PssTestPayload{})
	
	// topic will be the mapping in pss used to dispatch to the proper handler
	// the dispatcher is protocol agnostic
	topic, _ := ps.MakeTopic(name, version)
	
	// this is the protocols.Protocol that we want to be made accessible through Pss
	// set up the protocol mapping to pss, and register it for this topic
	// this is an optional step, we are not forcing to use protocols in the handling of pss, it might be anything
	targetprotocol := makeCustomProtocol(name, version, vct, ps.NodeId)
	pssprotocol := NewPssProtocol(ps, &topic, vct, targetprotocol)
	ps.Register(name, version, pssprotocol.GetHandler())
	
	handlefunc := makePssHandleForward(ps)
	
	pt, ct := newPssTester(t, ps, addr, 2, handlefunc)
	
	// pss msg we will send
	pssdata := makeFakeMsg(ps, vct, "Bar")
	pssenv := PssEnvelope{
		Topic: topic,
		TTL:   DefaultTTL,
		Payload:  pssdata,
	}
	pssmsg := PssMsg{
		To: addr.OverlayAddr(),
		Payload: pssenv,
	}

	peersmsgcode, found := ct.GetCode(&peersMsg{})
	if !found {
		t.Fatalf("peersMsg not defined")
	}
	
	subpeersmsgcode, found := ct.GetCode(&subPeersMsg{})
	if !found {
		t.Fatalf("subpeersMsg not defined")
	}

	pssmsgcode, found := ct.GetCode(&PssMsg{})
	if !found {
		t.Fatalf("PssMsg not defined")
	}

	//addr_sim := NewPeerAddrFromNodeId(pt.Ids[1])
	
	t.Logf("made pssdata %v pssenv %v pssmsg %v", pssdata, pssenv, pssmsg)

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
						Code: subpeersmsgcode,
						Msg:  &subPeersMsg{},
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
	
	for _, id := range pt.Ids {
		err := pt.TestExchanges(
			p2ptest.Exchange{
				Expects: []p2ptest.Expect{
					p2ptest.Expect{
						Code: peersmsgcode,
						Msg:  &peersMsg{},
						Peer: id,
					},
				},
			},
		)
		if err != nil {
		//	t.Fatalf("peersMsg to peer %v fail: %v", id, err)
		}
	}

	err := pt.TestExchanges(
		p2ptest.Exchange{
			Triggers: []p2ptest.Trigger{
				p2ptest.Trigger{
					Code: pssmsgcode,
					Msg: pssmsg,
					Peer: pt.Ids[0],
				},
			},

			Expects: []p2ptest.Expect{
				p2ptest.Expect{
					Code: pssmsgcode,
					Msg: pssmsg,
					Peer:    pt.Ids[0],
					Timeout: time.Second * 2,
				},
			},
		},
	)

	if err != nil {
		t.Fatalf("PssMsg sending %v to %v (pivot) fail: %v", pt.Ids[0], addr.OverlayAddr(), err)
	}
}


// set up testing for passing of pss messages through the complete stack
// - messages come in on the testing.ProtocolTester, which uses the normal Bzz() protocol setup
// - pssmsg types received by testing.ProtocolTester get passed to PssProtocol.handlePss
// - PssProtocol.handlePss starts PssProtocol.VirtualProtocol with the decoded sender address as peer
// - PssProtocol.VirtualProtocol makes a VirtualPeer, which is a p2p.Peer that allows us to hack into the rw on p2p.Server level, and pass messages to it
// - PssProtocol.VirtualProtocol runs the run-function passed from THIS TESTER, which:
//   * hacks a simnode, whose Messenger bridges the VirtualPeer's MsgReadWriter to ProtoRW.ReadMsg() that handles subprotocols
//   * Instantiates the actual virtual protocol, implanting the aforementioned Messenger: Result: we can now get messages BACK from the virtual protocol msg handlers, too
//   * Links the virtual protocol with the VirtualPeer's readwriter (manually runs p2p.Peer.matchProtocol(). Result: VirtualPeer's ReadMsg can now reach the subprotocols
//   * starts the VirtualPeer (sits on p2p.ReadMsg receiving injected messages from PssProtocol.handlePss)
//   * in turn starts a protocols.Peer, which has the subprotocol handler
// - Pss.Protocol.handlePss "injects" the message in the VirtualPeer's hacked RW, and wanders thus:
//   1. comes in on p2p.ReadMsg() (on PssReadWriter from VirtualPeer)
//   2. passed to ProtoRW.in from VirtualPeer.running["topic"] in p2p.handle()
//   3. comes out on the PssMessenger on the protocols.Peer, in handleIncoming()
//   4. passed to the handler, which in this case just echoes it back by sending using protocols.Peer.Send()
//   5. passed to hacked PssMessenger's SendMsg on the protocols.Peer
//   6. passed to Pss.Send(), which iterates its kademlia and attempts to forward

/*
func newPssProtocolTester(t *testing.T, n int, topic string, version uint, addr *peerAddr) *pssProtocolTester {

	// contains the handler
	var pssprotocol *PssProtocol
	var vprotocol *p2p.Protocol

	ps, ct := newPssBaseTester(t, topic, version,addr)

	vct := protocols.NewCodeMap(topic, version, 65535, &PssTestPayload{})

	h := NewHive(NewHiveParams(), ps.Overlay)

	// handles incoming pssmsg structs
	srv := func(p Peer) error {
		p.Register(&PssMsg{}, pssprotocol.handlePss)
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

	run := func(vp *p2p.VirtualPeer) error {

		// This is the big, nasty hack workarond:
		// we do a detour to make a messenger with the ProtoRW from the virtualpeer's running protocols, whose name matches the "pss topic"
		// thus we enable the p2p.Peer:handle() method to send to the ProtoRW.in channel, and we can use original protocol handler functions as-is

		// tried to use adapters.SimPipe, but it calls p2p.Send (from p2p/message.go) explicitly, and rlpencodes the message
		// which means we would have to decode it and encode it again to get at the message itself
		// and for that we would also have to know what message structure it originally was, which the subsequent call to MsgReadWriter.WriteMsg() wouldn't reveal...
		// ... without looking up the code in a codemap lookup, for which we whould have had to store the codemap there too, etc. etc.
		m := &PssMessenger{
			RecipientUAddr: adapters.NodeId{
				NodeID: vp.ID(),
			},
			send:       ps.Send,
			peerLookup: ps.GetPeerFromNodeId,
		}
		nastysimnodehack := adapters.NewSimNode(ps.NodeId, nil, func(rw p2p.MsgReadWriter) adapters.Messenger {
			m.rw, _ = vp.GetProtoReadWriter(topic)
			return m
		})

		vrun := func(p *protocols.Peer) error {
			glog.V(logger.Detail).Infof("running vprotocol: %v", p)
			// this struct is for returning messages through writemsg only
			// p2p/protocols.Send -> adapters.Messenger.SendMsg -> p2p.Send -> rw.WriteMsg()
			// we need the rw to be the PssReadWriter, which will then pass it on to Pss.Send()
			ptp := &PssTestPeer{
				Peer: p,
			}
			p.Register(&PssTestPayload{}, ptp.SimpleHandlePssPayload)
			err := p.Run()
			return err
		}

		vprotocol = protocols.NewProtocol(topic, version, vrun, nastysimnodehack, vct, nil, nil)

		vp.LinkProtocols([]p2p.Protocol{*vprotocol})
		err := vp.Run()

		glog.V(logger.Warn).Infof("virtualprotocol peer died: %v", err)
		return nil
	}

	pssprotocol = NewOldPssProtocol(ps, topic, version, run, ct)

	pt := &pssProtocolTester{
		pssTester: &pssTester{
			ProtocolTester: ptt,
			ct:             ct,
			vct:            vct,
			PssProtocol:    pssprotocol,
		},
	}

	return pt
}
*/

func newPssTester(t *testing.T, ps *Pss, addr *peerAddr, numsimnodes int, handlefunc func(interface{}) error) (*p2ptest.ProtocolTester, *protocols.CodeMap) {
	
	ct := BzzCodeMap()
	ct.Register(&peersMsg{})
	ct.Register(&getPeersMsg{})
	ct.Register(&subPeersMsg{})
	ct.Register(&PssMsg{})
	
	// set up the outer protocol
	h := NewHive(NewHiveParams(), ps.Overlay)

	srv := func(p Peer) error {
		p.Register(&PssMsg{}, handlefunc)
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

	ptt := p2ptest.NewProtocolTester(t, NodeId(addr), numsimnodes, protocall)
	return ptt, ct
}

func newPssBase(t *testing.T, topic string, version int, addr *peerAddr) *Pss {
	return makePss(addr)
}

func makePss(addr *peerAddr) *Pss {
	nid := adapters.NewNodeId(addr.UnderlayAddr())

	kp := NewKadParams()
	kp.MinProxBinSize = 3

	overlay := NewKademlia(addr.UnderlayAddr(), kp)
	ps := NewPss(overlay, nid)
	return ps
}


func makeCustomProtocol(name string, version int, ct *protocols.CodeMap, id *adapters.NodeId) *p2p.Protocol {
	// creating a protocols.Protocol means using a protocols.Peer
	// so we need codemap, messenger, nodeadapter
	na := adapters.NewSimNode(id, nil, func (rw p2p.MsgReadWriter) adapters.Messenger {
		return adapters.NewSimPipe(rw)
	})

	run := func(p *protocols.Peer) error {
		glog.V(logger.Detail).Infof("running vprotocol: %v", p)
		ptp := &PssTestPeer{ // analogous to bzzPeer in the Bzz() protocol constructor
			Peer: p,
		}
		p.Register(&PssTestPayload{}, ptp.SimpleHandlePssPayload)
		err := p.Run()
		return err
	}

	return protocols.NewProtocol(name, uint(version), run, na, ct, nil, nil)
}

// does exactly what it says
func makeFakeMsg(ps *Pss, ct *protocols.CodeMap, content string) []byte {
	data := PssTestPayload{
	}
	code, found := ct.GetCode(data)
	if !found {
		return []byte{}
	}

	rlpbundle, err := ps.MakeMsg(code, data)
	if err != nil {
		return nil
	}

	return rlpbundle
}

func makePssHandleForward(ps *Pss) func(msg interface{}) error {
	// for the simple check it passes on the message if it's not for us
	return func(msg interface{}) error {
		pssmsg := msg.(*PssMsg)
		
		if ps.isSelfRecipient(*pssmsg) {
			glog.V(logger.Debug).Infof("pss for us .. yay!")
		} else {
			to := ps.GetMsgRecipient(*pssmsg)
			ps.Overlay.EachLivePeer(to, 255, func(p Peer, po int) bool {
				err := p.Send(pssmsg)

				if err != nil {
					return true
				}
				return false
			})
		}
		return nil
	}
}

func makePssHandleProtocol(ps *Pss) func(msg interface{}) error {
	return func(msg interface{}) error {
		pssmsg := msg.(PssMsg)
		
		if ps.isSelfRecipient(pssmsg) {
			env := pssmsg.Payload
			umsg := env.Payload // this will be rlp encrypted
			f := ps.handlers[env.Topic]
			if f == nil {
				return fmt.Errorf("No registered handler for topic '%s'", env.Topic)
			}
			nid := adapters.NewNodeId(env.SenderUAddr)
			p := p2p.NewPeer(nid.NodeID, "psspeer", []p2p.Cap{})
			return f(umsg, p, env.SenderOAddr)
		} else {
			to := ps.GetMsgRecipient(pssmsg)
			ps.Overlay.EachLivePeer(to, 255, func(p Peer, po int) bool {
				err := p.Send(pssmsg)

				if err != nil {
					return true
				}
				return false
			})
		}
		return nil
	}
}


// echoes an incoming message
// it comes in through
// Any pointer receiver that has protocols.Peer
func (ptp *PssTestPeer) SimpleHandlePssPayload(msg interface{}) error {
	pmsg := msg.(*PssTestPayload)
	glog.V(logger.Detail).Infof("PssTestPayloadhandler got message %v", pmsg)
	ptp.Send(pmsg)
	return nil
}

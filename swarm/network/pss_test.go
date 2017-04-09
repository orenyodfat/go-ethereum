package network

import (
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

// Test: simnode sends "forwarded" pssmsg to pivotnode, pivotnode echoes back by relaying to closest of the (2) connected (sim)peers to the sender it was "forwarded" from
// see detailed procedure descriptions on the newPssProtocolTester below

// the returned message doesn't current quite match; deviate within the first 32 bytes.'
// probably has to do with the address; we should check only the parts of the msg we expect to be the same
// also concurrency has the expect time out sometimes before the Pss.Send() reply completes

func TestPssRoundtrip(t *testing.T) {

	addr := RandomAddr()
	pt := newPssProtocolTester(t, 2, "foo", 42, addr)

	msg := makeFakeMsg(t, pt.pssTester, pt.Ids[1].Bytes())

	// attempted to build on the p2p.Protocol returned by Bzz()
	// but couldn't get past the handshake
	// so split up in two protocols instead

	//msg, _ := pt.MakeMsg(0, &bzzHandshake{0, 322, addr})

	payload := PssEnvelope{
		Topic: MakeTopic("42"),
		TTL:   DefaultTTL,
		Data:  msg,
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
					Msg: &PssMsg{
						To:   addr_sim.OverlayAddr(),
						Data: payload,
					},
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
//   6. passed to hacked PssMessenger's SendMsg on the protocols.Peer
//   7. passed to Pss.Send(), which iterates its kademlia and attempts to forward

func newPssProtocolTester(t *testing.T, n int, topic string, version uint, addr *peerAddr) *pssProtocolTester {

	// contains the handler
	var pssprotocol *PssProtocol
	var vprotocol *p2p.Protocol

	ps, ct, vct := newPssBaseTester(t, topic, version, n, addr)

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

	pssprotocol = NewPssProtocol(ps, topic, version, run, ct)

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

// Set up overlay routing and the pss resolver
// we return two code maps
// 1) codemap for the "real" protocol, handling network i/o
// 2) codemap for the "virtual" protocol, handling the msg payload inside the pssmsgs

func newPssBaseTester(t *testing.T, topic string, version uint, n int, laddr *peerAddr) (*Pss, *protocols.CodeMap, *protocols.CodeMap) {

	var lto Overlay
	var lps *Pss

	vct := protocols.NewCodeMap(topic, version, 65535, &PssTestPayload{})

	ct := BzzCodeMap()
	ct.Register(&PssMsg{})
	ct.Register(&peersMsg{})
	ct.Register(&getPeersMsg{})
	ct.Register(&SubPeersMsg{})
	ct.Register(&PssTestPayload{})

	kp := NewKadParams()
	kp.MinProxBinSize = 3

	nid := adapters.NewNodeId(laddr.UnderlayAddr())

	lto = NewKademlia(laddr.UnderlayAddr(), kp)
	lps = NewPss(lto, nid)

	return lps, ct, vct
}

// does exactly what it says
func makeFakeMsg(t *testing.T, pt *pssTester, sender []byte) []byte {

	code, found := pt.vct.GetCode(&PssTestPayload{})
	if !found {
		t.Fatalf("pssTestPayload type not registered")
	}

	data := PssTestPayload{
		Data: "Bar",
	}

	rlpbundle, err := pt.MakeMsg(code, data)
	if err != nil {
		return nil
	}

	return rlpbundle
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

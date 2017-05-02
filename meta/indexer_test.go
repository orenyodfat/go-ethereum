package meta

import (
	"bytes"
	"net"

	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/p2p/adapters"
	"github.com/ethereum/go-ethereum/p2p/protocols"
	"github.com/ethereum/go-ethereum/p2p/simulations"
	p2ptest "github.com/ethereum/go-ethereum/p2p/testing"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/ethereum/go-ethereum/swarm/network"
)

// example node simulation peer
// modeled from swarm/network/simulations/discovery/discovery_test.go - commit 08b1e42f
type pssTestNode struct {
	*network.Hive
	*network.Pss
	*adapters.SimNode

	id      *adapters.NodeId
	network *simulations.Network
	trigger chan *adapters.NodeId
	run     adapters.ProtoCall
	ct      *protocols.CodeMap
	expectC chan []int
	ws      *http.Handler
	apifunc func() []rpc.API
}

const (
	protocolName    = "foo"
	protocolVersion = 42
)

//
func init() {
	h := log.CallerFileHandler(log.StreamHandler(os.Stdout, log.TerminalFormat(true)))
	log.Root().SetHandler(h)
}
func check(e error) {
	if e != nil {
		panic(e)
	}
}

func createIndexerPayload() (indexerPayload *IndexerPayload, err error) {

	var resp *http.Response
	var respbody []byte
	server_url := "http://localhost:8500/"

	dat, err := ioutil.ReadFile("testjson.json")
	check(err)
	fmt.Print(string(dat))

	r := bytes.NewReader(dat)

	resp, err = http.Post(server_url+"bzzr:/", "application/json", r)

	if err != nil {
		return nil, fmt.Errorf("Request failed: %v", err)
	}
	defer resp.Body.Close()
	respbody, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("Failed to read body: %v", err)

	}
	fmt.Println("hash :" + string(respbody))
	return &IndexerPayload{Data: string(respbody), Command: "update"}, nil

}
func testSendToNode(t *testing.T) {
	fmt.Println("TestIndexer")
	indexer, _ := NewIndexer()
	//indexer.Subscribetometaupdates()

	peerid := "b4287791639128495e865a47ee49c8431e2d9dddc9bcea0b8a70a199a78382980cbaeb99fac9c1fb18ae0f261c5106e11a89c3cff74dc560de7d82c162423e68"
	peeridhex, _ := hex.DecodeString(peerid)
	nid := adapters.NewNodeId(peeridhex)
	fmt.Println(nid.NodeID)
	peer := p2p.NewPeer(nid.NodeID, "adapters. .Name(nid.Bytes())", []p2p.Cap{})

	fmt.Println(peer.ID())

	payload, err := createIndexerPayload()
	if err != nil {
		t.Fatal(err)
	}

	wr, _ := p2p.MsgPipe()

	protocols.NewPeer(peer, indexer.vct, wr).Send(payload)
}
func testIndexer(t *testing.T) {
	fmt.Println("TestIndexer")
	indexer, _ := NewIndexer()
	//indexer.Subscribetometaupdates()

	addr := network.RandomAddr()

	pt := p2ptest.NewProtocolTester(t, network.NodeId(addr), 2, indexer.proto.Run)

	code, found := indexer.vct.GetCode(&IndexerPayload{})
	if found == false {
		fmt.Println("not found")
		return
	}
	payload, err := createIndexerPayload()
	if err != nil {
		t.Fatal(err)
	}

	pt.TestExchanges(
		p2ptest.Exchange{
			Triggers: []p2ptest.Trigger{
				p2ptest.Trigger{
					Code: code,
					Msg:  payload,
					Peer: pt.Ids[0],
				},
			},
		})

	// vct := protocols.NewCodeMap("indexer", uint(1), 65535, &IndexerUpdateNotification{})
	// code, found = vct.GetCode(&IndexerUpdateNotification{})
	// if found == false {
	// 	fmt.Println("not found")
	// 	return
	// }
	// for _, id := range pt.Ids {
	// 	pt.TestExchanges(
	// 		p2ptest.Exchange{
	// 			Expects: []p2ptest.Expect{
	// 				p2ptest.Expect{
	// 					Code: code,
	// 					Msg:  &IndexerUpdateNotification{},
	// 					Peer: id,
	// 				},
	// 			},
	// 		},
	// 	)
	// }
	//subscribetoIndexerNotifications(t)
	time.Sleep(time.Second * 5)
}

func subscribetoindexernotifictions(indexer *Indexer, pss *network.Pss) (err error) {

	ch := make(chan []byte)
	//fmt.Println("sendUpdateNotification", "from", pss.GetAddr().OverlayAddr())
	fmt.Println("subscribetoindexernotifictions", indexer.notificationtopic)
	pss.Register(indexer.notificationtopic, testNotificationsHandler())

	psssub, err := pss.Subscribe(&indexer.notificationtopic, ch)
	if err != nil {
		return fmt.Errorf("pss subscription topic %v  failed: %v", indexer.notificationtopic, err)
	}

	fmt.Println("subscribetoindexernotifictions")

	go func(topic network.PssTopic) {
		for {
			select {
			case msg := <-ch:
				//if err := notifier.Notify(sub.ID, msg); err != nil {
				log.Warn(fmt.Sprintf("notification on pss sub topic %v rpc  msg %v failed!", topic, msg))
				//}
			case err := <-psssub.Err():
				log.Warn(fmt.Sprintf("caught subscription error in pss sub topic: %v %v", topic, err))
				return
				// case <-notifier.Closed():
				// 	log.Warn(fmt.Sprintf("rpc sub notifier closed"))
				// 	psssub.Unsubscribe()
				// 	return
				// case err := <-sub.Err():
				// 	log.Warn(fmt.Sprintf("rpc sub closed: %v", err))
				// 	psssub.Unsubscribe()
				// 	return
			}
		}
		//return nil
	}(indexer.notificationtopic)
	return nil
}

func TestIndexerPss(t *testing.T) {

	var action func(ctx context.Context) error
	var check func(ctx context.Context, id *adapters.NodeId) (bool, error)
	var ctx context.Context
	var result *simulations.StepResult
	var timeout time.Duration
	var cancel context.CancelFunc

	var firstpssnode *adapters.NodeId
	var secondpssnode *adapters.NodeId

	payload, err := createIndexerPayload()
	if err != nil {
		t.Fatal(err)
	}
	indexer, _ := NewIndexer()

	vct := protocols.NewCodeMap(protocolName, uint(protocolVersion), 65535, &IndexerPayload{})
	//topic, _ := MakeTopic(protocolName, protocolName)

	fullnodes := []*adapters.NodeId{}
	trigger := make(chan *adapters.NodeId)
	net := simulations.NewNetwork(&simulations.NetworkConfig{
		Id:      "0",
		Backend: true,
	})
	testpeers := make(map[*adapters.NodeId]*IndexerPeer)
	nodes := newPssSimulationTester(t, 3, 2, net, trigger, vct, protocolName, protocolVersion, testpeers, indexer)
	ids := []*adapters.NodeId{} // ohh risky! but the action for a specific id should come before the expect anyway

	action = func(ctx context.Context) error {
		var thinnodeid *adapters.NodeId
		for id, _ := range nodes {
			ids = append(ids, id)
			if _, ok := testpeers[id]; ok {
				log.Trace(fmt.Sprintf("adding fullnode %x to testpeers %p", common.ByteLabel(id.Bytes()), testpeers))
				fullnodes = append(fullnodes, id)
			} else {
				thinnodeid = id
			}
		}
		if err := net.Connect(fullnodes[0], thinnodeid); err != nil {
			return err
		}
		if err := net.Connect(thinnodeid, fullnodes[1]); err != nil {
			return err
		}

		/*for i, id := range ids {
			var peerId *adapters.NodeId
			if i != 0 {
				peerId = ids[i-1]
				if err := net.Connect(id, peerId); err != nil {
					return err
				}
			}
		}*/
		return nil
	}
	check = func(ctx context.Context, id *adapters.NodeId) (bool, error) {
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		default:
		}

		node, ok := nodes[id]
		if !ok {
			return false, fmt.Errorf("unknown node: %s (%v)", id, node)
		} else {
			log.Trace(fmt.Sprintf("sim check ok node %v", id))
		}

		return true, nil
	}

	timeout = 10 * time.Second
	ctx, cancel = context.WithTimeout(context.Background(), timeout)

	result = simulations.NewSimulation(net).Run(ctx, &simulations.Step{
		Action:  action,
		Trigger: trigger,
		Expect: &simulations.Expectation{
			Nodes: ids,
			Check: check,
		},
	})
	if result.Error != nil {
		t.Fatalf("simulation failed: %s", result.Error)
	}
	cancel()

	nonode := &adapters.NodeId{}
	firstpssnode = nonode
	secondpssnode = nonode

	// first find a node that we're connected to
	for firstpssnode == nonode {
		log.Debug(fmt.Sprintf("Waiting for pss relaypeer for %x close to %x ...", common.ByteLabel(nodes[fullnodes[0]].OverlayAddr()), common.ByteLabel(nodes[ids[1]].OverlayAddr())))
		nodes[fullnodes[0]].Pss.Overlay.EachLivePeer(nodes[fullnodes[1]].OverlayAddr(), 256, func(p network.Peer, po int, isprox bool) bool {
			for _, id := range ids {
				if id.NodeID == p.ID() {
					firstpssnode = id
					log.Debug(fmt.Sprintf("PSS relay found; relaynode %v kademlia %v", common.ByteLabel(id.Bytes()), common.ByteLabel(firstpssnode.Bytes())))
				}
			}
			if firstpssnode == nonode {
				return true
			}
			return false
		})
		if firstpssnode == nonode {
			time.Sleep(time.Millisecond * 100)
		}
	}

	// then find the node it's connected to
	for secondpssnode == nonode {
		log.Debug(fmt.Sprintf("PSS kademlia: Waiting for recipientpeer for %x close to %x ...", common.ByteLabel(nodes[firstpssnode].OverlayAddr()), common.ByteLabel(nodes[fullnodes[1]].OverlayAddr())))
		nodes[firstpssnode].Pss.Overlay.EachLivePeer(nodes[fullnodes[1]].OverlayAddr(), 256, func(p network.Peer, po int, isprox bool) bool {
			for _, id := range ids {
				if id.NodeID == p.ID() && id.NodeID != fullnodes[0].NodeID {
					secondpssnode = id
					log.Debug(fmt.Sprintf("PSS recipient found; relaynode %v kademlia %v", common.ByteLabel(id.Bytes()), common.ByteLabel(secondpssnode.Bytes())))
				}
			}
			if secondpssnode == nonode {
				return true
			}
			return false
		})
		if secondpssnode == nonode {
			time.Sleep(time.Millisecond * 100)
		}
	}

	action = func(ctx context.Context) error {
		code, _ := indexer.vct.GetCode(&IndexerPayload{})
		// msgbytes, _ := network.MakeMsgTMP(code, &IndexerPayload{
		// 	Data: "ping",
		// })
		for _, id := range ids {

			msgbytes, _ := network.MakeMsg(code, IndexerPayload{Command: "register", From: nodes[id].OverlayAddr()})
			go func() {
				oaddr := nodes[secondpssnode].OverlayAddr()
				err := nodes[ids[0]].Pss.Send(oaddr, indexer.topic, msgbytes)
				if err != nil {
					t.Fatalf("could not send pss: %v", err)
				}
				trigger <- ids[0]
			}()

		}
		time.Sleep(time.Millisecond * 1000)
		msgbytes, _ := network.MakeMsg(code, payload)
		SetIndexerAddress(nodes[secondpssnode].OverlayAddr())

		go func() {
			oaddr := nodes[secondpssnode].OverlayAddr()
			err := nodes[ids[0]].Pss.Send(oaddr, indexer.topic, msgbytes)
			if err != nil {
				t.Fatalf("could not send pss: %v", err)
			}
			trigger <- ids[0]
		}()

		return nil
	}
	check = func(ctx context.Context, id *adapters.NodeId) (bool, error) {
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		default:
		}

		// also need to know if the protocolpeer is set up
		fmt.Println("check!")
		time.Sleep(time.Millisecond * 100)
		suc := <-testpeers[ids[0]].successC
		fmt.Println("check1")
		return suc, nil
		//return true, nil
	}

	timeout = 10 * time.Second
	ctx, cancel = context.WithTimeout(context.Background(), timeout)
	defer cancel()

	result = simulations.NewSimulation(net).Run(ctx, &simulations.Step{
		Action:  action,
		Trigger: trigger,
		Expect: &simulations.Expectation{
			Nodes: []*adapters.NodeId{ids[0]},
			Check: check,
		},
	})
	if result.Error != nil {
		t.Fatalf("simulation failed: %s", result.Error)
	}

	t.Log("Simulation Passed:")
}

// test framework below

// numnodes: how many nodes to create
// pssnodeidx: on which node indices to start the pss
// net: the simulated network
// trigger: hook needed for simulation event reporting
// vct: codemap for virtual protocol
// name: name for virtual protocol (and pss topic)
// version: name for virtual protocol (and pss topic)
// testpeers: pss-specific peers, with hook needed for simulation event reporting

func newPssSimulationTester(t *testing.T, numnodes int, numfullnodes int, net *simulations.Network, trigger chan *adapters.NodeId, vct *protocols.CodeMap, name string, version int, testpeers map[*adapters.NodeId]*IndexerPeer, indexer *Indexer) map[*adapters.NodeId]*pssTestNode {
	//topic, _ := network.MakeTopic(name, version)
	nodes := make(map[*adapters.NodeId]*pssTestNode, numnodes)
	psss := make(map[*adapters.NodeId]*network.Pss)
	net.SetNaf(func(conf *simulations.NodeConfig) adapters.NodeAdapter {
		node := &pssTestNode{
			Pss:     psss[conf.Id],
			Hive:    nil,
			SimNode: &adapters.SimNode{},
			id:      conf.Id,
			network: net,
			trigger: trigger,
			ct:      vct,
			apifunc: func() []rpc.API { return nil },
			expectC: make(chan []int),
		}
		var handlefunc func(interface{}) error
		addr := network.NewPeerAddrFromNodeId(conf.Id)
		if testpeers[conf.Id] != nil {
			handlefunc = makePssHandleProtocol(psss[conf.Id])
			log.Trace(fmt.Sprintf("Making full protocol id %x addr %x (testpeers %p)", common.ByteLabel(conf.Id.Bytes()), common.ByteLabel(addr.OverlayAddr()), testpeers))
		} else {
			handlefunc = makePssHandleForward(psss[conf.Id])
		}
		testservice := newPssTestService(t, handlefunc, node)
		nodes[conf.Id] = testservice.node
		svc := adapters.NewSimNode(conf.Id, testservice, net)
		testservice.Start(svc)
		return node.SimNode
	})

	configs := make([]*simulations.NodeConfig, numnodes)

	for i := 0; i < numnodes; i++ {
		configs[i] = simulations.RandomNodeConfig()
	}
	for i, conf := range configs {

		addr := network.NewPeerAddrFromNodeId(conf.Id)
		psss[conf.Id] = makePss(addr.OverlayAddr())

		// addr := network.NewPeerAddrFromNodeId(conf.Id)
		//
		// psss[conf.Id] = makePss(addr.OverlayAddr())
		if i < numfullnodes {
			tp := &IndexerPeer{
				Peer: &protocols.Peer{
					Peer: &p2p.Peer{},
				},
				pss: psss[conf.Id],

				successC: make(chan bool),
				resultC:  make(chan int),
			}
			testpeers[conf.Id] = tp
			indexer.Subscribetometaupdates(conf.Id)
			indexer.IndexerNotificationSetup()
			//addr := NewPeerAddrFromNodeId(conf.Id)
			psss[conf.Id] = indexer.pss
			//targetprotocol := makeCustomProtocol(name, version, vct, testpeers[conf.Id])
			//pssprotocol := network.NewPssProtocol(psss[conf.Id], &topic, vct, indexer.proto)
			//psss[conf.Id].Register(topic, targetprotocol.GetHandler())
		}

		net.NewNodeWithConfig(conf)
		if err := net.Start(conf.Id); err != nil {
			t.Fatalf("error starting node %s: %s", conf.Id.Label(), err)
		}
		subscribetoindexernotifictions(indexer, psss[conf.Id])
	}
	return nodes
}

func makePssHandleForward(ps *network.Pss) func(msg interface{}) error {
	// for the simple check it passes on the message if it's not for us
	return func(msg interface{}) error {
		pssmsg := msg.(*network.PssMsg)
		if ps.IsSelfRecipient(pssmsg) {
			log.Trace("pss for us .. yay!")
		} else {
			log.Trace("passing on pss")
			return ps.Forward(pssmsg)
		}
		return nil
	}
}

func makePssHandleProtocol(ps *network.Pss) func(msg interface{}) error {
	return func(msg interface{}) error {
		pssmsg := msg.(*network.PssMsg)

		if ps.IsSelfRecipient(pssmsg) {
			log.Trace("pss for us ... let's process!")
			env := pssmsg.Payload
			umsg := env.Payload // this will be rlp encrypted
			f := ps.GetHandler(env.Topic)
			if f == nil {
				return fmt.Errorf("No registered handler for topic '%s'", env.Topic)
			}
			nid := adapters.NewNodeId(env.SenderUAddr)
			p := p2p.NewPeer(nid.NodeID, fmt.Sprintf("%x", common.ByteLabel(nid.Bytes())), []p2p.Cap{})
			return f(umsg, p, env.SenderOAddr)
		} else {
			log.Trace("pss was for someone else :'(")
			return ps.Forward(pssmsg)
		}
		//return nil
	}
}

func (n *pssTestNode) OverlayAddr() []byte {
	return n.Pss.Overlay.GetAddr().OverlayAddr()
}

func (n *pssTestNode) UnderlayAddr() []byte {
	return n.id.Bytes()
}

type pssTestService struct {
	node    *pssTestNode // get addrs from this
	msgFunc func(interface{}) error
}

func newPssTestService(t *testing.T, handlefunc func(interface{}) error, testnode *pssTestNode) *pssTestService {
	hp := network.NewHiveParams()
	//hp.CallInterval = 250
	testnode.Hive = network.NewHive(hp, testnode.Pss.Overlay)
	return &pssTestService{
		//nid := adapters.NewNodeId(addr.UnderlayAddr())
		msgFunc: handlefunc,
		node:    testnode,
	}
}

func (self *pssTestService) Start(server p2p.Server) error {
	self.node.SimNode = server.(*adapters.SimNode) // server is adapter.SimnNode now
	return nil
}

func (self *pssTestService) Stop() error {
	return nil
}

func (self *pssTestService) Protocols() []p2p.Protocol {
	ct := network.BzzCodeMap()
	for _, m := range network.DiscoveryMsgs {
		ct.Register(m)
	}
	ct.Register(&network.PssMsg{})

	srv := func(p network.Peer) error {
		p.Register(&network.PssMsg{}, self.msgFunc)
		self.node.Add(p)
		p.DisconnectHook(func(err error) {
			self.node.Remove(p)
		})
		return nil
	}

	proto := network.Bzz(self.node.OverlayAddr(), self.node.UnderlayAddr(), ct, srv, nil, nil)

	return []p2p.Protocol{*proto}
}

func (self *pssTestService) APIs() []rpc.API {
	/*return []rpc.API{
		rpc.API{
			Namespace: "eth",
			Version:	"0.1/pss",
			Service:	api.NewPssApi(self.pss),
			Public:		true,
		},
	}*/
	return nil
}

//
// func makePss(addr []byte) *network.Pss {
// 	kp := network.NewKadParams()
// 	kp.MinProxBinSize = 3
//
// 	pp := network.NewPssParams()
//
// 	overlay := network.NewKademlia(addr, kp)
// 	ps := network.NewPss(overlay, pp)
// 	//overlay.Prune(time.Tick(time.Millisecond * 250))
// 	return ps
// }

func testNotificationsHandler() func([]byte, *p2p.Peer, []byte) error {
	//pingtopic, _ := MakeTopic(PingTopicName, PingTopicVersion)
	return func(msg []byte, p *p2p.Peer, from []byte) error {

		fmt.Println("got notification")

		return nil
	}
}

func TestPssFullWS(t *testing.T) {

	// settings for ws servers
	var srvsendep = "localhost:18546"
	var srvrecvep = "localhost:18547"
	var clientrecvok, clientsendok bool
	var clientrecv, clientsend *rpc.Client

	var action func(ctx context.Context) error
	var check func(ctx context.Context, id *adapters.NodeId) (bool, error)
	var ctx context.Context
	var result *simulations.StepResult
	var timeout time.Duration
	var cancel context.CancelFunc

	payload, err := createIndexerPayload()

	if err != nil {
		t.Fatal(err)
	}

	indexer, _ = NewIndexer()

	var firstpssnode, secondpssnode *adapters.NodeId
	fullnodes := []*adapters.NodeId{}
	vct := protocols.NewCodeMap(protocolName, protocolVersion, 65535, &IndexerPayload{})
	//topic, _ := MakeTopic(pingTopicName, pingTopicVersion)

	trigger := make(chan *adapters.NodeId)
	simnet := simulations.NewNetwork(&simulations.NetworkConfig{
		Id:      "0",
		Backend: true,
	})
	testpeers := make(map[*adapters.NodeId]*IndexerPeer)
	nodes := newPssSimulationTester(t, 3, 2, simnet, trigger, vct, protocolName, protocolVersion, testpeers, indexer)
	ids := []*adapters.NodeId{} // ohh risky! but the action for a specific id should come before the expect anyway

	action = func(ctx context.Context) error {
		var thinnodeid *adapters.NodeId
		for id, node := range nodes {
			ids = append(ids, id)
			if _, ok := testpeers[id]; ok {
				log.Trace(fmt.Sprintf("adding fullnode %x to testpeers %p", common.ByteLabel(id.Bytes()), testpeers))
				fullnodes = append(fullnodes, id)
				//node.Pss.Register(topic, node.Pss.GetPingHandler())
				srv := rpc.NewServer()
				for _, rpcapi := range node.apifunc() {
					srv.RegisterName(rpcapi.Namespace, rpcapi.Service)
				}
				ws := srv.WebsocketHandler([]string{"*"})
				node.ws = &ws
			} else {
				thinnodeid = id
			}
		}
		if err := simnet.Connect(fullnodes[0], thinnodeid); err != nil {
			return err
		}
		if err := simnet.Connect(thinnodeid, fullnodes[1]); err != nil {
			return err
		}

		return nil
	}

	check = func(ctx context.Context, id *adapters.NodeId) (bool, error) {
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		default:
		}

		node, ok := nodes[id]
		if !ok {
			return false, fmt.Errorf("unknown node: %s (%v)", id, node)
		} else {
			log.Trace(fmt.Sprintf("sim check ok node %v", id))
		}

		return true, nil
	}

	timeout = 10 * time.Second
	ctx, cancel = context.WithTimeout(context.Background(), timeout)

	result = simulations.NewSimulation(simnet).Run(ctx, &simulations.Step{
		Action:  action,
		Trigger: trigger,
		Expect: &simulations.Expectation{
			Nodes: ids,
			Check: check,
		},
	})
	if result.Error != nil {
		t.Fatalf("simulation failed: %s", result.Error)
	}
	cancel()

	nonode := &adapters.NodeId{}
	firstpssnode = nonode
	secondpssnode = nonode

	// first find a node that we're connected to
	for firstpssnode == nonode {
		log.Debug(fmt.Sprintf("Waiting for pss relaypeer for %x close to %x ...", common.ByteLabel(nodes[fullnodes[0]].OverlayAddr()), common.ByteLabel(nodes[fullnodes[1]].OverlayAddr())))
		nodes[fullnodes[0]].Pss.Overlay.EachLivePeer(nodes[fullnodes[1]].OverlayAddr(), 256, func(p network.Peer, po int, isprox bool) bool {
			for _, id := range ids {
				if id.NodeID == p.ID() {
					firstpssnode = id
					log.Debug(fmt.Sprintf("PSS relay found; relaynode %x", common.ByteLabel(nodes[firstpssnode].OverlayAddr())))
				}
			}
			if firstpssnode == nonode {
				return true
			}
			return false
		})
		if firstpssnode == nonode {
			time.Sleep(time.Millisecond * 100)
		}
	}

	// then find the node it's connected to
	for secondpssnode == nonode {
		log.Debug(fmt.Sprintf("PSS kademlia: Waiting for recipientpeer for %x close to %x ...", common.ByteLabel(nodes[firstpssnode].OverlayAddr()), common.ByteLabel(nodes[fullnodes[1]].OverlayAddr())))
		nodes[firstpssnode].Pss.Overlay.EachLivePeer(nodes[fullnodes[1]].OverlayAddr(), 256, func(p network.Peer, po int, isprox bool) bool {
			for _, id := range ids {
				if id.NodeID == p.ID() && id.NodeID != fullnodes[0].NodeID {
					secondpssnode = id
					log.Debug(fmt.Sprintf("PSS recipient found; relaynode %x", common.ByteLabel(nodes[secondpssnode].OverlayAddr())))
				}
			}
			if secondpssnode == nonode {
				return true
			}
			return false
		})
		if secondpssnode == nonode {
			time.Sleep(time.Millisecond * 100)
		}
	}

	srvrecvl, err := net.Listen("tcp", srvrecvep)
	if err != nil {
		t.Fatalf("Tcp (recv) on %s failed: %v", srvrecvep, err)
	}
	go func() {
		err := http.Serve(srvrecvl, *nodes[fullnodes[1]].ws)
		if err != nil {
			t.Fatalf("http serve (recv) on %s failed: %v", srvrecvep, err)
		}
	}()

	srvsendl, err := net.Listen("tcp", srvsendep)
	if err != nil {
		t.Fatalf("Tcp (send) on %s failed: %v", srvsendep, err)
	}
	go func() {
		err := http.Serve(srvsendl, *nodes[fullnodes[0]].ws)
		if err != nil {
			t.Fatalf("http serve (send) on %s failed: %v", srvrecvep, err)
		}
	}()

	for !clientrecvok {
		log.Trace("attempting clientrecv connect")
		clientrecv, err = rpc.DialWebsocket(context.Background(), "ws://"+srvrecvep, "ws://localhost")
		if err == nil {
			clientrecvok = true
		} else {
			log.Debug("clientrecv failed, retrying", "error", err)
			time.Sleep(time.Millisecond * 250)
		}
	}

	for !clientsendok {
		log.Trace("attempting clientsend connect")
		clientsend, err = rpc.DialWebsocket(context.Background(), "ws://"+srvsendep, "ws://localhost")
		if err == nil {
			clientsendok = true
		} else {
			log.Debug("clientsend failed, retrying", "error", err)
			time.Sleep(time.Millisecond * 250)
		}
	}

	trigger = make(chan *adapters.NodeId)
	ch := make(chan string)

	action = func(ctx context.Context) error {
		go func() {
			clientrecv.EthSubscribe(ctx, ch, "newMsg", indexer.topic)
			b, _ := json.Marshal(payload)
			clientsend.Call(nil, "eth_sendRaw", nodes[secondpssnode].Pss.Overlay.GetAddr().OverlayAddr(), indexer.topic, b)
			trigger <- secondpssnode
		}()
		return nil
	}
	check = func(ctx context.Context, id *adapters.NodeId) (bool, error) {
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		default:
		}

		select {
		case msg := <-ch:
			log.Trace(fmt.Sprintf("notify!: %v", msg))
		case <-time.NewTimer(time.Second).C:
			log.Trace(fmt.Sprintf("no notifies :'("))
		}
		// also need to know if the protocolpeer is set up

		return true, nil
	}

	timeout = 10 * time.Second
	ctx, cancel = context.WithTimeout(context.Background(), timeout)
	defer cancel()

	result = simulations.NewSimulation(simnet).Run(ctx, &simulations.Step{
		Action:  action,
		Trigger: trigger,
		Expect: &simulations.Expectation{
			Nodes: []*adapters.NodeId{secondpssnode},
			Check: check,
		},
	})
	if result.Error != nil {
		t.Fatalf("simulation failed: %s", result.Error)
	}

	t.Log("Simulation Passed:")
}

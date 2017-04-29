package discovery_test

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/docker/docker/pkg/reexec"
	"github.com/ethereum/go-ethereum/log"
	p2pnode "github.com/ethereum/go-ethereum/node"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/p2p/adapters"
	"github.com/ethereum/go-ethereum/p2p/simulations"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/ethereum/go-ethereum/swarm/network"
)

// serviceName is used with the exec adapter so the exec'd binary knows which
// service to execute
const serviceName = "discovery"

func init() {
	// register the discovery service which will run as a devp2p
	// protocol when using the exec adapter
	adapters.RegisterService(serviceName, func(id *adapters.NodeId) p2pnode.Service {
		return newNode(id)
	})

	// log.Root().SetHandler(log.LvlFilterHandler(log.LvlError, log.StreamHandler(os.Stderr, log.TerminalFormat(false))))
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlTrace, log.StreamHandler(os.Stderr, log.TerminalFormat(false))))
}

func TestMain(m *testing.M) {
	// reexec a service if we have been exec'd by the exec adapter
	if reexec.Init() {
		return
	}

	os.Exit(m.Run())
}

func TestDiscoverySimulationDockerAdapter(t *testing.T) {
	setup := func(net *simulations.Network, trigger chan *adapters.NodeId) {
		var ids []*adapters.NodeId

		// TODO: get events from the devp2p node
		time.AfterFunc(10*time.Second, func() {
			for _, id := range ids {
				trigger <- id
			}
		})

		net.SetNaf(func(conf *simulations.NodeConfig) adapters.NodeAdapter {
			node, err := adapters.NewDockerNode(conf.Id, conf.PrivateKey, serviceName)
			if err != nil {
				panic(err)
			}
			ids = append(ids, conf.Id)
			return node
		})
	}

	testDiscoverySimulation(t, setup)
}

func TestDiscoverySimulationExecAdapter(t *testing.T) {
	baseDir, err := ioutil.TempDir("", "swarm-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(baseDir)

	setup := func(net *simulations.Network, trigger chan *adapters.NodeId) {
		var ids []*adapters.NodeId

		// TODO: get events from the devp2p node
		time.AfterFunc(10*time.Second, func() {
			for _, id := range ids {
				trigger <- id
			}
		})

		net.SetNaf(func(conf *simulations.NodeConfig) adapters.NodeAdapter {
			node, err := adapters.NewExecNode(conf.Id, conf.PrivateKey, serviceName, baseDir)
			if err != nil {
				panic(err)
			}
			ids = append(ids, conf.Id)
			return node
		})
	}

	testDiscoverySimulation(t, setup)
}

func TestDiscoverySimulationSimAdapter(t *testing.T) {
	setup := func(net *simulations.Network, trigger chan *adapters.NodeId) {
		net.SetNaf(func(conf *simulations.NodeConfig) adapters.NodeAdapter {
			node := newNode(conf.Id)
			node.trigger = trigger
			return adapters.NewSimNode(conf.Id, node, net)
		})
	}

	testDiscoverySimulation(t, setup)
}

func testDiscoverySimulation(t *testing.T, setup func(net *simulations.Network, trigger chan *adapters.NodeId)) {
	// create 10 node network
	nodeCount := 10
	trigger := make(chan *adapters.NodeId)
	net := simulations.NewNetwork(&simulations.NetworkConfig{
		Id:      "0",
		Backend: true,
	})
	defer net.Shutdown()
	setup(net, trigger)
	ids := make([]*adapters.NodeId, nodeCount)
	for i := 0; i < nodeCount; i++ {
		conf, err := net.NewNode()
		if err != nil {
			t.Fatalf("error starting node %s: %s", conf.Id.Label(), err)
		}
		if err := net.Start(conf.Id); err != nil {
			t.Fatalf("error starting node %s: %s", conf.Id.Label(), err)
		}
		ids[i] = conf.Id
	}

	// run a simulation which connects the 10 nodes in a ring and waits
	// for full peer discovery
	action := func(ctx context.Context) error {
		for i, id := range ids {
			var peerId *adapters.NodeId
			if i == 0 {
				peerId = ids[len(ids)-1]
			} else {
				peerId = ids[i-1]
			}
			if err := net.Connect(id, peerId); err != nil {
				return err
			}
		}
		return nil
	}
	check := func(ctx context.Context, id *adapters.NodeId) (bool, error) {
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		default:
		}

		node := net.GetNode(id)
		if node == nil {
			return false, fmt.Errorf("unknown node: %s", id)
		}

		// TODO: check list of peers
		_ = node

		return true, nil
	}

	timeout := 30 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	result := simulations.NewSimulation(net).Run(ctx, &simulations.Step{
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

	t.Log("Simulation Passed:")
	t.Logf("Duration: %s", result.FinishedAt.Sub(result.StartedAt))
	for _, id := range ids {
		t.Logf("Node %s passed in %s", id.Label(), result.Passes[id].Sub(result.StartedAt))
	}
	t.Logf("Events:")
	for _, event := range result.NetworkEvents {
		t.Log(event)
	}
}

type node struct {
	*network.Hive
	*adapters.SimNode

	id       *adapters.NodeId
	trigger  chan *adapters.NodeId
	protocol *p2p.Protocol
}

func newNode(id *adapters.NodeId) *node {
	addr := network.NewPeerAddrFromNodeId(id)
	kademlia := newKademlia(addr.OverlayAddr())
	hive := newHive(kademlia)
	codeMap := network.BzzCodeMap(network.DiscoveryMsgs...)
	node := &node{
		Hive: hive,
		id:   id,
	}
	services := func(peer network.Peer) error {
		discoveryPeer := network.NewDiscovery(peer, kademlia)
		node.Add(discoveryPeer)
		peer.DisconnectHook(func(err error) {
			node.Remove(discoveryPeer)
		})
		return nil
	}
	node.protocol = network.Bzz(addr.OverlayAddr(), addr.UnderlayAddr(), codeMap, services, nil, nil)
	return node
}

func newKademlia(overlayAddr []byte) *network.Kademlia {
	params := network.NewKadParams()
	params.MinProxBinSize = 2
	params.MaxBinSize = 3
	params.MinBinSize = 1
	params.MaxRetries = 1000
	params.RetryExponent = 2
	params.RetryInterval = 1000000

	return network.NewKademlia(overlayAddr, params)
}

func newHive(kademlia *network.Kademlia) *network.Hive {
	params := network.NewHiveParams()
	params.CallInterval = 5000

	return network.NewHive(params, kademlia)
}

func (n *node) Protocols() []p2p.Protocol {
	return []p2p.Protocol{*n.protocol}
}

func (n *node) APIs() []rpc.API {
	return nil
}

func (n *node) Start(server p2p.Server) error {
	return n.Hive.Start(server, n.hiveKeepAlive)
}

func (n *node) Stop() error {
	n.Hive.Stop()
	return nil
}

func (n *node) Add(peer network.Peer) error {
	err := n.Hive.Add(peer)
	n.triggerCheck()
	return err
}

func (n *node) Remove(peer network.Peer) {
	n.Hive.Remove(peer)
	n.triggerCheck()
}

func (n *node) hiveKeepAlive() <-chan time.Time {
	return time.Tick(time.Second)
}

func (n *node) triggerCheck() {
	// TODO: rate limit the trigger?
	go func() { n.trigger <- n.id }()
}

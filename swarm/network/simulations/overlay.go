// +build none

// You can run this simulation using
//
//    go run ./swarm/network/simulations/overlay.go
package main

import (
	"fmt"
	"math/rand"
	"os"
	//	"reflect"
	"runtime"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/p2p/adapters"
	"github.com/ethereum/go-ethereum/p2p/discover"
	"github.com/ethereum/go-ethereum/p2p/simulations"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/ethereum/go-ethereum/swarm/network"
)

// Network extends simulations.Network with hives for each node.
type Network struct {
	*simulations.Network
	hives map[discover.NodeID]*network.Hive
}

// SimNode is the adapter used by Swarm simulations.
type SimNode struct {
	hive     *network.Hive
	protocol *p2p.Protocol
}

func (s *SimNode) Protocols() []p2p.Protocol {
	return []p2p.Protocol{*s.protocol}
}

func (s *SimNode) APIs() []rpc.API {
	return nil
}

// the hive update ticker for hive
func af() <-chan time.Time {
	return time.NewTicker(1 * time.Second).C
}

// Start() starts up the hive
// makes SimNode implement node.Service
func (self *SimNode) Start(server p2p.Server) error {
	return self.hive.Start(server, af)
}

// Stop() shuts down the hive
// makes SimNode implement node.Service
func (self *SimNode) Stop() error {
	self.hive.Stop()
	return nil
}

// NewSimNode creates adapters for nodes in the simulation.
func (self *Network) NewSimNode(conf *simulations.NodeConfig) adapters.NodeAdapter {
	id := conf.Id
	addr := network.NewPeerAddrFromNodeId(id)
	kp := network.NewKadParams()

	kp.MinProxBinSize = 2
	kp.MaxBinSize = 3
	kp.MinBinSize = 1
	kp.MaxRetries = 1000
	kp.RetryExponent = 2
	kp.RetryInterval = 1000000

	to := network.NewKademlia(addr.OverlayAddr(), kp) // overlay topology driver
	hp := network.NewHiveParams()
	hp.CallInterval = 5000
	pp := network.NewHive(hp, to) // hive
	self.hives[id.NodeID] = pp    // remember hive

	services := func(p network.Peer) error {
		dp := network.NewDiscovery(p, to)
		pp.Add(dp)
		log.Trace(fmt.Sprintf("kademlia on %v", dp))
		p.DisconnectHook(func(err error) {
			pp.Remove(dp)
		})
		return nil
	}

	ct := network.BzzCodeMap(network.DiscoveryMsgs...) // bzz protocol code map

	node := &SimNode{
		hive:     pp,
		protocol: network.Bzz(addr.OverlayAddr(), addr.UnderlayAddr(), ct, services, nil, nil),
	}
	return adapters.NewSimNode(id, node, self.Network)

}

func NewNetwork(net *simulations.Network) *Network {
	n := &Network{
		Network: net,
		hives:   make(map[discover.NodeID]*network.Hive),
	}
	n.SetNaf(n.NewSimNode)
	return n
}

func nethook(conf *simulations.NetworkConfig) (simulations.NetworkControl, *simulations.ResourceController) {
	conf.DefaultMockerConfig = simulations.DefaultMockerConfig()
	conf.DefaultMockerConfig.SwitchonRate = 100
	conf.DefaultMockerConfig.NodesTarget = 15
	conf.DefaultMockerConfig.NewConnCount = 1
	conf.DefaultMockerConfig.DegreeTarget = 0
	conf.Id = "0"
	conf.Backend = true
	net := NewNetwork(simulations.NewNetwork(conf))

	ids := make([]*adapters.NodeId, 10)
	for i := 0; i < 10; i++ {
		conf, err := net.NewNode()
		if err != nil {
			panic(err.Error())
		}
		ids[i] = conf.Id
	}

	for i, id := range ids {
		var peerId *adapters.NodeId
		if i == 0 {
			peerId = ids[len(ids)-1]
		} else {
			peerId = ids[i-1]
		}
		err := net.hives[id.NodeID].Register(network.NewPeerAddrFromNodeId(peerId))
		if err != nil {
			panic(err.Error())
		}
	}
	go func() {
		for _, id := range ids {
			n := rand.Intn(1000)
			time.Sleep(time.Duration(n) * time.Millisecond)
			net.Start(id)
			log.Debug(fmt.Sprintf("node %v starting up", id))
			// time.Sleep(1000 * time.Millisecond)
			// net.Stop(id)
		}
	}()

	for i, id := range ids {
		n := 3000 + i*1000
		go func(id *adapters.NodeId) {
			for {
				// n := rand.Intn(5000)
				// n := 3000
				time.Sleep(time.Duration(n) * time.Millisecond)
				log.Debug(fmt.Sprintf("node %v shutting down", id))
				net.Stop(id)
				// n = rand.Intn(5000)
				n = 2000
				time.Sleep(time.Duration(n) * time.Millisecond)
				log.Debug(fmt.Sprintf("node %v starting up", id))
				net.Start(id)
				n = 5000
			}
		}(id)
	}

	nodes := simulations.NewResourceContoller(
		&simulations.ResourceHandlers{
			//GET /<networkId>/nodes  -- returns all nodes' kademlia table
			Retrieve: &simulations.ResourceHandler{
				Handle: func(msg interface{}, parent *simulations.ResourceController) (interface{}, error) {
					var results []string
					for _, id := range ids {
						pp := net.hives[id.NodeID]
						results = append(results, pp.String())
					}
					return results, nil
				},
				//Type: reflect.TypeOf([]string{}), // this is input not output param structure
			},
		})
	for _, id := range ids {
		idc := simulations.NewResourceContoller(
			&simulations.ResourceHandlers{
				//GET /<networkId>/nodes/<nodeId>  -- returns <nodeId>'s kademlia table
				Retrieve: &simulations.ResourceHandler{
					Handle: func(msg interface{}, parent *simulations.ResourceController) (interface{}, error) {
						pp := net.hives[id.NodeID]
						if pp != nil {
							return pp.String(), nil
						}
						//this shouldn't happen anymore, but just in case
						return nil, fmt.Errorf("Node not found")
					},
					//Type: reflect.TypeOf([]string{}), // this is input not output param structure
				},
			})
		nodes.SetResource(id.String(), idc)
	}
	return net, nodes
}

// var server
func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	log.Root().SetHandler(log.LvlFilterHandler(log.LvlTrace, log.StreamHandler(os.Stderr, log.TerminalFormat(false))))

	c, quitc := simulations.NewSessionController(nethook)

	simulations.StartRestApiServer("8888", c)
	// wait until server shuts down
	<-quitc

}

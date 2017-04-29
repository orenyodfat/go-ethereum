// Copyright 2016 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package swarm

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/contracts/chequebook"
	"github.com/ethereum/go-ethereum/contracts/ens"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/node"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/p2p/adapters"
	"github.com/ethereum/go-ethereum/p2p/discover"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/ethereum/go-ethereum/swarm/api"
	httpapi "github.com/ethereum/go-ethereum/swarm/api/http"
	"github.com/ethereum/go-ethereum/swarm/fuse"
	"github.com/ethereum/go-ethereum/swarm/network"
	"github.com/ethereum/go-ethereum/swarm/storage"
)

// the swarm stack
type Swarm struct {
	config      *api.Config            // swarm configuration
	api         *api.Api               // high level api layer (fs/manifest)
	dns         api.Resolver           // DNS registrar
	//dbAccess    *network.DbAccess      // access to local chunk db iterator and storage counter
	storage     storage.ChunkStore     // internal access to storage, common interface to cloud storage backends
	dpa         *storage.DPA           // distributed preimage archive, the local API to the storage with document level storage/retrieval support
	//depo        network.StorageHandler // remote request handler, interface between bzz protocol and the storage
	cloud       storage.CloudStore     // procurement, cloud storage backend (can multi-cloud)
	hive        *network.Hive          // the logistic manager
	backend     chequebook.Backend     // simple blockchain Backend
	privateKey  *ecdsa.PrivateKey
	corsString  string
	swapEnabled bool
	pssEnabled  bool
	pss			*network.Pss
	lstore      *storage.LocalStore // local store, needs to store for releasing resources after node stopped
	sfs         *fuse.SwarmFS       // need this to cleanup all the active mounts on node exit
	na			*adapters.NodeAdapter
}

type SwarmAPI struct {
	Api     *api.Api
	Backend chequebook.Backend
	PrvKey  *ecdsa.PrivateKey
}

func (self *Swarm) API() *SwarmAPI {
	return &SwarmAPI{
		Api:     self.api,
		Backend: self.backend,
		PrvKey:  self.privateKey,
	}
}

// creates a new swarm service instance
// implements node.Service
func NewSwarm(ctx *node.ServiceContext, backend chequebook.Backend, config *api.Config, swapEnabled, syncEnabled bool, cors string, pssEnabled bool) (self *Swarm, err error) {
	if bytes.Equal(common.FromHex(config.PublicKey), storage.ZeroKey) {
		return nil, fmt.Errorf("empty public key")
	}
	if bytes.Equal(common.FromHex(config.BzzKey), storage.ZeroKey) {
		return nil, fmt.Errorf("empty bzz key")
	}

	self = &Swarm{
		config:      config,
		swapEnabled: swapEnabled,
		backend:     backend,
		privateKey:  config.Swap.PrivateKey(),
		corsString:  cors,
		pssEnabled:	pssEnabled,
	}
	log.Debug(fmt.Sprintf("Setting up Swarm service components"))

	hash := storage.MakeHashFunc(config.ChunkerParams.Hash)
	self.lstore, err = storage.NewLocalStore(hash, config.StoreParams)
	if err != nil {
		return
	}

	// setup local store
	//glog.V(logger.Debug).Infof("Set up local storage")

	//self.dbAccess = network.NewDbAccess(self.lstore)
	log.Debug("Set up local db access (iterator/counter)")
	
	kp := network.NewKadParams()
	
	to := network.NewKademlia(
		common.FromHex(config.BzzKey),
		kp,
	)
	// set up the kademlia hive
	self.hive = network.NewHive(
		config.HiveParams,                    // configuration parameters
		to,
	)
	log.Debug(fmt.Sprintf("Set up swarm network with Kademlia hive"))

	// setup cloud storage backend
	//cloud := network.NewForwarder(self.hive)
	//glog.V(logger.Debug).Infof("-> set swarm forwarder as cloud storage backend")
	
	// setup cloud storage internal access layer
	self.storage = storage.NewNetStore(hash, self.lstore, nil, config.StoreParams)
	log.Debug("-> swarm net store shared access layer to Swarm Chunk Store")

	// set up Depo (storage handler = cloud storage access layer for incoming remote requests)
	// self.depo = network.NewDepo(hash, self.lstore, self.storage)
	// glog.V(logger.Debug).Infof("-> REmote Access to CHunks")

	// set up DPA, the cloud storage local access layer
	dpaChunkStore := storage.NewDpaChunkStore(self.lstore, self.storage)
	log.Debug(fmt.Sprintf("-> Local Access to Swarm"))
	// Swarm Hash Merklised Chunking for Arbitrary-length Document/File storage
	self.dpa = storage.NewDPA(dpaChunkStore, self.config.ChunkerParams)
	log.Debug(fmt.Sprintf("-> Content Store API"))

	// set up high level api
	transactOpts := bind.NewKeyedTransactor(self.privateKey)

	if backend == (*ethclient.Client)(nil) {
		log.Warn("No ENS, please specify non-empty --ethapi to use domain name resolution")
	} else {
		self.dns, err = ens.NewENS(transactOpts, config.EnsRoot, self.backend)
		if err != nil {
			return nil, err
		}
	}
	log.Debug(fmt.Sprintf("-> Swarm Domain Name Registrar @ address %v", config.EnsRoot.Hex()))

	self.api = api.NewApi(self.dpa, self.dns)
	// Manifests for Smart Hosting
	log.Debug(fmt.Sprintf("-> Web3 virtual server API"))

	self.sfs = fuse.NewSwarmFS(self.api)
	log.Debug("-> Initializing Fuse file system")

	return self, nil
}

/*
Start is called when the stack is started
* starts the network kademlia hive peer management
* (starts netStore level 0 api)
* starts DPA level 1 api (chunking -> store/retrieve requests)
* (starts level 2 api)
* starts http proxy server
* registers url scheme handlers for bzz, etc
* TODO: start subservices like sword, swear, swarmdns
*/
// implements the node.Service interface
func (self *Swarm) Start(net p2p.Server) error {
	connectPeer := func(url string) error {
		node, err := discover.ParseNode(url)
		if err != nil {
			return fmt.Errorf("invalid node URL: %v", err)
		}
		net.AddPeer(node)
		return nil
	}
	// set chequebook
	if self.swapEnabled {
		ctx := context.Background() // The initial setup has no deadline.
		err := self.SetChequebook(ctx)
		if err != nil {
			return fmt.Errorf("Unable to set chequebook for SWAP: %v", err)
		}
		log.Debug(fmt.Sprintf("-> cheque book for SWAP: %v", self.config.Swap.Chequebook()))
	} else {
		log.Debug(fmt.Sprintf("SWAP disabled: no cheque book set"))
	}

	log.Warn(fmt.Sprintf("Starting Swarm service"))
	
	glog.V(logger.Warn).Infof("Starting Swarm service")
	self.hive.Start(
		connectPeer,
		func () <-chan time.Time{
			return time.NewTicker(time.Second).C
		},
	)

	log.Info(fmt.Sprintf("Swarm network started on bzz address: %v", self.hive.GetAddr()))

	if self.pssEnabled {
		pssparams := network.NewPssParams()
		self.pss = network.NewPss(self.hive.Overlay, pssparams)
		
		// for testing purposes, shold be removed in production environment!!
		pingtopic, _ := network.MakeTopic("pss", 1)
		self.pss.Register(pingtopic, func(msg []byte, p *p2p.Peer, from []byte) error {
			if bytes.Equal([]byte("ping"), msg) {
				log.Trace(fmt.Sprintf("swarm pss ping from %x sending pong", common.ByteLabel(from)))
				self.pss.Send(from, pingtopic, []byte("pong"))
			}
			return nil
		})
		
		log.Debug("Pss started: %v", self.pss)
	}
	
	self.dpa.Start()
	log.Debug(fmt.Sprintf("Swarm DPA started"))

	// start swarm http proxy server
	if self.config.Port != "" {
		addr := ":" + self.config.Port
		go httpapi.StartHttpServer(self.api, &httpapi.ServerConfig{
			Addr:       addr,
			CorsString: self.corsString,
		})
	}

	log.Debug(fmt.Sprintf("Swarm http proxy started on port: %v", self.config.Port))

	if self.corsString != "" {
		log.Debug(fmt.Sprintf("Swarm http proxy started with corsdomain: %v", self.corsString))
	}

	return nil
}

// implements the node.Service interface
// stops all component services.
func (self *Swarm) Stop() error {
	self.dpa.Stop()
	self.hive.Stop()
	if ch := self.config.Swap.Chequebook(); ch != nil {
		ch.Stop()
		ch.Save()
	}

	if self.lstore != nil {
		self.lstore.DbStore.Close()
	}
	self.sfs.Stop()
	return self.config.Save()
}

// implements the node.Service interface
func (self *Swarm) Protocols() []p2p.Protocol {
	//proto, err := network.Bzz(self.depo, self.backend, self.hive, self.dbAccess, self.config.Swap, self.config.SyncParams, self.config.NetworkId)
	ct := network.BzzCodeMap()
	for _, m := range network.DiscoveryMsgs {
		ct.Register(m)
	}
	if self.pssEnabled {
		ct.Register(&network.PssMsg{})
	}
	
	srv := func(p network.Peer) error {
		if self.pssEnabled {
			p.Register(&network.PssMsg{}, func(msg interface{}) error {
				pssmsg := msg.(*network.PssMsg)

				if self.pss.IsSelfRecipient(pssmsg) {
					log.Trace("pss for us, yay! ... let's process!")
					env := pssmsg.Payload
					umsg := env.Payload
					f := self.pss.GetHandler(env.Topic)
					if f == nil {
						return fmt.Errorf("No registered handler for topic '%s'", env.Topic)
					}
					nid := adapters.NewNodeId(env.SenderUAddr)
					p := p2p.NewPeer(nid.NodeID, fmt.Sprintf("%x", common.ByteLabel(nid.Bytes())), []p2p.Cap{})
					return f(umsg, p, env.SenderOAddr)
				} else {
					log.Trace("pss was for someone else :'( ... forwarding")
					return self.pss.Forward(pssmsg)
				}
				return nil
			})
		}
		self.hive.Add(p)
		p.DisconnectHook(func(err error) {
			self.hive.Remove(p)
		})
		return nil
	}
	
	proto := network.Bzz(
		self.hive.Overlay.GetAddr().OverlayAddr(),
		self.hive.Overlay.GetAddr().UnderlayAddr(),
		ct,
		srv,
		nil,
		nil,
	)
	
	return []p2p.Protocol{*proto}
}

// implements node.Service
// Apis returns the RPC Api descriptors the Swarm implementation offers
func (self *Swarm) APIs() []rpc.API {

	apis := []rpc.API{
		// public APIs
		{
			Namespace: "bzz",
			Version:   "0.1",
			Service:   &Info{self.config, chequebook.ContractParams},
			Public:    true,
		},
		// admin APIs
		{
			Namespace: "bzz",
			Version:   "0.1",
			Service:   api.NewControl(self.api, self.hive),
			Public:    false,
		},
		{
			Namespace: "chequebook",
			Version:   chequebook.Version,
			Service:   chequebook.NewApi(self.config.Swap.Chequebook),
			Public:    false,
		},
		{
			Namespace: "swarmfs",
			Version:   fuse.Swarmfs_Version,
			Service:   self.sfs,
			Public:    false,
		},
		// storage APIs
		// DEPRECATED: Use the HTTP API instead
		{
			Namespace: "bzz",
			Version:   "0.1",
			Service:   api.NewStorage(self.api),
			Public:    true,
		},
		{
			Namespace: "bzz",
			Version:   "0.1",
			Service:   api.NewFileSystem(self.api),
			Public:    false,
		},
		// {Namespace, Version, api.NewAdmin(self), false},
	}

	if self.pssEnabled {
		apis = append(apis, rpc.API{
			Namespace: "eth",
			Version:	"0.1/pss",
			Service:	api.NewPssApi(self.pss),
			Public:		true,
		})
	}
		
	return apis
}

func (self *Swarm) Api() *api.Api {
	return self.api
}

// SetChequebook ensures that the local checquebook is set up on chain.
func (self *Swarm) SetChequebook(ctx context.Context) error {
	err := self.config.Swap.SetChequebook(ctx, self.backend, self.config.Path)
	if err != nil {
		return err
	}
	log.Info(fmt.Sprintf("new chequebook set (%v): saving config file, resetting all connections in the hive", self.config.Swap.Contract.Hex()))
	self.config.Save()
	//self.hive.DropAll()
	return nil
}

// Local swarm without netStore
func NewLocalSwarm(datadir, port string) (self *Swarm, err error) {

	prvKey, err := crypto.GenerateKey()
	if err != nil {
		return
	}

	config, err := api.NewConfig(datadir, common.Address{}, prvKey, network.NetworkId)
	if err != nil {
		return
	}
	config.Port = port

	dpa, err := storage.NewLocalDPA(datadir)
	if err != nil {
		return
	}

	self = &Swarm{
		api:    api.NewApi(dpa, nil),
		config: config,
	}

	return
}
	
// serialisable info about swarm
type Info struct {
	*api.Config
	*chequebook.Params
}

func (self *Info) Info() *Info {
	return self
}

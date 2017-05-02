package meta

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/p2p/adapters"
	"github.com/ethereum/go-ethereum/p2p/protocols"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/ethereum/go-ethereum/swarm/network"
	"github.com/ethereum/go-ethereum/swarm/storage"
)

type IndexerPayload struct {
	Data    string
	Command string
	From    []byte
}

type NewIndex struct {
	Index string
	Key   string //can be name,album,song...
	Value string
	Hash  string
}

type IndexerUpdateNotification struct {
	Index   NewIndex
	Type    string //"notification"
	Subtype string //"new /update"
}

type Address struct {
	addr []byte
}

type Indexer struct {
	lock        sync.Mutex                 // mutex for balance access
	count       int                        // units of chunk/retrieval request
	metaobjects map[string]*IndexerPayload // map for index ,object
	pss         *network.Pss

	vct               *protocols.CodeMap
	vctnotification   *protocols.CodeMap
	proto             *p2p.Protocol
	protonotification *p2p.Protocol
	topic             network.PssTopic
	notificationtopic network.PssTopic
	name              string
	notificationname  string
	version           int
	clientsaddresses  []Address
}

type IndexerPeer struct {
	*protocols.Peer

	pss *network.Pss

	hasProtocol bool
	successC    chan bool
	resultC     chan int
}

const (
	notificationnameconst = "indexer_notification"
	notificationversion   = 1
)

var indexeradrr []byte

func SetIndexerAddress(addr []byte) {

	indexeradrr = addr
}

func waitforws(topic *network.PssTopic) {
	eprecv := "localhost:18547"
	epsend := "localhost:18546"

	successC := make(chan struct{})

	clientrecv, err := rpc.DialWebsocket(context.Background(), "ws://"+eprecv, "ws://localhost")
	if err != nil {
		log.Error("could not connect to recv", "error", err)
		os.Exit(1)
	}
	clientsend, err := rpc.DialWebsocket(context.Background(), "ws://"+epsend, "ws://localhost")
	if err != nil {
		log.Error("could not connect to send", "error", err)
		os.Exit(1)
	}

	//	addr, _ := hex.DecodeString("0b7a432ce59d0b98b33020964a9a248c112950206337f4ff42ae040e9abcf279")
	//topic, _ := network.MakeTopic(name, version)
	log.Trace("clientrecv: %v", clientrecv)
	log.Trace("clientsend: %v", clientsend)
	//log.Trace("rcp oaddr: %x", addr)
	log.Trace("topic: %x", topic)
	ctx := context.Background()
	ch := make(chan string)
	clientrecv.EthSubscribe(ctx, ch, "newMsg", topic)
	go func() {
		select {
		case msg := <-ch:
			log.Trace(fmt.Sprintf("notify!: %v", msg))
		case <-time.NewTimer(time.Second).C:
			log.Trace(fmt.Sprintf("no notifies :'("))
		}
		successC <- struct{}{}
	}()
	//clientsend.Call(&TestResult{}, "eth_sendRaw", addr, topic, []byte("ping"))
	<-successC
}

// //indexer constructor
func NewIndexer() (self *Indexer, err error) {
	self = &Indexer{

		metaobjects:      make(map[string]*IndexerPayload),
		count:            0,
		name:             "indexer",
		notificationname: notificationnameconst,
		version:          notificationversion,
	}
	return self, nil
}

var indexer *Indexer

//
func (self *Indexer) Subscribetometaupdates(n *adapters.NodeId) (err error) {

	fmt.Println("Subscribetometaupdates")

	vct := protocols.NewCodeMap(self.name, uint(self.version), 65535, &IndexerPayload{})
	targetproto := makeIndexerProtocol(self.name, self.version, vct, &IndexerPeer{})

	topic, _ := network.MakeTopic(self.name, self.version)

	addr := network.NewPeerAddrFromNodeId(n)
	pss := makePss(addr.OverlayAddr())

	pssprotocol := network.NewPssProtocol(pss, &topic, vct, targetproto)
	pss.Register(topic, pssprotocol.GetHandler())

	//waitforws(&topic)
	self.pss = pss
	self.proto = targetproto
	self.vct = vct
	self.topic = topic
	indexer = self
	return nil
}

func (self *Indexer) IndexerNotificationSetup() (err error) {

	vct := protocols.NewCodeMap(self.notificationname, uint(self.version), 65535, &IndexerUpdateNotification{})
	//targetproto := makeIndexerProtocol(self.notificationname, self.version, vct, &IndexerPeer{})
	topic, _ := network.MakeTopic(self.notificationname, self.version)
	//pssprotocol := network.NewPssProtocol(self.pss, &topic, vct, targetproto)
	//self.pss.Register(topic, pssprotocol.GetHandler())

	//self.proto = targetproto
	self.vctnotification = vct
	self.notificationtopic = topic
	indexer.notificationtopic = topic
	//self.protonotification = targetproto
	return nil
}

func makeIndexerProtocol(name string, version int, ct *protocols.CodeMap, indexerpeer *IndexerPeer) *p2p.Protocol {
	run := func(p *protocols.Peer) error {
		log.Trace(fmt.Sprintf("running indexer vprotocol on peer %v", p))
		indexerpeer.Peer = p
		p.Register(&IndexerPayload{}, indexerpeer.HandleIndexerPayload)
		err := p.Run()
		return err
	}

	return protocols.NewProtocol(name, uint(version), run, ct, nil, nil)
}

// 1.get the update payload
// 2. parse and get a specific field
// 3. create an index for the specific field (mocked)
// 4. store the index - currently not use use ENS so just update a new index
// 5. Publish the index to the subscribers
func (ptp *IndexerPeer) HandleIndexerPayload(msg interface{}) error {
	pmsg := msg.(*IndexerPayload)
	log.Trace(fmt.Sprintf("HandleIndexerPayload got message %v", pmsg))
	if pmsg.Command == "update" {

		fmt.Println(pmsg.Data)

		data, err := getUpdate(pmsg.Data)
		if err != nil {
			log.Trace(fmt.Sprintf("error in getUpdate %v", err.Error()))
			return err
		}
		name, err := getValue(data, "name")
		fmt.Println("name :" + name)
		index := createIndex(pmsg.Data, "name", name)
		hash, err := storeindex(index)
		index.Hash = hash
		sendUpdateNotification(*index, ptp, "new")
		//ptp.Send(pmsg)
	} else {
		if pmsg.Command == "register" {
			indexer.clientsaddresses = append(indexer.clientsaddresses, Address{pmsg.From})
			fmt.Println("registerfrom :", pmsg.From)
		}
	}

	return nil
}

func getValue(data []byte, key string) (name string, err error) {

	var parsed map[string]interface{}
	err = json.Unmarshal(data, &parsed)
	fmt.Println("getName" + string(data))
	if err != nil {
		fmt.Println("err" + err.Error())
	}
	return parsed[key].(string), nil
}

func getUpdate(hash string) (updateblob []byte, err error) {

	var resp *http.Response
	var respbody []byte
	server_url := "http://localhost:8500/"

	resp, err = http.Get(server_url + "bzzr:/" + hash)

	if err != nil {
		return nil, fmt.Errorf("Request failed: %v", err)
	}
	defer resp.Body.Close()
	respbody, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("Failed to read body: %v", err)
	}

	return respbody, nil
}

func storeindex(index *NewIndex) (hash string, err error) {
	var resp *http.Response
	var respbody []byte
	server_url := "http://localhost:8500/"

	jsResp, err := json.Marshal(index)
	r := bytes.NewReader(jsResp)

	resp, err = http.Post(server_url+"bzzr:/", "application/text", r)

	if err != nil {
		return "", fmt.Errorf("Request failed: %v", err)
	}
	defer resp.Body.Close()
	respbody, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("Failed to read body: %v", err)
	}
	return string(respbody), nil
}

//currently mocked
func createIndex(hash string, key string, Value string) (index *NewIndex) {

	return &NewIndex{Index: hash, Key: key, Value: Value}
}

func sendUpdateNotification(index NewIndex, ptp *IndexerPeer, subtype string) {

	payload := &IndexerUpdateNotification{Index: index, Type: "notification", Subtype: subtype}

	b, _ := json.Marshal(payload)

	pss := indexer.pss //makePss(indexeradrr)

	notificationtopic, _ := network.MakeTopic(notificationnameconst, notificationversion)

	for i := 0; i < len(indexer.clientsaddresses); i++ {
		indexer.pss.Send(indexer.clientsaddresses[i].addr, notificationtopic, b)
	}

	fmt.Println("sendUpdateNotification", "from", pss.GetAddr().OverlayAddr(), payload, indexer.notificationtopic)

}

func makePss(addr []byte) *network.Pss {

	// set up storage
	cachedir, err := ioutil.TempDir("", "pss-cache")
	if err != nil {
		log.Error("create pss cache tmpdir failed", "error", err)
		os.Exit(1)
	}

	dpa, err := storage.NewLocalDPA(cachedir)
	if err != nil {
		log.Error("local dpa creation failed", "error", err)
		os.Exit(1)
	}

	/*chunkerparams := storage.NewChunkerParams()
	storeparams := storage.NewStoreParams(cachedir)
	hash := storage.MakeHashFunc(chunkerParams.Hash)

	self.lstore, err = storage.NewLocalStore(hash, storeparams)
	if err != nil {
		log.Error("localstore creation failed", "error", err)
		os.Exit(1)
	}*/

	kp := network.NewKadParams()
	kp.MinProxBinSize = 3

	pp := network.NewPssParams(dpa)

	overlay := network.NewKademlia(addr, kp)
	ps := network.NewPss(overlay, pp)
	//overlay.Prune(time.Tick(time.Millisecond * 250))
	return ps
}

package meta

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/p2p/protocols"
)

type IndexerPayload struct {
	Data    string
	Command string
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

type Indexer struct {
	lock        sync.Mutex                 // mutex for balance access
	count       int                        // units of chunk/retrieval request
	metaobjects map[string]*IndexerPayload // map for index ,object
	//	ps          *network.Pss
	vct   *protocols.CodeMap
	proto *p2p.Protocol
	//topic       network.PssTopic
	name    string
	version int
}

type IndexerPeer struct {
	*protocols.Peer
}

// //indexer constructor
func NewIndexer() (self *Indexer, err error) {
	self = &Indexer{
		metaobjects: make(map[string]*IndexerPayload),
		count:       0,
		name:        "indexer",
		version:     1,
	}
	return self, nil
}

//
func (self *Indexer) Subscribetometaupdates() (err error) {

	vct := protocols.NewCodeMap(self.name, uint(self.version), 65535, &IndexerPayload{})
	targetproto := makeIndexerProtocol(self.name, self.version, vct, &IndexerPeer{})

	self.proto = targetproto
	self.vct = vct

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

	fmt.Println("sendUpdateNotification", payload)

	// rw1, _ := p2p.MsgPipe()
	// go func() {
	// 	p2p.Send(rw1, 8, [][]byte{{0, 0}})
	// 	rw1.Close()
	// }()

	ptp.Send(payload)

	fmt.Println("sendUpdateNotification done-")

}

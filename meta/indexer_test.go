package meta

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/p2p/adapters"
	"github.com/ethereum/go-ethereum/p2p/protocols"
	p2ptest "github.com/ethereum/go-ethereum/p2p/testing"
	"github.com/ethereum/go-ethereum/swarm/network"
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
func TestSendToNode(t *testing.T) {
	fmt.Println("TestIndexer")
	indexer, _ := NewIndexer()
	indexer.Subscribetometaupdates()

	peerid := "b4287791639128495e865a47ee49c8431e2d9dddc9bcea0b8a70a199a78382980cbaeb99fac9c1fb18ae0f261c5106e11a89c3cff74dc560de7d82c162423e68"
	peeridhex, _ := hex.DecodeString(peerid)
	nid := adapters.NewNodeId(peeridhex)
	fmt.Println(nid.NodeID)
	peer := p2p.NewPeer(nid.NodeID, adapters.Name(nid.Bytes()), []p2p.Cap{})
	fmt.Println(peer.ID())

	payload, err := createIndexerPayload()
	if err != nil {
		t.Fatal(err)
	}

	wr, _ := p2p.MsgPipe()

	protocols.NewPeer(peer, indexer.vct, wr).Send(payload)
}
func TestIndexer(t *testing.T) {
	fmt.Println("TestIndexer")
	indexer, _ := NewIndexer()
	indexer.Subscribetometaupdates()

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

// func subscribetoIndexerNotifications(t *testing.T) (err error) {
//
// 	vct := protocols.NewCodeMap("indexer", uint(1), 65535, &IndexerUpdateNotification{})
// 	targetproto := makeIndexerNotificationsProtocol("indexer", 1, vct, &IndexerPeer{})
// 	addr := network.RandomAddr()
// 	pt := p2ptest.NewProtocolTester(t, network.NodeId(addr), 2, targetproto.Run)
//
// 	code, found := vct.GetCode(&IndexerUpdateNotification{})
// 	if found == false {
// 		fmt.Println("not found")
// 		return
// 	}
//
// 	for _, id := range pt.Ids {
// 		pt.TestExchanges(
// 			p2ptest.Exchange{
// 				Expects: []p2ptest.Expect{
// 					p2ptest.Expect{
// 						Code: code,
// 						Msg:  &IndexerUpdateNotification{},
// 						Peer: id,
// 					},
// 				},
// 			},
// 		)
// 	}
// 	time.Sleep(time.Second * 5)
//
// 	return nil
// }
// func makeIndexerNotificationsProtocol(name string, version int, ct *protocols.CodeMap, indexerpeer *IndexerPeer) *p2p.Protocol {
// 	run := func(p *protocols.Peer) error {
// 		log.Trace(fmt.Sprintf("running  vprotocol on peer %v", p))
//
// 		indexerpeer.Peer = p
// 		p.Register(&IndexerUpdateNotification{}, indexerpeer.HandleIndexerNotificationsPayload)
// 		err := p.Run()
// 		return err
// 	}
//
// 	return protocols.NewProtocol(name, uint(version), run, ct, nil, nil)
// }
//
// func (ptp *IndexerPeer) HandleIndexerNotificationsPayload(msg interface{}) error {
// 	pmsg := msg.(*IndexerUpdateNotification)
// 	log.Trace(fmt.Sprintf("HandleIndexerNotificationsPayload got message %v", pmsg))
// 	//ptp.Send(pmsg)
//
// 	return nil
// }

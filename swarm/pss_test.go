package swarm

import (
	"context"
	//"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"
	
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/swarm/network"
	"github.com/ethereum/go-ethereum/rpc"
	
)

func init() {
	h := log.CallerFileHandler(log.StreamHandler(os.Stderr, log.TerminalFormat(true)))
	log.Root().SetHandler(h)
}

type TestResult struct {
	Foo string `json:"foo"`
}

func TestDialWS(t *testing.T) {
	ep := "localhost:8546"
	
	client, err := rpc.DialWebsocket(context.Background(), "ws://" + ep, "ws://localhost")
	if err != nil {
		t.Fatalf("could not connect: %v", err)
	} else {
		addr := network.RandomAddr()
		log.Trace("client: %v", client)
		ctx := context.Background()
		ch := make(chan string)
		client.Call(&TestResult{}, "eth_sendRaw", addr.OverlayAddr(), "foo", 42, []byte("pong"))
		client.EthSubscribe(ctx, ch, "newMsg")
		select {
			case msg := <- ch:
				log.Trace(fmt.Sprintf("notify!: %v", msg))
			case <- time.NewTimer(time.Second).C:
				log.Trace(fmt.Sprintf("no notifies :'("))
		}
	}
	
}


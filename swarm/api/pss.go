package api

import (
	"context"
	"fmt"
	
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/ethereum/go-ethereum/swarm/network"
)

type PssApi struct {
	*network.Pss
}

func NewPssApi(ps *network.Pss) *PssApi {
	return &PssApi{Pss: ps}
}

func (self *PssApi) NewMsg(ctx context.Context) (*rpc.Subscription, error) {
	notifier, supported := rpc.NotifierFromContext(ctx)
	if !supported {
		log.Error("subscribe not supported")
	}
	
	sub := notifier.CreateSubscription()
	
	go func() {
		// to notify use:
		// notifier.Notify(sub.ID, "ping"); err != nil {
		return
	}()
	
	return sub, nil
}

func (self *PssApi) SendRaw(to []byte, name string, version int, msg []byte) error {
	topic, _ := network.MakeTopic(name, version)
	err := self.Pss.Send(to, topic, msg)
	if err != nil {
		return fmt.Errorf("send error: %v", err)
	}
	return fmt.Errorf("ok sent")
}

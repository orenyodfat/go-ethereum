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

func (self *PssApi) NewMsg(ctx context.Context, topic network.PssTopic) (*rpc.Subscription, error) {
	notifier, supported := rpc.NotifierFromContext(ctx)
	if !supported {
		return nil, fmt.Errorf("Subscribe not supported")
	}
	
	sub := notifier.CreateSubscription()
	
	ch := make(chan []byte)
	psssub, err := self.Pss.Subscribe(&topic, ch)
	if err != nil {
		return nil, fmt.Errorf("pss subscription topic %v (rpc sub id %v) failed: %v", topic, sub.ID, err)
	}
		
	go func(topic network.PssTopic) {
		for {
			select {
				case msg := <-ch:
					if err := notifier.Notify(sub.ID, msg); err != nil {
						log.Warn(fmt.Sprintf("notification on pss sub topic %v rpc (sub %v) msg %v failed!", topic, sub.ID, msg))
					}
				case err := <-psssub.Err():
					log.Warn(fmt.Sprintf("caught subscription error in pss sub topic: %v", topic, err))
					return
				case <- notifier.Closed():
					log.Warn(fmt.Sprintf("rpc sub notifier closed"))
					psssub.Unsubscribe()
					return
				case err := <- sub.Err():
					log.Warn(fmt.Sprintf("rpc sub closed: %v", err))
					psssub.Unsubscribe()
					return
			}
		}
		return
	}(topic)
	
	return sub, nil
}

func (self *PssApi) SendRaw(to []byte, topic network.PssTopic, msg []byte) error {
	err := self.Pss.Send(to, topic, msg)
	if err != nil {
		return fmt.Errorf("send error: %v", err)
	}
	return fmt.Errorf("ok sent")
}

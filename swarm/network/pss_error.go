package network

import (
	"fmt"
)

const (
	pssError_NoForwarder = iota
	pssError_ForwardToSelf
	pssError_BlockByCache
)

var pssErrorToString = map[int]string{
	pssError_NoForwarder:   "no available forwarders in routing table",
	pssError_ForwardToSelf: "forward to self",
	pssError_BlockByCache:  "message found in blocking cache",
}

type pssError struct {
	ErrorCode int
}

func newPssError(code uint) *pssError {
	return &pssError{ErrorCode: int(code)}
}

func (self *pssError) Error() string {
	if pssErrorToString[self.ErrorCode] == "" {
		return fmt.Sprintf("unknown error code '%d'", self.ErrorCode)
	}
	return pssErrorToString[self.ErrorCode]
}

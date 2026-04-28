package mab

import (
	"sync"
)

// ContextStorage is a temporary cache to store the context (state)
// used for a specific request ID until the request completes.
// It is needed to update context LinUCB
type ContextStorage struct {
	// sync.Map is safe for concurrent use. So we use it since the LB can handle multiple request simultaneously.
	data sync.Map
}

var GlobalContextStorage = &ContextStorage{}

func (s *ContextStorage) Store(reqID string, ctx *Context) {
	s.data.Store(reqID, ctx)
}

func (s *ContextStorage) RetrieveAndDelete(reqID string) *Context {
	val, ok := s.data.LoadAndDelete(reqID)
	if !ok {
		return nil
	}
	return val.(*Context)
}

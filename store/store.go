package store

import (
	"context"
	"fmt"
	"sync"
)

type Store interface {
	Get(context.Context, string) (string, error)
	Set(context.Context, string, string) error
	Delete(context.Context, string) error
}

type MemStore struct {
	data map[string]string
	lock *sync.Mutex
}

type NotFoundError struct {
	Key string
}

func (e NotFoundError) Error() string {
	return fmt.Sprintf("key %s was not found", string(e.Key))
}

func (s MemStore) Get(ctx context.Context, key string) (string, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	val, ok := s.data[string(key)]
	if !ok {
		return "", NotFoundError{Key: key}
	}
	return val, nil
}

func (s MemStore) Set(ctx context.Context, key string, val string) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.data[string(key)] = string(val)
	return nil
}

func (s MemStore) Delete(ctx context.Context, key string) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	delete(s.data, string(key))
	return nil
}

func NewMemStore() Store {
	var lock sync.Mutex
	return MemStore{
		data: make(map[string]string),
		lock: &lock,
	}
}

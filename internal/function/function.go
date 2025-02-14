package function

import (
	"encoding/json"
	"fmt"

	"time"

	"github.com/serverledge-faas/serverledge/internal/cache"
	"github.com/serverledge-faas/serverledge/utils"
	clientv3 "go.etcd.io/etcd/client/v3"
	"golang.org/x/net/context"
)

// Function describes a serverless function.
type Function struct {
	Name            string
	Runtime         string  // example: python310
	MemoryMB        int64   // MB
	CPUDemand       float64 // 1.0 -> 1 core
	Handler         string  // example: "module.function_name"
	TarFunctionCode string  // input is .tar
	CustomImage     string  // used if custom runtime is chosen
	Signature       *Signature
}

func (f *Function) getEtcdKey() string {
	return getEtcdKey(f.Name)
}

func getEtcdKey(funcName string) string {
	return fmt.Sprintf("/function/%s", funcName)
}

// GetFunction retrieves a Function given its name. If it doesn't exist, returns false
func GetFunction(name string) (*Function, bool) {

	val, found := getFromCache(name)
	if !found {
		// cache miss
		f, response := getFromEtcd(name)
		if !response {
			return nil, false
		}
		//insert a new element to the cache
		cache.GetCacheInstance().Set(name, f, cache.DefaultExp)
		return f, true
	}

	return val, true

}

func (f *Function) String() string {
	return f.Name
}

func getFromCache(name string) (*Function, bool) {
	localCache := cache.GetCacheInstance()
	f, found := localCache.Get(name)
	if !found {
		return nil, false
	}
	//cache hit
	//return a safe copy of the function previously obtained
	function := *f.(*Function)
	return &function, true

}

func getFromEtcd(name string) (*Function, bool) {
	cli, err := utils.GetEtcdClient()
	if err != nil {
		return nil, false
	}
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	getResponse, err := cli.Get(ctx, getEtcdKey(name))
	if err != nil || len(getResponse.Kvs) < 1 {
		return nil, false
	}

	var f Function
	err = json.Unmarshal(getResponse.Kvs[0].Value, &f)
	if err != nil {
		return nil, false
	}

	return &f, true
}

// SaveToEtcd registers the function to Etcd
func (f *Function) SaveToEtcd() error {
	cli, err := utils.GetEtcdClient()
	if err != nil {
		return err
	}
	ctx := context.TODO()

	payload, err := json.Marshal(*f)
	if err != nil {
		return fmt.Errorf("Could not marshal function: %v", err)
	}
	_, err = cli.Put(ctx, f.getEtcdKey(), string(payload))
	if err != nil {
		return fmt.Errorf("Failed Put: %v", err)
	}

	// Add the function to the local cache
	cache.GetCacheInstance().Set(f.Name, f, cache.DefaultExp)

	return nil
}

// Delete removes a function from Etcd and the local cache.
func (f *Function) Delete() error {
	cli, err := utils.GetEtcdClient()
	if err != nil {
		return err
	}
	ctx := context.TODO()

	dresp, err := cli.Delete(ctx, f.getEtcdKey())
	if err != nil {
		return fmt.Errorf("Failed Delete: %v", err)
	} else if dresp.Deleted != 1 {
		fmt.Printf("no function with key '%s' exists", f.getEtcdKey())
	}

	// Remove the function from the local cache
	cache.GetCacheInstance().Delete(f.Name)

	return nil
}

func (f *Function) Equals(f2 *Function) bool {
	return (f == nil && f2 == nil) || (f.Name == f2.Name &&
		f.CustomImage == f2.CustomImage &&
		f.CPUDemand == f2.CPUDemand &&
		f.Runtime == f2.Runtime &&
		f.Handler == f2.Handler &&
		f.MemoryMB == f2.MemoryMB &&
		f.TarFunctionCode == f2.TarFunctionCode)
}

// Exists checks if the function is already saved to Etcd
func (f *Function) Exists() bool {
	savedFunction, ok := GetFunction(f.Name)
	return ok && f.Equals(savedFunction)
}

// GetAll returns all function names
func GetAll() ([]string, error) {
	return GetAllWithPrefix("/function")
}

// GetAllWithPrefix is used to get all /function or /workflow currently registered in etcd
func GetAllWithPrefix(prefix string) ([]string, error) {
	cli, err := utils.GetEtcdClient()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.TODO(), 10*time.Second)
	defer cancel()

	resp, err := cli.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	functions := make([]string, len(resp.Kvs))
	for i, s := range resp.Kvs {
		functions[i] = string(s.Key)[len(prefix+"/"):]
	}

	return functions, ctx.Err()
}

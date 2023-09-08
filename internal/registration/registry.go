package registration

import (
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/serverledge-faas/serverledge/internal/config"
	"github.com/serverledge-faas/serverledge/utils"
	"github.com/lithammer/shortuuid"
	_ "go.etcd.io/etcd/client/v3"
	clientv3 "go.etcd.io/etcd/client/v3"
	"golang.org/x/net/context"
)

var BASEDIR = "registry"
var TTL = config.GetInt(config.REGISTRATION_TTL, 20) // lease time in Seconds

// getEtcdKey append to a given unique id the logical path depending on the Area.
// If it is called with  an empty string  it returns the base path for the current local Area.
func (r *Registry) getEtcdKey(id string) (key string) {
	return fmt.Sprintf("%s/%s/%s", BASEDIR, r.Area, id)
}

// RegisterToEtcd make a registration to the local Area; etcd put operation is performed
func (r *Registry) RegisterToEtcd(hostport string) (string, error) {
	etcdClient, err := utils.GetEtcdClient()
	if err != nil {
		log.Fatal(UnavailableClientErr)
		return "", UnavailableClientErr
	}

	ctx, _ := context.WithTimeout(context.Background(), 1*time.Second)
	//generate unique identifier
	id := shortuuid.New() + strconv.FormatInt(time.Now().UnixNano(), 10)
	r.Key = r.getEtcdKey(id)
	resp, err := etcdClient.Grant(ctx, int64(TTL))
	if err != nil {
		log.Fatal(err)
		return "", err
	}

	log.Printf("Registration key: %s\n", r.Key)
	// save couple (id, hostport) to the correct Area-dir on etcd
	_, err = etcdClient.Put(ctx, r.Key, hostport, clientv3.WithLease(resp.ID))
	if err != nil {
		log.Fatal(IdRegistrationErr)
		return "", IdRegistrationErr
	}

	cancelCtx, _ := context.WithCancel(etcdClient.Ctx())

	// the key id will be kept alive until a fault will occur
	keepAliveCh, err := etcdClient.KeepAlive(cancelCtx, resp.ID)
	if err != nil || keepAliveCh == nil {
		log.Fatal(KeepAliveErr)
		return "", KeepAliveErr
	}

	go func() {
		for range keepAliveCh {
			// eat messages until keep alive channel closes
			//log.Println(alive.ID)
		}
	}()

	return r.Key, nil
}

// GetAll is used to obtain the list of  other server's addresses under a specific local Area
func (r *Registry) GetAll(remotes bool) (map[string]string, error) {
	var baseDir string
	if remotes {
		baseDir = fmt.Sprintf("%s/%s/%s/", BASEDIR, "cloud", r.Area)
	} else {
		baseDir = r.getEtcdKey("")
	}
	ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)
	etcdClient, err := utils.GetEtcdClient()
	if err != nil {
		log.Fatal(UnavailableClientErr)
		return nil, UnavailableClientErr
	}
	//retrieve all url of the other servers under my Area
	resp, err := etcdClient.Get(ctx, baseDir, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("Could not read from etcd: %v", err)
	}

	servers := make(map[string]string)
	for _, s := range resp.Kvs {
		servers[string(s.Key)] = string(s.Value)
		//audit todo delete the next line
		if remotes {
			log.Printf("found remote server at: %s\n", servers[string(s.Key)])
		} else {
			log.Printf("found edge server at: %s\n", servers[string(s.Key)])
		}
	}

	return servers, nil
}

// GetCloudNodes retrieves the list of Cloud servers in a given region
func GetCloudNodes(region string) (map[string]string, error) {
	baseDir := fmt.Sprintf("%s/%s/%s/", BASEDIR, "cloud", region)
	ctx, _ := context.WithTimeout(context.Background(), 1*time.Second)
	etcdClient, err := utils.GetEtcdClient()
	if err != nil {
		log.Fatal(UnavailableClientErr)
		return nil, UnavailableClientErr
	}

	resp, err := etcdClient.Get(ctx, baseDir, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("Could not read from etcd: %v", err)
	}

	servers := make(map[string]string)
	for _, s := range resp.Kvs {
		servers[string(s.Key)] = string(s.Value)
	}

	return servers, nil
}

// GetCloudNodesInRegion retrieves the list of Cloud servers in a given region
func GetCloudNodesInRegion(region string) (map[string]string, error) {
	baseDir := fmt.Sprintf("%s/%s/%s/", BASEDIR, "cloud", region)
	ctx, _ := context.WithTimeout(context.Background(), 1*time.Second)
	etcdClient, err := utils.GetEtcdClient()
	if err != nil {
		log.Fatal(UnavailableClientErr)
		return nil, UnavailableClientErr
	}

	resp, err := etcdClient.Get(ctx, baseDir, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("Could not read from etcd: %v", err)
	}

	servers := make(map[string]string)
	for _, s := range resp.Kvs {
		servers[string(s.Key)] = string(s.Value)
	}

	return servers, nil
}

// Deregister deletes from etcd the key, value pair previously inserted
func (r *Registry) Deregister() (e error) {
	etcdClient, err := utils.GetEtcdClient()
	if err != nil {
		log.Fatal(UnavailableClientErr)
		return UnavailableClientErr
	}

	ctx, _ := context.WithTimeout(context.Background(), 1*time.Second)
	_, err = etcdClient.Delete(ctx, r.Key)
	if err != nil {
		return err
	}

	log.Println("Deregister : " + r.Key)
	return nil
}

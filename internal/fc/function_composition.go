package fc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cornelk/hashmap"
	"github.com/grussorusso/serverledge/internal/cache"
	"github.com/grussorusso/serverledge/internal/function"
	"github.com/grussorusso/serverledge/internal/types"
	"github.com/grussorusso/serverledge/utils"
	"github.com/labstack/gommon/log"
	"golang.org/x/exp/slices"
	"time"
)

// FunctionComposition is a serverless Function Composition
type FunctionComposition struct {
	Name               string // al posto del nome potrebbe essere un id da mettere in etcd
	Functions          map[string]*function.Function
	Workflow           Dag
	RemoveFnOnDeletion bool
}

type ExecutionReportId string

func CreateExecutionReportId(dagNode DagNode) ExecutionReportId {
	return ExecutionReportId(printType(dagNode.GetNodeType()) + "_" + string(dagNode.GetId()))
}

type CompositionExecutionReport struct {
	Result       map[string]interface{}
	Reports      *hashmap.Map[ExecutionReportId, *function.ExecutionReport]
	ResponseTime float64   // time waited by the user to get the output of the entire composition
	Progress     *Progress `json:"-"` // skipped in Json marshaling
}

func (cer *CompositionExecutionReport) GetSingleResult() string {
	if len(cer.Result) == 1 {
		for _, value := range cer.Result {
			return fmt.Sprintf("%v", value)
		}
	}
	return fmt.Sprintf("%v", cer.Result)
}

func (cer *CompositionExecutionReport) GetAllResults() string {
	result := "[\n"
	for _, value := range cer.Result {
		result += fmt.Sprintf("\t%v\n", value)
	}
	result += "]\n"
	return result
}

// NewFC instantiates a new FunctionComposition with a name and a corresponding dag. Function can contain duplicate functions (with the same name)
func NewFC(name string, dag Dag, functions []*function.Function, removeFnOnDeletion bool) FunctionComposition {
	functionMap := make(map[string]*function.Function)
	if functions != nil {
		for _, f := range functions {
			// if the function is already added, simply replace it
			functionMap[f.Name] = f
		}
	}

	return FunctionComposition{
		Name:               name,
		Functions:          functionMap,
		Workflow:           dag,
		RemoveFnOnDeletion: removeFnOnDeletion,
	}
}

func (fc *FunctionComposition) getEtcdKey() string {
	return getEtcdKey(fc.Name)
}

func getEtcdKey(fc string) string {
	return fmt.Sprintf("/fc/%s", fc)
}

// GetAllFC returns the function composition names
func GetAllFC() ([]string, error) {
	return function.GetAllWithPrefix("/fc")
}

func getFCFromCache(name string) (*FunctionComposition, bool) {
	localCache := cache.GetCacheInstance()
	cachedObj, found := localCache.Get(name)
	if !found {
		return nil, false
	}
	//cache hit
	//return a safe copy of the function composition previously obtained
	fc := *cachedObj.(*FunctionComposition)
	return &fc, true
}

func getFCFromEtcd(name string) (*FunctionComposition, error) {
	cli, err := utils.GetEtcdClient()
	if err != nil {
		return nil, errors.New("failed to connect to ETCD")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	key := getEtcdKey(name)
	getResponse, err := cli.Get(ctx, key)
	if err != nil || len(getResponse.Kvs) < 1 {
		return nil, fmt.Errorf("failed to retrieve value for key %s", key)
	}

	var f FunctionComposition
	err = json.Unmarshal(getResponse.Kvs[0].Value, &f)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal json: %v", err)
	}

	return &f, nil
}

// GetFC gets the FunctionComposition from cache or from ETCD
func GetFC(name string) (*FunctionComposition, bool) {
	val, found := getFCFromCache(name)
	if !found {
		// cache miss
		f, err := getFCFromEtcd(name)
		if err != nil {
			return nil, false
		}
		//insert a new element to the cache
		cache.GetCacheInstance().Set(name, f, cache.DefaultExp)
		return f, true
	}

	return val, true
}

// SaveToEtcd creates and register the function composition in Serverledge
// It is like SaveToEtcd for a simple function
func (fc *FunctionComposition) SaveToEtcd() error {
	cli, err := utils.GetEtcdClient()
	if err != nil {
		return err
	}
	ctx := context.TODO()

	// Save all functions in the dag to ETCD
	// funcs := make([]*function.Function, 0)
	for _, fName := range fc.Workflow.GetUniqueDagFunctions() {
		_, exists := function.GetFunction(fName)
		if !exists {
			errSave := fc.Functions[fName].SaveToEtcd()
			if errSave != nil {
				return fmt.Errorf("failed to save function %s: %v", fName, errSave)
			}
		}
		// funcs = append(funcs, f)
	}

	// marshal the function composition object into json
	payload, err := json.Marshal(*fc)
	if err != nil {
		return fmt.Errorf("could not marshal function composition: %v", err)
	}
	// saves the json object into etcd
	_, err = cli.Put(ctx, fc.getEtcdKey(), string(payload))
	if err != nil {
		return fmt.Errorf("failed etcd Put: %v", err)
	}

	// Add the function composition to the local cache
	cache.GetCacheInstance().Set(fc.Name, fc, cache.DefaultExp)

	return nil
}

// Invoke schedules each function of the composition and invokes them
func (fc *FunctionComposition) Invoke(r *CompositionRequest) (CompositionExecutionReport, error) {
	requestId := ReqId(r.ReqId)
	input := r.Params

	// initialize struct progress from dag
	progress := InitProgressRecursive(requestId, &fc.Workflow)
	// initialize partial data cache
	// partialDataCache.InitNewRequest(requestId)
	// initialize partial data with input, directly from the Start.Next node
	pd := NewPartialData(requestId, fc.Workflow.Start.Next, "nil", input)
	pd.Data = input
	// saving partial data and progress to cache
	err := SavePartialData(pd, cache.Persist)
	if err != nil {
		return CompositionExecutionReport{Result: nil}, fmt.Errorf("failed to save partial data %v", err)
	}
	err = SaveProgress(progress, cache.Persist)
	if err != nil {
		return CompositionExecutionReport{Result: nil}, fmt.Errorf("failed to save progress: %v", err)
	}

	shouldContinue := true
	for shouldContinue {
		// executing dag
		shouldContinue, err = fc.Workflow.Execute(r)
		if err != nil {
			progress.Print()
			return CompositionExecutionReport{Result: nil, Progress: progress}, fmt.Errorf("failed dag execution: %v", err)
		}
	}
	// retrieving output of  execution
	result, err := RetrieveSinglePartialData(requestId, fc.Workflow.End.GetId(), cache.Persist)
	if err != nil {
		return CompositionExecutionReport{Result: nil, Progress: progress}, fmt.Errorf("failed to retrieve composition result (partial data) %v", err)
	}

	// deleting progresses and partial datas from cache and etcd
	err = DeleteProgress(requestId, cache.Persist)
	if err != nil {
		return CompositionExecutionReport{}, err
	}
	_, errDel := DeleteAllPartialData(requestId, cache.Persist)
	if errDel != nil {
		return CompositionExecutionReport{}, errDel
	}
	// fmt.Printf("Succesfully deleted %d partial datas and progress for request %s\n", removed, requestId)
	r.ExecReport.Result = result.Data
	//progress.NextGroup = -1
	//r.ExecReport.Progress = progress
	return r.ExecReport, nil
}

// Delete removes the FunctionComposition from cache and from etcd, so it cannot be invoked anymore
func (fc *FunctionComposition) Delete() error {
	cli, err := utils.GetEtcdClient()
	if err != nil {
		return err
	}
	ctx := context.TODO()
	if fc.RemoveFnOnDeletion {
		for _, f := range fc.Functions {
			err = f.Delete()
			if err != nil {
				fmt.Printf("failed to delete function %s associated to function composition %s: %v", f.Name, fc.Name, err)
			}
		}
	}

	dresp, err := cli.Delete(ctx, fc.getEtcdKey())
	if err != nil || dresp.Deleted != 1 {
		return fmt.Errorf("failed Delete: %v", err)
	}

	// Remove the function from the local cache
	cache.GetCacheInstance().Delete(fc.Name)

	return nil
}

// DeleteAll deletes the function composition from Etcd and the Functions associated with it
func (fc *FunctionComposition) DeleteAll() error {
	err := fc.Delete()

	for _, fName := range fc.Workflow.GetUniqueDagFunctions() {
		f, exists := function.GetFunction(fName)
		if !exists {
			return fmt.Errorf("funtion %s does not exist", fName)
		}
		err1 := f.Delete()
		if err1 != nil {
			return fmt.Errorf("the deletion of the function %s has failed", f.Name)
		}
	}

	return err
}

// Exists return true if the function composition exists either in etcd or in cache. If it only exists in Etcd, it saves the composition also in caches
func (fc *FunctionComposition) Exists() bool {
	_, found := getFCFromCache(fc.Name)
	if !found {
		// cache miss
		f, err := getFCFromEtcd(fc.Name)
		if err.Error() == fmt.Sprintf("failed to retrieve value for key %s", getEtcdKey(fc.Name)) {
			return false
		} else if err != nil {
			log.Error(err.Error())
			return false
		}
		//insert a new element to the cache
		cache.GetCacheInstance().Set(f.Name, f, cache.DefaultExp)
		return true
	}
	return found
}

// Equals is used in tests to check function composition equality
func (fc *FunctionComposition) Equals(cmp types.Comparable) bool {
	fc2 := cmp.(*FunctionComposition)
	if fc.Name != fc2.Name {
		return false
	}
	funcs1 := fc.Workflow.GetUniqueDagFunctions()
	funcs2 := fc2.Workflow.GetUniqueDagFunctions()
	if !slices.Equal(funcs1, funcs2) {
		return false
	}
	if !fc.Workflow.Equals(&fc2.Workflow) {
		return false
	}
	return true
}

func (fc *FunctionComposition) String() string {
	functions := "["
	i := 0
	for name, _ := range fc.Functions {
		functions += name
		if i < len(fc.Functions)-1 {
			functions += ", "
		}
		i++
	}
	functions += "]"
	workflow := fc.Workflow.String()
	return fmt.Sprintf(`FunctionComposition{
		Name: %s,
		Functions: %s,
		Workflow:\n%s,
		RemoveFnOnDeletion: %t
	}`, fc.Name, functions, workflow, fc.RemoveFnOnDeletion)
}

// MarshalJSON for CompositionExecutionReport is necessary as the hashmap cannot be directly marshaled
func (cer CompositionExecutionReport) MarshalJSON() ([]byte, error) {
	// Create a map to hold the JSON representation of the FunctionComposition
	data := make(map[string]interface{})
	data["Result"] = cer.Result // al posto del nome potrebbe essere un id da mettere in etcd
	data["ResponseTime"] = cer.ResponseTime

	reports := make(map[ExecutionReportId]*function.ExecutionReport)

	cer.Reports.Range(func(id ExecutionReportId, report *function.ExecutionReport) bool {
		reports[id] = report
		return true
	})
	data["Reports"] = reports

	return json.Marshal(data)
}

func (cer CompositionExecutionReport) UnmarshalJSON(data []byte) error {
	var tempMap map[string]json.RawMessage
	if err := json.Unmarshal(data, &tempMap); err != nil {
		return err
	}

	if rawResult, ok := tempMap["Result"]; ok {
		if err := json.Unmarshal(rawResult, &cer.Result); err != nil {
			return err
		}
	} else {
		return fmt.Errorf("missing 'Result' field in JSON")
	}

	if rawResponseTime, ok := tempMap["ResponseTime"]; ok {
		if err := json.Unmarshal(rawResponseTime, &cer.ResponseTime); err != nil {
			return err
		}
	} else {
		return fmt.Errorf("missing 'ResponseTime' field in JSON")
	}

	//if rawProgress, ok := tempMap["Progress"]; ok {
	//	if err := json.Unmarshal(rawProgress, &cer.Progress); err != nil {
	//		return err
	//	}
	//} else {
	//	return fmt.Errorf("missing 'Progress' field in JSON")
	//}
	var tempReportsMap map[string]json.RawMessage
	if err := json.Unmarshal(tempMap["Reports"], &tempReportsMap); err != nil {
		return err
	}
	cer.Reports = hashmap.New[ExecutionReportId, *function.ExecutionReport]()
	for id, execReport := range tempReportsMap {
		var execReportVar function.ExecutionReport
		err := json.Unmarshal(execReport, &execReportVar)
		if err != nil {
			return err
		}
		cer.Reports.Set(ExecutionReportId(id), &execReportVar)
	}
	return nil
}

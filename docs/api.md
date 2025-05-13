## Serverledge API Reference


<!--

<details>
 <summary><code>POST</code> <code><b>/create</b></code> <code>(registers a new function)</code></summary>
 Details
</details>
-->


### Registering a new function

 <code>POST</code> <code><b>/create</b></code> (registers a new function)

##### Parameters

> | name      |  required   | type               | description                                                           |
> |-----------|-------------|-------------------------|------------|
> | `Name`    |         yes | string  | Name of the function (globally unique)  |
> | `Runtime`         | yes | string  | Base container runtime (e.g., `python310`)
> | `MemoryMB`        | yes | int     | Memory (in MB) reserved for each function instance
> | `CPUDemand`       |     | float   | Max CPU cores (or fractions of) allocated to function instances (e.g., `1.0` means up to 1 core, `-1.0` means no cap)
> | `Handler`         | (yes)    | string  | Function entrypoint in the source package; syntax and semantics depend on the chosen runtime (e.g., `module.function_name`). Not needed if `Runtime` is `custom`
> | `TarFunctionCode` | (yes)    | string  | Source code package as a base64-encoded TAR archive. Not needed if `Runtime` is `custom`
> | `CustomImage`     |     | string  | If `Runtime` is `custom`: custom container image to use


##### Responses

> | http code     | content-type                      | response                        | comments                                    |
> |---------------|-----------------------------------|---------------------------------|-----------------------------------|
> | `200`         | `application/json`        | `{ "Created": "function_name" }`    |                            |
> | `404`         | `text/plain`              | `Invalid runtime.` |    Chosen `Runtime` does not exist      |
> | `409`         | `text/plain`              |  |    Function already exists                        |
> | `503`         | `text/plain`              |  |    Creation failed                        |


### Updating a function

 <code>POST</code> <code><b>/update</b></code> (registers a new function or updates one)

See `/create` above. The only difference is that `/update` does not return any
error if a function with the same name already exists. 



------------------------------------------------------------------------------------------
### Deleting a function

 <code>POST</code> <code><b>/delete</b></code> (deletes an existing function)

##### Parameters

> | name      |  required   | type               | description                                                           |
> |-----------|-------------|-------------------------|------------|
> | `Name`    |         yes | string  | Name of the function  |


##### Responses

> | http code     | content-type                      | response                        | comments                                    |
> |---------------|-----------------------------------|---------------------------------|-----------------------------------|
> | `200`         | `application/json`        | `{ "Deleted": "function_name" }`    |                            |
> | `404`         | `text/plain`              | `Unknown function.` |    The function does not exist      |
> | `503`         | `text/plain`              |  |    Creation failed                        |



------------------------------------------------------------------------------------------

### Invoking a function

 <code>POST</code> <code><b>/invoke/<func></b></code> (invokes function `<func>`)

##### Parameters

> | name      |  required   | type               | description                                                           |
> |-----------|-------------|-------------------------|------------|
> | `Params`          | yes | dict    | Key-value specification of invocation parameters  |
> | `CanDoOffloading` |     | bool    | Whether the request can be offloaded (default: true)  |
> | `Async`           |     | bool    | Whether the invocation is asynchronous (default: false)  |
> | `QoSClass`        |     | int     | ID of the QoS class for the request     |
> | `QoSMaxRespT`     |     | float   | Desired max response time  |
> | `ReturnOutput`    |     | bool    | Whether function std. output and error should be collected (if supported by the function runtime)  |


##### Responses

> | http code     | content-type                      | response                        | comments                                    |
> |---------------|-----------------------------------|---------------------------------|-----------------------------------|
> | `200`         | `application/json`        | *See below.*    |                            |
> | `404`         | `text/plain`              | `Function unknown.` |          |
> | `429`         | `text/plain`              |  | Not served because of excessive load.         |
> | `500`         | `text/plain`              |  |    Invocation failed.                        |

An example response for a successful **synchronous** request:
	
	{
	    "Success": true,
	    "Result": "{\"IsPrime\": false}",
	    "ResponseTime": 0.712851098,
	    "IsWarmStart": false,
	    "InitTime": 0.709491144,
	    "OffloadLatency": 0,
	    "Duration": 0.003351790000000021,
	    "SchedAction": ""
	}

`Result` contains the object returned by the function upon completion.
The other fields provide lower-level information. For instance, `Duration`
reports the execution time of the function (in seconds), excluding all the
communication and initialization overheads. `IsWarmStart` indicates whether
a warm container has been used for the request.


An example response for a successful **asynchronous** request:

	{
		"ReqId": "isprime-98330239242748"
	}

`ReqId` can be used later to poll the execution results.

------------------------------------------------------------------------------------------
### Polling for the results of an async request

 <code>GET</code> <code><b>/poll/<reqId></b></code> (polls the results of `<reqId>`)

##### Parameters

`<reqId>` is the request identifier, as returned by `/invoke`.

##### Responses

> | http code     | content-type                      | response                        | comments                                    |
> |---------------|-----------------------------------|---------------------------------|-----------------------------------|
> | `200`         | `application/json`        | *See response to synchronous requests.*    |                            |
> | `404`         | `text/plain`              | |   Results not found.        |
> | `500`         | `text/plain`              | `Could not retrieve results` |    
> | `500`         | `text/plain`              | `Failed to connect to Global Registry` |    

------------------------------------------------------------------------------------------
### Prewarming a function

 <code>POST</code> <code><b>/prewarm</b></code> (prewarms instances for a function)

##### Parameters

> | name      |  required   | type               | description                                                           |
> |-----------|-------------|-------------------------|------------|
> | `Function`    |         yes | string  | Name of the function  |
> | `Instances`   |         yes | int  | Instances to spawn (0 to only pull the image)|
> | `ForceImagePull`  |             | bool  | Always check for image updates, even if a local copy exists  |


##### Responses

> | http code     | content-type                      | response                        | comments                                    |
> |---------------|-----------------------------------|---------------------------------|-----------------------------------|
> | `200`         | `application/json`        | `{ "Prewarmed": N }`    |  The number of prewarmed instances is returned. **It might be less than `Instances`** due to resource shortage. 
> | `404`         | `text/plain`              | `Unknown function.` |    The function does not exist      |
> | `503`         | `text/plain`              |  |    Prewarming failed                        |

------------------------------------------------------------------------------------------
### Status information

 <code>GET</code> <code><b>/status</b></code> (get current status of the node)

##### Parameters

None

##### Responses

> | http code     | content-type                      | response                        | comments                                    |
> |---------------|-----------------------------------|---------------------------------|-----------------------------------|
> | `200`         | `application/json`        | *See response example.*    |                            |
> | `500`         | `text/plain`              | `Could not retrieve results` |    

An example response for a successful request:

```
{
	"Url": "http://192.168.1.23:1323",
	"AvailableWarmContainers": {
		"isprime": 1,
		"sleepSec": 2
	},
	"AvailableMemMB": 3072,
	"AvailableCPUs": 8,
	"Coordinates": {
		"Vec": [
			0,
			0,
			0
		],
		"Error": 1.5,
		"Height": 0.00001
	},
	"LoadAvg": [
		1.34,
		1.43,
		0.94
	]
}
```

`AvailableMemMB` and `AvailableCPUs` indicate the currently amount of memory
and CPUs (intended as vCPUs or fractions of them) that can be allocated to new
requests in the node. `Coordinates.Vec` reports the coordinates of this node
in the virtual coordinate space computed by Vivaldi algorithm.

### Listing functions

 <code>GET</code> <code><b>/functions</b></code> (get list of registered functions)

##### Parameters

None

##### Responses

> | http code     | content-type                      | response                        | comments                                    |
> |---------------|-----------------------------------|---------------------------------|-----------------------------------|
> | `200`         | `application/json`        | *List of names of existing functions.*    |                            |
> | `500`         | `text/plain`              | `Could not retrieve results` |    

Note that functions are globally registered in the system. Therefore, the same
list is returned by every node in the same cluster.

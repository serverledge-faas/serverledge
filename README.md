![ServerlEdge](docs/logo.png)


Serverledge is a Function-as-a-Service (FaaS) framework designed to
work in Edge-Cloud environments.

Serverledge allows user to define and execute functions across
distributed nodes located in Edge locations or in Cloud data centers.
Each Serverledge node serves function invocation requests using a local
container pool, so that functions are executed within containers for isolation purposes.
When Edge nodes are overloaded, they can to offload computation
to neighbor Edge nodes or to the Cloud.

Note that Serverledge is a research prototype and it is not (yet) meant to
be used for production use, as it may lack critical security features.

Serverledge has been first described in a [paper](http://www.ce.uniroma2.it/publications/serverledgePerCom2023.pdf) presented at *IEEE PerCom 2023*. If you use Serverledge in your own work, please cite it:

    @inproceedings{serverledge2023percom,
      author={Russo Russo, Gabriele and Mannucci, Tiziana and Cardellini, Valeria and Lo Presti, Francesco},
      booktitle={2023 IEEE International Conference on Pervasive Computing and Communications (PerCom)}, 
      title={Serverledge: Decentralized Function-as-a-Service for the Edge-Cloud Continuum}, 
      year={2023},
      pages={131-140},
      doi={10.1109/PERCOM56429.2023.10099372}
    }

Serverledge has been used in other scientific papers, including:

- F. Righetti, B. Cornacchia, G. Russo Russo, N. Tonellotto, V. Cardellini, C. Vallati, *Energy-Efficient Function Invocation Scheduling for Edge FaaS Platforms*, Proc. of 2025 IEEE Int'l Conference on Smart Computing (SMARTCOMP '25), [doi](https://doi.ieeecomputersociety.org/10.1109/SMARTCOMP65954.2025.00057)

- R. Farahani, N. Mehran, S. Ristov, R. Prodan, *HEFTLess: A Bi-Objective Serverless Workflow Batch Orchestration on the Computing Continuum*. Proc. of IEEE Int'l Conference on Cluster Computing (CLUSTER '24): 286-296, [doi](https://doi.org/10.1109/CLUSTER59578.2024.00032)

- G. Russo Russo, D. Ferrarelli, D. Pasquali, V. Cardellini, F. Lo Presti, *QoS-aware offloading policies for serverless functions in the Cloud-to-Edge continuum*, Future Gener. Comput. Syst. 156: 1-15 (2024), [doi](https://doi.org/10.1016/j.future.2024.02.019)

- A. Semjonov, H. Bornholdt, J. Edinger, G. Russo Russo, *Wasimoff: Distributed Computation Offloading Using WebAssembly in the Browser*, Proc. of \*LESS 2024 (in conjunction with IEEE PerCom '24), 203-208, [doi](https://doi.org/10.1109/PerComWorkshops59983.2024.10503392)

## Requirements

- **Linux** OS
- [Docker Engine](https://docs.docker.com/engine/) 
- Golang 1.24+

## Building from sources

Download a copy of the source code and build the project:

```
$ make
```

You will find executables in `./bin/`.

## Running (single-node deployment)

As functions are executed within Docker containers, you need Docker to
be installed on the host. Furthermore, Serverledge needs
permissions to create containers.

You also need an `etcd` server to run Serverledge. To quickly start a local
server, just download [a recent release](https://github.com/etcd-io/etcd/releases) 
of `etcd` and just run `./etcd`.

Start a local Serverledge node:

	$ bin/serverledge

### Creating and invoking functions

Register a function `func` from example python code (the handler is formatted like this: $(filename).$(functionName)):

	$ bin/serverledge-cli create -f func --memory 600 --src examples/hello.py --runtime python310 --handler "hello.handler"

Register a function `func` from example javascript code (the handler is formatted like this: $(filename) and the name of the function is "handler"):

	$ bin/serverledge-cli create -f func --memory 600 --src examples/hello.js --runtime nodejs17ng --handler "hello"
    $ bin/serverledge-cli create -f func --memory 600 --src examples/inc.js --runtime nodejs17ng --handler "inc"

Invoke `func` with arguments `a=2` and `b=3`:

	$ bin/serverledge-cli invoke -f func -p "a:2" -p "b:3"

For non-trivial inputs, it is recommended to specify function arguments through a
JSON file, instead of using the `-p` flag, as follows:

	$ bin/serverledge-cli invoke -f func --params_file input.json

where `input.json` may contain:

	{
		"a": 2,
		"b": 3
	}

#### Getting function standard output

You may want to see the content printed by the function to its standard output/error. To do so, add the `--return_output` flag (`-o` for short):

	$ bin/serverledge-cli invoke -f func -p "a:2" -p "b:3" --return_output

Note that we currently support output capture only for some runtimes (e.g., Python supports it).

## Distributed Deployment

[This repository](https://github.com/serverledge-faas/serverledge-deploy) provides an
Ansible playbook to deploy Serverledge in a distributed configuration.

In this case, you can instruct `serverledge-cli` to
connect to a node other than `localhost` or use a non-default port
by means of environment variables or command-line options:

- Use `--host <HOST>` (or `-H <HOST>`) and/or `--port <PORT>` (or, `-P <PORT>`)
  to specify the server
  host and port on the command line
- Alternatively, you can set the environment variables
  `SERVERLEDGE_HOST` and/or `SERVERLEDGE_PORT`, which are read by the client.

Example:

    $ bin/serverledge-cli status -H <host ip-address> -P <port number>

## Configuration

You can provide a configuration file using YAML or TOML syntax. Depending on the
chosen format, the default file name will be `serverledge-conf.yaml` or
`serverledge-conf.toml`. The file can be either placed in `/etc/serverledge`,
in the user `$HOME` directory, or in the working directory where the server is
started.

Alternatively, you can indicate a specific configuration file when starting the
server:

	$ bin/serverledge <config file>

Further information about supported configuration options is available [here](./docs/configuration.md).

### Example

The configuration file may look like this:

	container.pool.memory: 4096
	etcd.address: "1.2.3.4:2379"
	scheduler.queue.capacity: 0
	metrics.enabled: true

## Workflows

Serverledge has been extended to support the composition of multiple functions into workflows.
More information [here](./docs/workflows.md)

## Additional Documentation

 - [API reference](./docs/api.md)
 - [Writing functions](./docs/writing-functions.md)
 - [Serverledge Internals: Executor](./docs/executor.md)
 - [Metrics](./docs/metrics.md)

## Related Projects

A graphical UI to demonstrate the functionality of Serverledge has been
developed
at the Dept. of Information Engineering @ University of Pisa, Italy.
The project can be found
[here](https://github.com/Serverledge-UNIPI/Serverledge_Demo)

## License

Serverledge is distributed under the terms of the [MIT
license](https://github.com/serverledge-faas/serverledge/blob/master/LICENSE.txt).

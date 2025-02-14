Two approaches are available to prepare a custom container image containing
your function code, as explained below.



## Custom image (the easy way)

The easiest way to build a custom function image is by leveraging the
Serverledge base runtime image, i.e., `grussorusso/serverledge-base`.
This image contains a simple implementation of the [Executor](https://github.com/serverledge-faas/serverledge/blob/main/docs/executor.md)
server. When the function is invoked, the Executor runs a user-specified
command as a new process and sets a few environment variables that may be
used by the called process:

- `PARAMS_FILE`: path of a file containing JSON-marshaled function parameters
- `RESULT_FILE`: name of the file where the function must write its JSON-encoded result
- `CONTEXT`: (optional) a JSON-encoded representation of the execution context

You can write a `Dockerfile` as follows to build your own runtime image, e.g.:

	FROM grussorusso/serverledge-base as BASE

	# Extend any image you want, e.g.;
	FROM tensorflow/tensorflow:latest

	# Required: install the executor as /executor
	COPY --from=BASE /executor /
	CMD /executor

	# Required: this is the command representing your function
	ENV CUSTOM_CMD "python /function.py"

	# Install your code and any dependency, e.g.:
	RUN pip3 install pillow
	COPY function.py /
	# ...

A complete [example](../examples/c++/README.md) is provided for C++.

## Custom image (the better way)

For higher efficiency, instead of using the default Executor implementation,
you can define your image from scratch rewriting/extending the
`Dockerfile` and code used to build Serverledge runtime images.
For instance, if you want to create a custom image for a Python-based function,
you may write your own  `Dockerfile` based on the content of `<ServerledgeRepo>/images/pythonXXX/`.

By doing so, you get rid of some process creation overheads, as
your function is directly called upon arrival of invocation requests.

## Using a custom image

The new function can be created using the CLI interface, setting `custom` as the desired runtime and
specifying the `custom_image` parameter.

	bin/serverledge-cli create -function myfunc -memory 256 -runtime custom -custom_image MY_IMAGE_TAG 

### Example
The `examples/jsonschema` directory of the repository provides example files on
how to build a custom image for a Python function requiring additional
libraries.

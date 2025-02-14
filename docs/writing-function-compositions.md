# Functions composition

Serverledge accepts workflows defined by users through a subset of the JSON-based *Amazon States Language*, currently in use by AWS Step Functions. Furthermore, you can define workflows programmatically
with the Builder APIs.

Serverledge workflows comprise 4 types of nodes:
- **SimpleNode**: a node that wraps a function. This is the only node that executes user-defined functions.
- **ChoiceNode**: a node with N conditions that transfers its input to the first branch whose condition is evaluated as true
- **FanOutNode**: a node with N outputs that copies (or scatters) the input to all the outputs (with subsequent nodes activated in parallel)
-  **FanInNode**: a node with N inputs that waits for the termination of all the parent nodes, and then merges the results in one output. The node fails after a specified timeout.

Three special nodes are always present and pre-built when using the APIs:
- **StartNode**: the single node from which the workflow starts executing
- **EndNode**: the final node of the workflow
- **ErrorNode**: a node that terminates workflow execution with failure 


## Signature
Specifying a signature is optional for Serverledge functions. However,
in order to use functions within workflows, they must have an associated signature.
A signature specifies the type of the inputs accepted by the function, as well as the type of the produced outputs. For instance, a *Fibonacci* function might have a single integer input, and produce a single integer output.

The signature can be specified when creating a function through the CLI.

Inputs can be specified using the `--input` (or `-i` for short) option, while outputs are specified using the `--output` (or `-o` for short) option.

Both inputs and outputs are specified using the syntax `name:type`, where `type` is one of the following strings: `Int`, `Float`, `Text`, `Bool`.

Example:

	bin/serverledge-cli create --function inc \
	     --memory 128 \
	     --src examples/inc.py \
	     --runtime python310 \
	     --handler "inc.handler"\
	     --input "n:Int" --output "m:Int"



## Amazon States Language

TODO

## Builder API

It is possible to use the internal builder APIs to build complex workflows programmatically in Go.
Here is an example of a workflow made by two simple nodes and a choice node, with N alternative conditions

	N := 4
	function := function.Function{...}
	condition := make([]Condition, N)
	condition[0] = Condition{...}
	...
	condition[N-1] = Condition{...}
	
	NewBuilder().
	    AddSimpleNode(&function).
	    AddSimpleNode(&function2).
	    AddChoiceNode(conditions).
	    ForEach(NewBuilder().
	            AddSimpleNode(&function).
	            Build()).
	    EndChoiceNode().
    Build()
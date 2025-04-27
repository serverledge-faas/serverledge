# Workflows

Serverledge accepts workflows defined by users through a subset of the JSON-based *Amazon States Language*, currently in use by AWS Step Functions. 

## Tasks

Serverledge workflows currently comprise 4 types of *tasks*:
- **Simple**: a task that wraps a function. This is the only task that executes user-defined functions.
- **Choice**: a task with N alternative branches, each associated with a condition; execution control and input data are transferred to the first branch whose condition is evaluated as true
- **FanOut**: a task with N outputs that copies (or scatters) the input to all the outputs (with subsequent nodes activated in parallel); *experimental*
-  **FanIn**: a task with N inputs that waits for the termination of all the parent nodes, and then merges the results in one output. The node fails after a specified timeout.

Other special types of tasks are always present and pre-built when using the APIs:
- **Start**: the task from which the workflow starts executing (not associated with any function)
- **End**: the final task of the workflow (not associated with any function)
- **Success**: a task that -- as soon it is activated -- terminates workflow execution reporting success 
- **Fail**: a task that -- as soon it is activated -- terminates workflow execution reporting a failure 


## Writing Functions

*Simple* tasks execute a regular Serverledge function. Any function previously
registered in Serverledge can be associated with a Simple task. The only
additional requirement is that **functions used in workflows must be associated
with a Signature upon creation** (which is optional in general).

### Signature
A signature specifies the name and the type of the inputs accepted by the function, as well as the type of the returned outputs. For instance, a *Fibonacci* function might have a single integer input, and produce a single integer output.

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



## Example

The file `examples/workflow-simple.json` contains an example ASL definition of
a workflow. To register it:

    bin/serverledge-cli create-workflow -s examples/workflow-simple.json -f myWorkflow

To execute it with input `n=2`:

    bin/serverledge-cli invoke-workflow -f myWorkflow -p "input:2"


package workflow

import (
	"fmt"

	"github.com/serverledge-faas/serverledge/internal/function"
)

// Builder is a utility struct that helps easily define the Workflow, using the Builder pattern.
// Use NewBuilder() to safely initialize it. Then use the available methods to iteratively build the workflow.
// Finally use Build() to get the complete Workflow.
type Builder struct {
	workflow     Workflow
	branches     int
	prevNode     Task
	errors       []error
	BranchNumber int
}

func (b *Builder) appendError(err error) {
	fmt.Printf("Error in builder: %v\n", err)
	b.errors = append(b.errors, err)
}

type ChoiceBranchBuilder struct {
	builder   *Builder
	completed int // counter of branches that reach the end node
}

func NewBuilder() *Builder {
	db := Builder{
		workflow:     newWorkflow(),
		branches:     1,
		errors:       make([]error, 0),
		BranchNumber: 0,
	}
	db.prevNode = db.workflow.Start
	return &db
}

// AddFunctionTask connects a simple node to the previous node
func (b *Builder) AddFunctionTask(f *function.Function) *Builder {
	return b.AddFunctionTaskWithId(f, "")
}

// AddFunctionTaskWithId connects a simple node with the specified id to the previous node
func (b *Builder) AddFunctionTaskWithId(f *function.Function, id string) *Builder {
	nErrors := len(b.errors)
	if nErrors > 0 {
		fmt.Printf("AddFunctionTask skipped, because of %d error(s) in builder\n", nErrors)
		return b
	}

	simpleNode := NewFunctionTask(f.Name)
	if id != "" {
		simpleNode.Id = TaskId(id)
	}
	b.workflow.add(simpleNode)

	switch prevTask := b.prevNode.(type) {
	case UnaryTask:
		err := prevTask.SetNext(simpleNode)
		if err != nil {
			b.appendError(err)
			return b
		}
	default:
		panic("Unsupported previous task:" + prevTask.String())
	}

	b.prevNode = simpleNode
	return b
}

// AddChoiceNode connects a choice node to the previous node. From the choice node, multiple branch are created and each one of those must be fully defined
func (b *Builder) AddChoiceNode(conditions ...Condition) *ChoiceBranchBuilder {
	nErrors := len(b.errors)
	if nErrors > 0 {
		fmt.Printf("NextBranch skipped, because of %d error(s) in builder\n", nErrors)
		return &ChoiceBranchBuilder{builder: b, completed: 0}
	}

	// fmt.Println("Added choice node to Workflow")
	choiceNode := NewChoiceTask(conditions)
	b.branches = len(conditions)
	b.workflow.add(choiceNode)
	switch prevTask := b.prevNode.(type) {
	case UnaryTask:
		err := prevTask.SetNext(choiceNode)
		if err != nil {
			b.appendError(err)
			return &ChoiceBranchBuilder{builder: b, completed: 0}
		}
	default:
		panic("Unsupported previous task:" + prevTask.String())
	}

	b.prevNode = choiceNode
	emptyBranches := make([]TaskId, 0, b.branches)
	choiceNode.AlternativeNextTasks = emptyBranches
	// we construct a new slice with capacity (b.branches) and size 0
	// Here we cannot chain directly, because we do not know which alternative to chain to which node
	// so we return a ChoiceBranchBuilder
	return &ChoiceBranchBuilder{builder: b, completed: 0}
}

// NextBranch is used to chain the next branch to a Workflow and then returns the ChoiceBranchBuilder.
// Tip: use a NewBuilder() as a parameter, instead of manually creating the Workflow!
// Internally, NextBranch replaces the StartTask of the input workflow with the choice alternative
// and chains the last node of the workflow to the EndTask of the building workflow
func (c *ChoiceBranchBuilder) NextBranch(toMerge *Workflow, err1 error) *ChoiceBranchBuilder {
	if err1 != nil {
		fmt.Println(err1)
		c.builder.appendError(err1)
	}
	nErrors := len(c.builder.errors)
	if nErrors > 0 {
		fmt.Printf("NextBranch skipped, because of %d error(s) in builder\n", nErrors)
		return c
	}

	//fmt.Println("Added simple node to a branch in choice node of Workflow")
	if c.HasNextBranch() {
		c.builder.BranchNumber++
		// getting start.Next from the toMerge
		startNext, _ := toMerge.Find(toMerge.Start.GetNext())
		// chains the alternative to the input workflow, which is already connected to a whole series of nodes
		if !toMerge.IsEmpty() {
			c.builder.workflow.add(startNext)
			switch prev := c.builder.prevNode.(type) {
			case UnaryTask:
				err := prev.SetNext(startNext)
				if err != nil {
					c.builder.appendError(err)
				}
			case ConditionalTask:
				err := prev.AddAlternative(startNext)
				if err != nil {
					c.builder.appendError(err)
				}
			default:
				panic("Unsupported previous task:" + prev.String())
			}

			// adds the nodes to the building workflow
			for _, n := range toMerge.Tasks {
				switch typedTask := n.(type) {
				case *StartTask:
					continue
				case *EndTask:
					continue
				case *ChoiceTask:
					c.builder.workflow.add(n)
					continue
				case UnaryTask:
					c.builder.workflow.add(n)
					nextNode, _ := toMerge.Find(typedTask.GetNext())
					// chain the last node(s) of the input workflow to the end node of the building workflow

					if nextNode == toMerge.End {
						errEnd := typedTask.SetNext(c.builder.workflow.End)
						if errEnd != nil {
							c.builder.appendError(errEnd)
							return c
						}
					}
				default:
					panic("Unsupported previous task:" + n.String())
				}
			}
			// so we completed a branch
			c.completed++
			c.builder.branches--
		} else {
			fmt.Printf("not adding another end node\n")
			c.EndNextBranch()
		}
	} else {
		panic("There is not a NextBranch. Use EndChoiceAndBuild to end the choiceNode.")
	}
	return c
}

// EndNextBranch is used to chain the next choice branch to the end node of the workflow, resulting in a no-op branch
func (c *ChoiceBranchBuilder) EndNextBranch() *ChoiceBranchBuilder {
	nErrors := len(c.builder.errors)
	if nErrors > 0 {
		fmt.Printf("NextBranch skipped, because of %d error(s) in builder\n", nErrors)
		return c
	}
	workflow := &c.builder.workflow

	if c.HasNextBranch() {
		c.builder.BranchNumber++ // we only increase the branch number, but we do not use in any node
		//fmt.Printf("Ending branch %d for Workflow\n", c.builder.BranchNumber)
		// chain the alternative of the choice node to the end node of the building workflow
		choice := c.builder.prevNode.(*ChoiceTask)
		var alternative Task
		if c.completed < len(choice.AlternativeNextTasks) {
			x := choice.AlternativeNextTasks[c.completed]
			alternative, _ = workflow.Find(x)
			switch typedTask := alternative.(type) {
			case UnaryTask:
				err := typedTask.SetNext(c.builder.workflow.End)
				if err != nil {
					c.builder.appendError(err)
					return c
				}
			default:
				panic("Unsupported previous task:" + alternative.String())
			}
		} else {
			alternative = choice // this is when a choice branch directly goes to end node
			err := alternative.(*ChoiceTask).AddAlternative(c.builder.workflow.End)
			if err != nil {
				c.builder.appendError(err)
				return c
			}
		}
		// ... and we completed a branch
		c.completed++
		c.builder.branches--
		if !c.HasNextBranch() {
			c.builder.prevNode = c.builder.workflow.End
		}
	} else {
		fmt.Println("warning: Useless call EndNextBranch: all branch are ended")
	}
	return c
}

func (c *ChoiceBranchBuilder) HasNextBranch() bool {
	return c.builder.branches > 0
}

// EndChoiceAndBuild connects all remaining branches to the end node and returns the workflow
func (c *ChoiceBranchBuilder) EndChoiceAndBuild() (*Workflow, error) {
	for c.HasNextBranch() {
		c.EndNextBranch()
		if len(c.builder.errors) > 0 {
			return nil, fmt.Errorf("build failed with errors:\n%v", c.builder.errors)
		}
	}

	return &c.builder.workflow, nil
}

// ForEachBranch chains each (remaining) output of a choice node to the same subsequent node, then returns the Builder
func (c *ChoiceBranchBuilder) ForEachBranch(dagger func() (*Workflow, error)) *ChoiceBranchBuilder {
	choiceNode := c.builder.prevNode.(*ChoiceTask)
	// we suppose the branches 0, ..., (completed-1) are already completed
	// once := true
	remainingBranches := c.builder.branches
	for i := c.completed; i < remainingBranches; i++ {
		c.builder.BranchNumber++
		//fmt.Printf("Adding workflow to branch %d\n", c.builder.BranchNumber)
		// recreates a workflow executing the same function
		workflowCopy, err := dagger()
		if err != nil {
			c.builder.appendError(err)
		}
		nextNode, _ := workflowCopy.Find(workflowCopy.Start.GetNext())
		c.builder.workflow.add(nextNode)
		err = choiceNode.AddAlternative(nextNode)
		if err != nil {
			c.builder.appendError(err)
		}
		// adds the nodes to the building workflow, but only once!
		for _, n := range workflowCopy.Tasks {
			switch typedTask := n.(type) {
			case *StartTask:
				continue
			case *EndTask:
				continue
			case UnaryTask:
				c.builder.workflow.add(n)
				// chain the last node(s) of the input workflow to the end node of the building workflow
				if typedTask.GetNext() == workflowCopy.End.GetId() {
					errEnd := typedTask.SetNext(c.builder.workflow.End)
					if errEnd != nil {
						c.builder.appendError(errEnd)
						return c
					}
				}
			default:
				panic("Unsupported previous task:" + n.String())
			}

		}
		// so we completed a branch
		c.completed++
		c.builder.branches--
	}
	return c
}

func (b *Builder) AddFailNodeAndBuild(errorName, errorMessage string) (*Workflow, error) {
	nErrors := len(b.errors)
	if nErrors > 0 {
		return nil, fmt.Errorf("AddFailNodeAndBuild failed because of the following %d error(s) in builder\n%v", nErrors, b.errors)
	}

	failNode := NewFailureTask(errorName, errorMessage)

	b.workflow.add(failNode)
	switch prevTask := b.prevNode.(type) {
	case UnaryTask:
		err := prevTask.SetNext(failNode)
		if err != nil {
			return nil, fmt.Errorf("failed to chain the Fail: %v", err)
		}
	default:
		panic("Unsupported previous task:" + prevTask.String())
	}
	b.prevNode = failNode
	return b.Build()
}

func (b *Builder) AddSucceedNodeAndBuild(message string) (*Workflow, error) {
	nErrors := len(b.errors)
	if nErrors > 0 {
		return nil, fmt.Errorf("AddSucceedNodeAndBuild failed because of the following %d error(s) in builder\n%v", nErrors, b.errors)
	}

	succeedNode := NewSuccessTask()

	b.workflow.add(succeedNode)
	switch prevTask := b.prevNode.(type) {
	case UnaryTask:
		err := prevTask.SetNext(succeedNode)
		if err != nil {
			return nil, fmt.Errorf("failed to chain the SuccessTask: %v", err)
		}
	default:
		panic("Unsupported previous task:" + prevTask.String())
	}
	b.prevNode = succeedNode
	return b.Build()
}

func (b *Builder) AddPassNode(result string) *Builder {
	nErrors := len(b.errors)
	if nErrors > 0 {
		fmt.Printf("AddFunctionTask skipped, because of %d error(s) in builder\n", nErrors)
		return b
	}

	passNode := NewPassTask(result)

	b.workflow.add(passNode)
	switch prevTask := b.prevNode.(type) {
	case UnaryTask:
		err := prevTask.SetNext(passNode)
		if err != nil {
			b.appendError(err)
			return b
		}
	default:
		panic("Unsupported previous task:" + prevTask.String())
	}

	b.prevNode = passNode
	return b
}

// Build ends the single branch with an EndTask. If there is more than one branch, it panics!
func (b *Builder) Build() (*Workflow, error) {
	switch typedTask := b.prevNode.(type) {
	case nil:
		return &b.workflow, nil
	case *EndTask:
		return &b.workflow, nil
	case UnaryTask:
		err := typedTask.SetNext(b.workflow.End)
		if err != nil {
			return nil, fmt.Errorf("failed to chain to end node: %v", err)
		}
	default:
		panic("Unsupported previous task:" + b.prevNode.String())
	}
	return &b.workflow, nil
}

func CreateEmptyWorkflow() (*Workflow, error) {
	return NewBuilder().Build()
}

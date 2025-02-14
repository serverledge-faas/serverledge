package workflow

import (
	"errors"
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
	b.errors = append(b.errors, err)
}

type ChoiceBranchBuilder struct {
	builder   *Builder
	completed int // counter of branches that reach the end node
}

// ParallelScatterBranchBuilder can only hold the same workflow executed in parallel in each branch
type ParallelScatterBranchBuilder struct {
	builder       *Builder
	completed     int
	terminalNodes []Task
	fanOutId      TaskId
}

// ParallelBroadcastBranchBuilder can hold different dags executed in parallel
type ParallelBroadcastBranchBuilder struct {
	builder       *Builder
	completed     int
	terminalNodes []Task
	fanOutId      TaskId
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

// AddSimpleNode connects a simple node to the previous node
func (b *Builder) AddSimpleNode(f *function.Function) *Builder {
	nErrors := len(b.errors)
	if nErrors > 0 {
		fmt.Printf("AddSimpleNode skipped, because of %d error(s) in builder\n", nErrors)
		return b
	}

	simpleNode := NewSimpleTask(f.Name)

	b.workflow.add(simpleNode)
	err := b.workflow.chain(b.prevNode, simpleNode)
	if err != nil {
		b.appendError(err)
		return b
	}

	b.prevNode = simpleNode
	// log.Println("Added simple node to Workflow")
	return b
}

// AddSimpleNodeWithId connects a simple node with the specified id to the previous node
func (b *Builder) AddSimpleNodeWithId(f *function.Function, id string) *Builder {
	nErrors := len(b.errors)
	if nErrors > 0 {
		fmt.Printf("AddSimpleNode skipped, because of %d error(s) in builder\n", nErrors)
		return b
	}

	simpleNode := NewSimpleTask(f.Name)
	simpleNode.Id = TaskId(id)

	b.workflow.add(simpleNode)
	err := b.workflow.chain(b.prevNode, simpleNode)
	if err != nil {
		b.appendError(err)
		return b
	}

	b.prevNode = simpleNode
	//fmt.Printf("Added simple node to Workflow with id %s\n", id)
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
	err := b.workflow.chain(b.prevNode, choiceNode)
	if err != nil {
		b.appendError(err)
		return &ChoiceBranchBuilder{builder: b, completed: 0}
	}
	b.prevNode = choiceNode
	emptyBranches := make([]TaskId, 0, b.branches)
	choiceNode.Alternatives = emptyBranches
	// we construct a new slice with capacity (b.branches) and size 0
	// Here we cannot chain directly, because we do not know which alternative to chain to which node
	// so we return a ChoiceBranchBuilder
	return &ChoiceBranchBuilder{builder: b, completed: 0}
}

// AddScatterFanOutNode connects a fanout node to the previous node. From the fanout node, multiple branch are created and each one of those must be fully defined, eventually ending in a FanInTask
func (b *Builder) AddScatterFanOutNode(fanOutDegree int) *ParallelScatterBranchBuilder {
	nErrors := len(b.errors)
	if nErrors > 0 {
		fmt.Printf("NextBranch skipped, because of %d error(s) in builder\n", nErrors)
		return &ParallelScatterBranchBuilder{builder: b, terminalNodes: make([]Task, 0)}
	}
	if fanOutDegree <= 1 {
		b.appendError(errors.New("fanOutDegree should be at least 1"))
		return &ParallelScatterBranchBuilder{builder: b, terminalNodes: make([]Task, 0)}
	}
	fanOut := NewFanOutTask(fanOutDegree, Scatter)
	b.workflow.add(fanOut)
	err := b.workflow.chain(b.prevNode, fanOut)
	if err != nil {
		b.appendError(err)
		return &ParallelScatterBranchBuilder{builder: b, completed: 0, terminalNodes: make([]Task, 0)}
	}
	//fmt.Println("Added fan out node to Workflow")
	b.branches = fanOutDegree
	b.prevNode = fanOut
	return &ParallelScatterBranchBuilder{builder: b, completed: 0, terminalNodes: make([]Task, 0), fanOutId: fanOut.Id}
}

// AddBroadcastFanOutNode connects a fanout node to the previous node. From the fanout node, multiple branch are created and each one of those must be fully defined, eventually ending in a FanInTask
func (b *Builder) AddBroadcastFanOutNode(fanOutDegree int) *ParallelBroadcastBranchBuilder {
	nErrors := len(b.errors)
	if nErrors > 0 {
		fmt.Printf("NextBranch skipped, because of %d error(s) in builder\n", nErrors)
		return &ParallelBroadcastBranchBuilder{builder: b, completed: 0, terminalNodes: make([]Task, 0)}
	}
	fanOut := NewFanOutTask(fanOutDegree, Broadcast)
	b.workflow.add(fanOut)
	err := b.workflow.chain(b.prevNode, fanOut)
	if err != nil {
		b.appendError(err)
		return &ParallelBroadcastBranchBuilder{builder: b, completed: 0, terminalNodes: make([]Task, 0)}
	}
	b.branches = fanOutDegree
	b.prevNode = fanOut

	return &ParallelBroadcastBranchBuilder{builder: b, completed: 0, terminalNodes: make([]Task, 0), fanOutId: fanOut.Id}
}

// NextBranch is used to chain the next branch to a Workflow and then returns the ChoiceBranchBuilder.
// Tip: use a NewBuilder() as a parameter, instead of manually creating the Workflow!
// Internally, NextBranch replaces the StartTask of the input workflow with the choice alternative
// and chains the last node of the workflow to the EndTask of the building workflow
func (c *ChoiceBranchBuilder) NextBranch(toMerge *Workflow, err1 error) *ChoiceBranchBuilder {
	if err1 != nil {
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
		startNext, _ := toMerge.Find(toMerge.Start.Next)
		// chains the alternative to the input workflow, which is already connected to a whole series of nodes
		if !toMerge.IsEmpty() {
			c.builder.workflow.add(startNext)
			err := c.builder.workflow.chain(c.builder.prevNode.(*ChoiceTask), startNext)
			if err != nil {
				c.builder.appendError(err)
			}
			// adds the nodes to the building workflow
			for _, n := range toMerge.Tasks {
				switch n.(type) {
				case *StartTask:
					continue
				case *EndTask:
					continue
				case *FanOutTask:
					c.builder.workflow.add(n)
					continue
				case *ChoiceTask:
					c.builder.workflow.add(n)
					continue
				default:
					c.builder.workflow.add(n)
					nextNode, _ := toMerge.Find(n.GetNext()[0])
					// chain the last node(s) of the input workflow to the end node of the building workflow

					if n.GetNext() != nil && len(n.GetNext()) > 0 && nextNode == toMerge.End {
						errEnd := c.builder.workflow.ChainToEndTask(n)
						if errEnd != nil {
							c.builder.appendError(errEnd)
							return c
						}
					}
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
		if c.completed < len(choice.Alternatives) {
			x := choice.Alternatives[c.completed]
			alternative, _ = workflow.Find(x)
		} else {
			alternative = choice // this is when a choice branch directly goes to end node
		}
		err := c.builder.workflow.ChainToEndTask(alternative)
		if err != nil {
			c.builder.appendError(err)
			return c
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
		nextNode, _ := workflowCopy.Find(workflowCopy.Start.Next)
		c.builder.workflow.add(nextNode)
		err = c.builder.workflow.chain(choiceNode, nextNode)
		if err != nil {
			c.builder.appendError(err)
		}
		// adds the nodes to the building workflow, but only once!
		for _, n := range workflowCopy.Tasks {
			switch n.(type) {
			case *StartTask:
				continue
			case *EndTask:
				continue
			case *FanOutTask:
				errFanout := fmt.Errorf("you're trying to chain a fanout node to an end node. This will interrupt the execution immediately after the fanout is reached")
				c.builder.appendError(errFanout)
				continue
			default:
				c.builder.workflow.add(n)
				// chain the last node(s) of the input workflow to the end node of the building workflow
				if n.GetNext() != nil && len(n.GetNext()) > 0 && n.GetNext()[0] == workflowCopy.End.GetId() {
					errEnd := c.builder.workflow.ChainToEndTask(n)
					if errEnd != nil {
						c.builder.appendError(errEnd)
						return c
					}
				}
			}

		}
		// so we completed a branch
		c.completed++
		c.builder.branches--
	}
	return c
}

func (p *ParallelBroadcastBranchBuilder) ForEachParallelBranch(dagger func() (*Workflow, error)) *ParallelBroadcastBranchBuilder {
	fanOutNode := p.builder.prevNode.(*FanOutTask)
	// we suppose the branches 0, ..., (completed-1) are already completed
	remainingBranches := p.builder.branches
	for i := p.completed; i < remainingBranches; i++ {
		p.builder.BranchNumber++
		//fmt.Printf("Adding workflow to branch %d\n", i)
		// recreates a workflow executing the same function
		workflowCopy, err := dagger()
		if err != nil {
			p.builder.appendError(err)
		}
		next, _ := workflowCopy.Find(workflowCopy.Start.Next)
		p.builder.workflow.add(next)
		err = p.builder.workflow.chain(fanOutNode, next)
		if err != nil {
			p.builder.appendError(err)
		}
		// adds the nodes to the building workflow, but only once!
		for _, n := range workflowCopy.Tasks {
			// chain the last node(s) of the input workflow to the end node of the building workflow
			switch n.(type) {
			case *StartTask:
				continue
			case *EndTask:
				continue
			case *FanOutTask:
				p.builder.appendError(fmt.Errorf("you're trying to chain a branch of a fanout node to an end node. This will interrupt the execution immediately after the fanout is reached"))
				continue
			default:
				p.builder.workflow.add(n)
				if n.GetNext() != nil && len(n.GetNext()) > 0 && n.GetNext()[0] == workflowCopy.End.GetId() {
					p.terminalNodes = append(p.terminalNodes, n) // we do not chain to end node, only add to terminal nodes, so that we can chain to a fan in later
				}
			}

		}
		// so we completed a branch
		p.completed++
		p.builder.branches--
	}
	return p
}

func (p *ParallelScatterBranchBuilder) ForEachParallelBranch(dagger func() (*Workflow, error)) *ParallelScatterBranchBuilder {
	fanOutNode := p.builder.prevNode.(*FanOutTask)
	// we suppose the branches 0, ..., (completed-1) are already completed
	remainingBranches := p.builder.branches
	for i := p.completed; i < remainingBranches; i++ {
		p.builder.BranchNumber++
		//fmt.Printf("Adding workflow to branch %d\n", i)
		// recreates a workflow executing the same function
		workflowCopy, err := dagger()
		if err != nil {
			p.builder.appendError(err)
		}
		next, _ := workflowCopy.Find(workflowCopy.Start.Next)
		p.builder.workflow.add(next)
		err = p.builder.workflow.chain(fanOutNode, next)
		if err != nil {
			p.builder.appendError(err)
		}
		// adds the nodes to the building workflow, but only once!
		for _, n := range workflowCopy.Tasks {
			// chain the last node(s) of the input workflow to the end node of the building workflow
			switch n.(type) {
			case *StartTask:
				continue
			case *EndTask:
				continue
			case *FanOutTask:
				p.builder.appendError(fmt.Errorf("you're trying to chain a branch of a fanout node to an end node. This will interrupt the execution immediately after the fanout is reached"))
				continue
			default:
				p.builder.workflow.add(n)
				if n.GetNext() != nil && len(n.GetNext()) > 0 && n.GetNext()[0] == workflowCopy.End.GetId() {
					p.terminalNodes = append(p.terminalNodes, n) // we do not chain to end node, only add to terminal nodes, so that we can chain to a fan in later
				}
			}
		}
		// so we completed a branch
		p.completed++
		p.builder.branches--
	}
	return p
}

func (p *ParallelScatterBranchBuilder) AddFanInNode() *Builder {
	//fmt.Println("Added fan in node after fanout in Workflow")
	workflow := &p.builder.workflow
	fanInNode := NewFanInTask(p.builder.prevNode.Width())
	p.builder.BranchNumber++
	// TODO: set fanin inside fanout, so that we know which fanin are dealing with
	for _, n := range p.terminalNodes {
		// terminal nodes
		errAdd := n.AddOutput(workflow, fanInNode.GetId())
		if errAdd != nil {
			p.builder.appendError(errAdd)
			return p.builder
		}
	}
	p.builder.workflow.add(fanInNode)
	p.builder.prevNode = fanInNode
	// finding fanOut node, then assigning corresponding fanIn
	fanOut, ok := p.builder.workflow.Find(p.fanOutId)
	if ok {
		fanOut.(*FanOutTask).AssociatedFanIn = fanInNode.Id
	} else {
		p.builder.appendError(fmt.Errorf("failed to find fanOutNode"))
	}
	return p.builder
}

func (p *ParallelBroadcastBranchBuilder) AddFanInNode() *Builder {
	//fmt.Println("Added fan in node after fanout in Workflow")
	workflow := &p.builder.workflow
	fanInNode := NewFanInTask(p.builder.prevNode.Width())
	p.builder.BranchNumber++
	for _, n := range p.terminalNodes {
		// terminal nodes
		errAdd := n.AddOutput(workflow, fanInNode.GetId())
		if errAdd != nil {
			p.builder.appendError(errAdd)
			return p.builder
		}
	}
	p.builder.workflow.add(fanInNode)
	p.builder.prevNode = fanInNode
	// finding fanOut node, then assigning corresponding fanIn
	fanOut, ok := p.builder.workflow.Find(p.fanOutId)
	if ok {
		fanOut.(*FanOutTask).AssociatedFanIn = fanInNode.Id
	} else {
		p.builder.appendError(fmt.Errorf("failed to find fanOutNode"))
	}
	return p.builder
}

func (p *ParallelBroadcastBranchBuilder) NextFanOutBranch(toMerge *Workflow, err1 error) *ParallelBroadcastBranchBuilder {
	if err1 != nil {
		p.builder.appendError(err1)
	}
	nErrors := len(p.builder.errors)
	if nErrors > 0 {
		fmt.Printf("NextBranch skipped, because of %d error(s) in builder\n", nErrors)
		return p
	}

	//fmt.Println("Added simple node to a branch in choice node of Workflow")
	if p.HasNextBranch() {
		p.builder.BranchNumber++
		// chains the alternative to the input workflow, which is already connected to a whole series of nodes
		next, _ := toMerge.Find(toMerge.Start.Next)
		err := p.builder.workflow.chain(p.builder.prevNode, next)
		if err != nil {
			p.builder.appendError(err)
		}
		// adds the nodes to the building workflow
		for _, n := range toMerge.Tasks {
			// chain the last node(s) of the input workflow to the end node of the building workflow
			switch n.(type) {
			case *StartTask:
				continue
			case *EndTask:
				continue
			case *FanOutTask:
				errFanout := fmt.Errorf("you're trying to chain a fanout node to an end node. This will interrupt the execution immediately after the fanout is reached")
				p.builder.appendError(errFanout)
				continue
			default:
				p.builder.workflow.add(n)
				if n.GetNext() != nil && len(n.GetNext()) > 0 && n.GetNext()[0] == toMerge.End.GetId() {
					p.terminalNodes = append(p.terminalNodes, n)
				}
			}
		}

		// so we completed a branch
		p.completed++
		p.builder.branches--
	} else {
		p.builder.appendError(errors.New("there is not a Next ParallelBranch. Use AddFanInNode to end the FanOutTask"))
	}

	return p
}

func (p *ParallelBroadcastBranchBuilder) HasNextBranch() bool {
	return p.builder.branches > 0
}

func (b *Builder) AddFailNodeAndBuild(errorName, errorMessage string) (*Workflow, error) {
	nErrors := len(b.errors)
	if nErrors > 0 {
		return nil, fmt.Errorf("AddFailNodeAndBuild failed because of the following %d error(s) in builder\n%v", nErrors, b.errors)
	}

	failNode := NewFailureTask(errorName, errorMessage)

	b.workflow.add(failNode)
	err := b.workflow.chain(b.prevNode, failNode)
	if err != nil {
		return nil, fmt.Errorf("failed to chain the Fail: %v", err)
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
	err := b.workflow.chain(b.prevNode, succeedNode)
	if err != nil {
		return nil, fmt.Errorf("failed to chain the SuccessTask: %v", err)
	}
	b.prevNode = succeedNode
	return b.Build()
}

func (b *Builder) AddPassNode(result string) *Builder {
	nErrors := len(b.errors)
	if nErrors > 0 {
		fmt.Printf("AddSimpleNode skipped, because of %d error(s) in builder\n", nErrors)
		return b
	}

	passNode := NewPassTask(result)

	b.workflow.add(passNode)
	err := b.workflow.chain(b.prevNode, passNode)
	if err != nil {
		b.appendError(err)
		return b
	}

	b.prevNode = passNode
	return b
}

// Build ends the single branch with an EndTask. If there is more than one branch, it panics!
func (b *Builder) Build() (*Workflow, error) {
	switch b.prevNode.(type) {
	case nil:
		return &b.workflow, nil
	case *EndTask:
		return &b.workflow, nil
	default:
		err := b.workflow.ChainToEndTask(b.prevNode)
		if err != nil {
			return nil, fmt.Errorf("failed to chain to end node: %v", err)
		}
	}
	return &b.workflow, nil
}

func CreateEmptyWorkflow() (*Workflow, error) {
	return NewBuilder().Build()
}

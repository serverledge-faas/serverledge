package fc

import (
	"errors"
	"fmt"

	"github.com/grussorusso/serverledge/internal/function"
)

// DagBuilder is a utility struct that helps easily define the Dag, using the Builder pattern.
// Use NewDagBuilder() to safely initialize it. Then use the available methods to iteratively build the dag.
// Finally use Build() to get the complete Dag.
type DagBuilder struct {
	dag          Dag
	branches     int
	prevNode     DagNode
	errors       []error
	BranchNumber int
}

func (b *DagBuilder) appendError(err error) {
	b.errors = append(b.errors, err)
}

type ChoiceBranchBuilder struct {
	dagBuilder *DagBuilder
	completed  int // counter of branches that reach the end node
}

// ParallelScatterBranchBuilder can only hold the same dag executed in parallel in each branch
type ParallelScatterBranchBuilder struct {
	dagBuilder    *DagBuilder
	completed     int
	terminalNodes []DagNode
	fanOutId      DagNodeId
}

// ParallelBroadcastBranchBuilder can hold different dags executed in parallel
type ParallelBroadcastBranchBuilder struct {
	dagBuilder    *DagBuilder
	completed     int
	terminalNodes []DagNode
	fanOutId      DagNodeId
}

func NewDagBuilder() *DagBuilder {
	db := DagBuilder{
		dag:          NewDAG(),
		branches:     1,
		errors:       make([]error, 0),
		BranchNumber: 0,
	}
	db.prevNode = db.dag.Start
	return &db
}

// AddSimpleNode connects a simple node to the previous node
func (b *DagBuilder) AddSimpleNode(f *function.Function) *DagBuilder {
	nErrors := len(b.errors)
	if nErrors > 0 {
		fmt.Printf("AddSimpleNode skipped, because of %d error(s) in dagBuilder\n", nErrors)
		return b
	}

	simpleNode := NewSimpleNode(f.Name)
	simpleNode.setBranchId(b.BranchNumber)

	b.dag.addNode(simpleNode)
	err := b.dag.chain(b.prevNode, simpleNode)
	if err != nil {
		b.appendError(err)
		return b
	}

	b.prevNode = simpleNode
	// log.Println("Added simple node to Dag")
	return b
}

// AddSimpleNodeWithId connects a simple node with the specified id to the previous node
func (b *DagBuilder) AddSimpleNodeWithId(f *function.Function, id string) *DagBuilder {
	nErrors := len(b.errors)
	if nErrors > 0 {
		fmt.Printf("AddSimpleNode skipped, because of %d error(s) in dagBuilder\n", nErrors)
		return b
	}

	simpleNode := NewSimpleNode(f.Name)
	simpleNode.Id = DagNodeId(id)
	simpleNode.setBranchId(b.BranchNumber)

	b.dag.addNode(simpleNode)
	err := b.dag.chain(b.prevNode, simpleNode)
	if err != nil {
		b.appendError(err)
		return b
	}

	b.prevNode = simpleNode
	//fmt.Printf("Added simple node to Dag with id %s\n", id)
	return b
}

// AddChoiceNode connects a choice node to the previous node. From the choice node, multiple branch are created and each one of those must be fully defined
func (b *DagBuilder) AddChoiceNode(conditions ...Condition) *ChoiceBranchBuilder {
	nErrors := len(b.errors)
	if nErrors > 0 {
		fmt.Printf("NextBranch skipped, because of %d error(s) in dagBuilder\n", nErrors)
		return &ChoiceBranchBuilder{dagBuilder: b, completed: 0}
	}

	// fmt.Println("Added choice node to Dag")
	choiceNode := NewChoiceNode(conditions)
	choiceNode.setBranchId(b.BranchNumber)
	b.branches = len(conditions)
	b.dag.addNode(choiceNode)
	err := b.dag.chain(b.prevNode, choiceNode)
	if err != nil {
		b.appendError(err)
		return &ChoiceBranchBuilder{dagBuilder: b, completed: 0}
	}
	b.prevNode = choiceNode
	b.dag.Width = len(conditions)
	emptyBranches := make([]DagNodeId, 0, b.branches)
	choiceNode.Alternatives = emptyBranches
	// we construct a new slice with capacity (b.branches) and size 0
	// Here we cannot chain directly, because we do not know which alternative to chain to which node
	// so we return a ChoiceBranchBuilder
	return &ChoiceBranchBuilder{dagBuilder: b, completed: 0}
}

// AddScatterFanOutNode connects a fanout node to the previous node. From the fanout node, multiple branch are created and each one of those must be fully defined, eventually ending in a FanInNode
func (b *DagBuilder) AddScatterFanOutNode(fanOutDegree int) *ParallelScatterBranchBuilder {
	nErrors := len(b.errors)
	if nErrors > 0 {
		fmt.Printf("NextBranch skipped, because of %d error(s) in dagBuilder\n", nErrors)
		return &ParallelScatterBranchBuilder{dagBuilder: b, terminalNodes: make([]DagNode, 0)}
	}
	if fanOutDegree <= 1 {
		b.appendError(errors.New("fanOutDegree should be at least 1"))
		return &ParallelScatterBranchBuilder{dagBuilder: b, terminalNodes: make([]DagNode, 0)}
	}
	fanOut := NewFanOutNode(fanOutDegree, Scatter)
	fanOut.setBranchId(b.BranchNumber)
	b.dag.addNode(fanOut)
	err := b.dag.chain(b.prevNode, fanOut)
	if err != nil {
		b.appendError(err)
		return &ParallelScatterBranchBuilder{dagBuilder: b, completed: 0, terminalNodes: make([]DagNode, 0)}
	}
	//fmt.Println("Added fan out node to Dag")
	b.branches = fanOutDegree
	b.prevNode = fanOut
	b.dag.Width = fanOutDegree
	return &ParallelScatterBranchBuilder{dagBuilder: b, completed: 0, terminalNodes: make([]DagNode, 0), fanOutId: fanOut.Id}
}

// AddBroadcastFanOutNode connects a fanout node to the previous node. From the fanout node, multiple branch are created and each one of those must be fully defined, eventually ending in a FanInNode
func (b *DagBuilder) AddBroadcastFanOutNode(fanOutDegree int) *ParallelBroadcastBranchBuilder {
	nErrors := len(b.errors)
	if nErrors > 0 {
		fmt.Printf("NextBranch skipped, because of %d error(s) in dagBuilder\n", nErrors)
		return &ParallelBroadcastBranchBuilder{dagBuilder: b, completed: 0, terminalNodes: make([]DagNode, 0)}
	}
	fanOut := NewFanOutNode(fanOutDegree, Broadcast)
	fanOut.setBranchId(b.BranchNumber)
	b.dag.addNode(fanOut)
	err := b.dag.chain(b.prevNode, fanOut)
	if err != nil {
		b.appendError(err)
		return &ParallelBroadcastBranchBuilder{dagBuilder: b, completed: 0, terminalNodes: make([]DagNode, 0)}
	}
	b.branches = fanOutDegree
	b.prevNode = fanOut
	b.dag.Width = fanOutDegree

	return &ParallelBroadcastBranchBuilder{dagBuilder: b, completed: 0, terminalNodes: make([]DagNode, 0), fanOutId: fanOut.Id}
}

// NextBranch is used to chain the next branch to a Dag and then returns the ChoiceBranchBuilder.
// Tip: use a NewDagBuilder() as a parameter, instead of manually creating the Dag!
// Internally, NextBranch replaces the StartNode of the input dag with the choice alternative
// and chains the last node of the dag to the EndNode of the building dag
func (c *ChoiceBranchBuilder) NextBranch(dagToChain *Dag, err1 error) *ChoiceBranchBuilder {
	if err1 != nil {
		c.dagBuilder.appendError(err1)
	}
	nErrors := len(c.dagBuilder.errors)
	if nErrors > 0 {
		fmt.Printf("NextBranch skipped, because of %d error(s) in dagBuilder\n", nErrors)
		return c
	}

	//fmt.Println("Added simple node to a branch in choice node of Dag")
	if c.HasNextBranch() {
		c.dagBuilder.BranchNumber++
		baseBranchNumber := c.dagBuilder.BranchNumber
		// getting start.Next from the dagToChain
		startNext, _ := dagToChain.Find(dagToChain.Start.Next)
		// chains the alternative to the input dag, which is already connected to a whole series of nodes
		if !dagToChain.IsEmpty() {
			c.dagBuilder.dag.addNode(startNext)
			err := c.dagBuilder.dag.chain(c.dagBuilder.prevNode.(*ChoiceNode), startNext)
			if err != nil {
				c.dagBuilder.appendError(err)
			}
			// adds the nodes to the building dag
			for _, n := range dagToChain.Nodes {
				switch n.(type) {
				case *StartNode:
					continue
				case *EndNode:
					continue
				case *FanOutNode:
					c.dagBuilder.dag.addNode(n)
					n.setBranchId(n.GetBranchId() + baseBranchNumber)
					continue
				case *ChoiceNode:
					c.dagBuilder.dag.addNode(n)
					n.setBranchId(n.GetBranchId() + baseBranchNumber)
					continue
				default:
					c.dagBuilder.dag.addNode(n)
					n.setBranchId(n.GetBranchId() + baseBranchNumber)
					nextNode, _ := dagToChain.Find(n.GetNext()[0])
					// chain the last node(s) of the input dag to the end node of the building dag

					if n.GetNext() != nil && len(n.GetNext()) > 0 && nextNode == dagToChain.End {
						errEnd := c.dagBuilder.dag.ChainToEndNode(n)
						if errEnd != nil {
							c.dagBuilder.appendError(errEnd)
							return c
						}
					}
				}
			}
			// so we completed a branch
			c.completed++
			c.dagBuilder.branches--
		} else {
			fmt.Printf("not adding another end node\n")
			c.EndNextBranch()
		}
	} else {
		panic("There is not a NextBranch. Use EndChoiceAndBuild to end the choiceNode.")
	}
	return c
}

// EndNextBranch is used to chain the next choice branch to the end node of the dag, resulting in a no-op branch
func (c *ChoiceBranchBuilder) EndNextBranch() *ChoiceBranchBuilder {
	nErrors := len(c.dagBuilder.errors)
	if nErrors > 0 {
		fmt.Printf("NextBranch skipped, because of %d error(s) in dagBuilder\n", nErrors)
		return c
	}
	dag := &c.dagBuilder.dag

	if c.HasNextBranch() {
		c.dagBuilder.BranchNumber++ // we only increase the branch number, but we do not use in any node
		//fmt.Printf("Ending branch %d for Dag\n", c.dagBuilder.BranchNumber)
		// chain the alternative of the choice node to the end node of the building dag
		choice := c.dagBuilder.prevNode.(*ChoiceNode)
		var alternative DagNode
		if c.completed < len(choice.Alternatives) {
			x := choice.Alternatives[c.completed]
			alternative, _ = dag.Find(x)
		} else {
			alternative = choice // this is when a choice branch directly goes to end node
		}
		err := c.dagBuilder.dag.ChainToEndNode(alternative)
		if err != nil {
			c.dagBuilder.appendError(err)
			return c
		}
		// ... and we completed a branch
		c.completed++
		c.dagBuilder.branches--
		if !c.HasNextBranch() {
			c.dagBuilder.prevNode = c.dagBuilder.dag.End
		}
	} else {
		fmt.Println("warning: Useless call EndNextBranch: all branch are ended")
	}
	return c
}

func (c *ChoiceBranchBuilder) HasNextBranch() bool {
	return c.dagBuilder.branches > 0
}

// EndChoiceAndBuild connects all remaining branches to the end node and returns the dag
func (c *ChoiceBranchBuilder) EndChoiceAndBuild() (*Dag, error) {
	for c.HasNextBranch() {
		c.EndNextBranch()
		if len(c.dagBuilder.errors) > 0 {
			return nil, fmt.Errorf("build failed with errors:\n%v", c.dagBuilder.errors)
		}
	}

	return &c.dagBuilder.dag, nil
}

// ForEachBranch chains each (remaining) output of a choice node to the same subsequent node, then returns the DagBuilder
func (c *ChoiceBranchBuilder) ForEachBranch(dagger func() (*Dag, error)) *ChoiceBranchBuilder {
	choiceNode := c.dagBuilder.prevNode.(*ChoiceNode)
	// we suppose the branches 0, ..., (completed-1) are already completed
	// once := true
	remainingBranches := c.dagBuilder.branches
	for i := c.completed; i < remainingBranches; i++ {
		c.dagBuilder.BranchNumber++
		//fmt.Printf("Adding dag to branch %d\n", c.dagBuilder.BranchNumber)
		// recreates a dag executing the same function
		dagCopy, errDag := dagger()
		if errDag != nil {
			c.dagBuilder.appendError(errDag)
		}
		nextNode, _ := dagCopy.Find(dagCopy.Start.Next)
		c.dagBuilder.dag.addNode(nextNode)
		err := c.dagBuilder.dag.chain(choiceNode, nextNode)
		if err != nil {
			c.dagBuilder.appendError(errDag)
		}
		// adds the nodes to the building dag, but only once!
		for _, n := range dagCopy.Nodes {
			switch n.(type) {
			case *StartNode:
				continue
			case *EndNode:
				continue
			case *FanOutNode:
				errFanout := fmt.Errorf("you're trying to chain a fanout node to an end node. This will interrupt the execution immediately after the fanout is reached")
				c.dagBuilder.appendError(errFanout)
				continue
			default:
				n.setBranchId(c.dagBuilder.BranchNumber)
				c.dagBuilder.dag.addNode(n)
				// chain the last node(s) of the input dag to the end node of the building dag
				if n.GetNext() != nil && len(n.GetNext()) > 0 && n.GetNext()[0] == dagCopy.End.GetId() {
					errEnd := c.dagBuilder.dag.ChainToEndNode(n)
					if errEnd != nil {
						c.dagBuilder.appendError(errEnd)
						return c
					}
				}
			}

		}
		// so we completed a branch
		c.completed++
		c.dagBuilder.branches--
	}
	return c
}

func (p *ParallelBroadcastBranchBuilder) ForEachParallelBranch(dagger func() (*Dag, error)) *ParallelBroadcastBranchBuilder {
	fanOutNode := p.dagBuilder.prevNode.(*FanOutNode)
	// we suppose the branches 0, ..., (completed-1) are already completed
	remainingBranches := p.dagBuilder.branches
	for i := p.completed; i < remainingBranches; i++ {
		p.dagBuilder.BranchNumber++
		//fmt.Printf("Adding dag to branch %d\n", i)
		// recreates a dag executing the same function
		dagCopy, errDag := dagger()
		if errDag != nil {
			p.dagBuilder.appendError(errDag)
		}
		next, _ := dagCopy.Find(dagCopy.Start.Next)
		p.dagBuilder.dag.addNode(next)
		err := p.dagBuilder.dag.chain(fanOutNode, next)
		if err != nil {
			p.dagBuilder.appendError(err)
		}
		// adds the nodes to the building dag, but only once!
		for _, n := range dagCopy.Nodes {
			// chain the last node(s) of the input dag to the end node of the building dag
			switch n.(type) {
			case *StartNode:
				continue
			case *EndNode:
				continue
			case *FanOutNode:
				p.dagBuilder.appendError(fmt.Errorf("you're trying to chain a branch of a fanout node to an end node. This will interrupt the execution immediately after the fanout is reached"))
				continue
			default:
				p.dagBuilder.dag.addNode(n)
				n.setBranchId(p.dagBuilder.BranchNumber)
				if n.GetNext() != nil && len(n.GetNext()) > 0 && n.GetNext()[0] == dagCopy.End.GetId() {
					p.terminalNodes = append(p.terminalNodes, n) // we do not chain to end node, only add to terminal nodes, so that we can chain to a fan in later
				}
			}

		}
		// so we completed a branch
		p.completed++
		p.dagBuilder.branches--
	}
	return p
}

func (p *ParallelScatterBranchBuilder) ForEachParallelBranch(dagger func() (*Dag, error)) *ParallelScatterBranchBuilder {
	fanOutNode := p.dagBuilder.prevNode.(*FanOutNode)
	// we suppose the branches 0, ..., (completed-1) are already completed
	remainingBranches := p.dagBuilder.branches
	for i := p.completed; i < remainingBranches; i++ {
		p.dagBuilder.BranchNumber++
		//fmt.Printf("Adding dag to branch %d\n", i)
		// recreates a dag executing the same function
		dagCopy, errDag := dagger()
		if errDag != nil {
			p.dagBuilder.appendError(errDag)
		}
		next, _ := dagCopy.Find(dagCopy.Start.Next)
		p.dagBuilder.dag.addNode(next)
		err := p.dagBuilder.dag.chain(fanOutNode, next)
		if err != nil {
			p.dagBuilder.appendError(err)
		}
		// adds the nodes to the building dag, but only once!
		for _, n := range dagCopy.Nodes {
			// chain the last node(s) of the input dag to the end node of the building dag
			switch n.(type) {
			case *StartNode:
				continue
			case *EndNode:
				continue
			case *FanOutNode:
				p.dagBuilder.appendError(fmt.Errorf("you're trying to chain a branch of a fanout node to an end node. This will interrupt the execution immediately after the fanout is reached"))
				continue
			default:
				p.dagBuilder.dag.addNode(n)
				n.setBranchId(p.dagBuilder.BranchNumber)
				if n.GetNext() != nil && len(n.GetNext()) > 0 && n.GetNext()[0] == dagCopy.End.GetId() {
					p.terminalNodes = append(p.terminalNodes, n) // we do not chain to end node, only add to terminal nodes, so that we can chain to a fan in later
				}
			}
		}
		// so we completed a branch
		p.completed++
		p.dagBuilder.branches--
	}
	return p
}

func (p *ParallelScatterBranchBuilder) AddFanInNode(mergeMode MergeMode) *DagBuilder {
	//fmt.Println("Added fan in node after fanout in Dag")
	dag := &p.dagBuilder.dag
	fanInNode := NewFanInNode(mergeMode, p.dagBuilder.prevNode.Width(), nil)
	p.dagBuilder.BranchNumber++
	fanInNode.setBranchId(p.dagBuilder.BranchNumber)
	// TODO: set fanin inside fanout, so that we know which fanin are dealing with
	for _, n := range p.terminalNodes {
		// terminal nodes
		errAdd := n.AddOutput(dag, fanInNode.GetId())
		if errAdd != nil {
			p.dagBuilder.appendError(errAdd)
			return p.dagBuilder
		}
	}
	p.dagBuilder.dag.addNode(fanInNode)
	p.dagBuilder.prevNode = fanInNode
	// finding fanOut node, then assigning corresponding fanIn
	fanOut, ok := p.dagBuilder.dag.Find(p.fanOutId)
	if ok {
		fanOut.(*FanOutNode).AssociatedFanIn = fanInNode.Id
	} else {
		p.dagBuilder.appendError(fmt.Errorf("failed to find fanOutNode"))
	}
	return p.dagBuilder
}

func (p *ParallelBroadcastBranchBuilder) AddFanInNode(mergeMode MergeMode) *DagBuilder {
	//fmt.Println("Added fan in node after fanout in Dag")
	dag := &p.dagBuilder.dag
	fanInNode := NewFanInNode(mergeMode, p.dagBuilder.prevNode.Width(), nil)
	p.dagBuilder.BranchNumber++
	fanInNode.setBranchId(p.dagBuilder.BranchNumber)
	for _, n := range p.terminalNodes {
		// terminal nodes
		errAdd := n.AddOutput(dag, fanInNode.GetId())
		if errAdd != nil {
			p.dagBuilder.appendError(errAdd)
			return p.dagBuilder
		}
	}
	p.dagBuilder.dag.addNode(fanInNode)
	p.dagBuilder.prevNode = fanInNode
	// finding fanOut node, then assigning corresponding fanIn
	fanOut, ok := p.dagBuilder.dag.Find(p.fanOutId)
	if ok {
		fanOut.(*FanOutNode).AssociatedFanIn = fanInNode.Id
	} else {
		p.dagBuilder.appendError(fmt.Errorf("failed to find fanOutNode"))
	}
	return p.dagBuilder
}

func (p *ParallelBroadcastBranchBuilder) NextFanOutBranch(dagToChain *Dag, err1 error) *ParallelBroadcastBranchBuilder {
	if err1 != nil {
		p.dagBuilder.appendError(err1)
	}
	nErrors := len(p.dagBuilder.errors)
	if nErrors > 0 {
		fmt.Printf("NextBranch skipped, because of %d error(s) in dagBuilder\n", nErrors)
		return p
	}

	//fmt.Println("Added simple node to a branch in choice node of Dag")
	if p.HasNextBranch() {
		p.dagBuilder.BranchNumber++
		// chains the alternative to the input dag, which is already connected to a whole series of nodes
		next, _ := dagToChain.Find(dagToChain.Start.Next)
		err := p.dagBuilder.dag.chain(p.dagBuilder.prevNode, next)
		if err != nil {
			p.dagBuilder.appendError(err)
		}
		// adds the nodes to the building dag
		for _, n := range dagToChain.Nodes {
			// chain the last node(s) of the input dag to the end node of the building dag
			switch n.(type) {
			case *StartNode:
				continue
			case *EndNode:
				continue
			case *FanOutNode:
				errFanout := fmt.Errorf("you're trying to chain a fanout node to an end node. This will interrupt the execution immediately after the fanout is reached")
				p.dagBuilder.appendError(errFanout)
				continue
			default:
				p.dagBuilder.dag.addNode(n)
				n.setBranchId(p.dagBuilder.BranchNumber)
				if n.GetNext() != nil && len(n.GetNext()) > 0 && n.GetNext()[0] == dagToChain.End.GetId() {
					p.terminalNodes = append(p.terminalNodes, n)
				}
			}
		}

		// so we completed a branch
		p.completed++
		p.dagBuilder.branches--
	} else {
		p.dagBuilder.appendError(errors.New("there is not a Next ParallelBranch. Use AddFanInNode to end the FanOutNode"))
	}

	return p
}

func (p *ParallelBroadcastBranchBuilder) HasNextBranch() bool {
	return p.dagBuilder.branches > 0
}

func (b *DagBuilder) AddFailNodeAndBuild(errorName, errorMessage string) (*Dag, error) {
	nErrors := len(b.errors)
	if nErrors > 0 {
		return nil, fmt.Errorf("AddFailNodeAndBuild failed because of the following %d error(s) in dagBuilder\n%v", nErrors, b.errors)
	}

	failNode := NewFailNode(errorName, errorMessage)
	failNode.setBranchId(b.BranchNumber)

	b.dag.addNode(failNode)
	err := b.dag.chain(b.prevNode, failNode)
	if err != nil {
		return nil, fmt.Errorf("failed to chain the FailNode: %v", err)
	}
	b.prevNode = failNode
	return b.Build()
}

func (b *DagBuilder) AddSucceedNodeAndBuild(message string) (*Dag, error) {
	nErrors := len(b.errors)
	if nErrors > 0 {
		return nil, fmt.Errorf("AddSucceedNodeAndBuild failed because of the following %d error(s) in dagBuilder\n%v", nErrors, b.errors)
	}

	succeedNode := NewSucceedNode(message)
	succeedNode.setBranchId(b.BranchNumber)

	b.dag.addNode(succeedNode)
	err := b.dag.chain(b.prevNode, succeedNode)
	if err != nil {
		return nil, fmt.Errorf("failed to chain the SucceedNode: %v", err)
	}
	b.prevNode = succeedNode
	return b.Build()
}

func (b *DagBuilder) AddPassNode(result string) *DagBuilder {
	nErrors := len(b.errors)
	if nErrors > 0 {
		fmt.Printf("AddSimpleNode skipped, because of %d error(s) in dagBuilder\n", nErrors)
		return b
	}

	passNode := NewPassNode(result)
	passNode.setBranchId(b.BranchNumber)

	b.dag.addNode(passNode)
	err := b.dag.chain(b.prevNode, passNode)
	if err != nil {
		b.appendError(err)
		return b
	}

	b.prevNode = passNode
	return b
}

func (b *DagBuilder) AddWaitNode(seconds int) *DagBuilder {
	nErrors := len(b.errors)
	if nErrors > 0 {
		fmt.Printf("AddSimpleNode skipped, because of %d error(s) in dagBuilder\n", nErrors)
		return b
	}

	passNode := NewWaitNode(seconds)
	passNode.setBranchId(b.BranchNumber)

	b.dag.addNode(passNode)
	err := b.dag.chain(b.prevNode, passNode)
	if err != nil {
		b.appendError(err)
		return b
	}

	b.prevNode = passNode
	return b
}

func (b *DagBuilder) AddWaitNodeWithTimestamp(timestampRFC3339 string) *DagBuilder {
	nErrors := len(b.errors)
	if nErrors > 0 {
		fmt.Printf("AddSimpleNode skipped, because of %d error(s) in dagBuilder\n", nErrors)
		return b
	}
	timestamp, ok := parseRFC3339(timestampRFC3339)
	if !ok {
		b.appendError(fmt.Errorf("failed to parse timestamp RFC3339 from string %s", timestampRFC3339))
		return b
	}
	passNode := NewWaitNodeFromTimestamp(timestamp)
	passNode.setBranchId(b.BranchNumber)

	b.dag.addNode(passNode)
	err := b.dag.chain(b.prevNode, passNode)
	if err != nil {
		b.appendError(err)
		return b
	}

	b.prevNode = passNode
	return b
}

// Build ends the single branch with an EndNode. If there is more than one branch, it panics!
func (b *DagBuilder) Build() (*Dag, error) {
	switch b.prevNode.(type) {
	case nil:
		return &b.dag, nil
	case *EndNode:
		return &b.dag, nil
	default:
		err := b.dag.ChainToEndNode(b.prevNode)
		if err != nil {
			return nil, fmt.Errorf("failed to chain to end node: %v", err)
		}
	}
	return &b.dag, nil
}

func CreateEmptyDag() (*Dag, error) {
	return NewDagBuilder().Build()
}

// CreateSequenceDag if successful, returns a dag pointer with a sequence of Simple Nodes
func CreateSequenceDag(funcs ...*function.Function) (*Dag, error) {
	builder := NewDagBuilder()
	for _, f := range funcs {
		builder = builder.AddSimpleNode(f)
	}
	return builder.Build()
}

func LambdaSequenceDag(funcs ...*function.Function) func() (*Dag, error) {
	return func() (*Dag, error) { return CreateSequenceDag(funcs...) }
}

// CreateChoiceDag if successful, returns a dag with one Choice Node with each branch consisting of the same sub-dag
func CreateChoiceDag(dagger func() (*Dag, error), condArr ...Condition) (*Dag, error) {
	return NewDagBuilder().
		AddChoiceNode(condArr...).
		ForEachBranch(dagger).
		EndChoiceAndBuild()
}

// CreateScatterSingleFunctionDag if successful, returns a dag with one fan out, N simple node with the same function
// and then a fan in node that merges all the result in an array.
func CreateScatterSingleFunctionDag(fun *function.Function, fanOutDegree int) (*Dag, error) {
	return NewDagBuilder().
		AddScatterFanOutNode(fanOutDegree).
		ForEachParallelBranch(func() (*Dag, error) { return CreateSequenceDag(fun) }).
		AddFanInNode(AddToArrayEntry).
		Build()
}

// CreateBroadcastDag if successful, returns a dag with one fan out node, N simple nodes with different functions and a fan in node
// The number of branches is defined by the number of given functions
func CreateBroadcastDag(dagger func() (*Dag, error), fanOutDegree int) (*Dag, error) {
	return NewDagBuilder().
		AddBroadcastFanOutNode(fanOutDegree).
		ForEachParallelBranch(dagger).
		AddFanInNode(AddNewMapEntry).
		Build()
}

func CreateAdHOCBroadcastDag(dagger func() (*Dag, error), fanOutDegree int, funct *function.Function) (*Dag, error) {
	return NewDagBuilder().
		AddBroadcastFanOutNode(fanOutDegree).
		ForEachParallelBranch(dagger).
		AddFanInNode(AddNewMapEntry).
		AddSimpleNode(funct).
		Build()
}

// CreateBroadcastMultiFunctionDag if successful, returns a dag with one fan out node, each branch chained with a different dag that run in parallel, and a fan in node.
// The number of branch is defined as the number of dagger functions.
func CreateBroadcastMultiFunctionDag(dagger ...func() (*Dag, error)) (*Dag, error) {
	builder := NewDagBuilder().
		AddBroadcastFanOutNode(len(dagger))
	for _, dagFn := range dagger {
		builder = builder.NextFanOutBranch(dagFn())
	}
	return builder.
		AddFanInNode(AddNewMapEntry).
		Build()
}

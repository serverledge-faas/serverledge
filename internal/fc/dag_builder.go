package fc

import "C"
import (
	"errors"
	"fmt"
	"github.com/grussorusso/serverledge/internal/function"
)

// DagBuilder is a utility struct that helps easily define the Dag, using the Builder pattern.
// Use NewDagBuilder() to safely initialize it. Then use the available methods to iteratively build the dag.
// Finally use Build() to get the complete Dag.
type DagBuilder struct {
	dag      Dag
	branches int
	prevNode DagNode
	errors   []error
}

func (b *DagBuilder) appendError(err error) {
	b.errors = append(b.errors, err)
}

type ChoiceBranchBuilder struct {
	dagBuilder DagBuilder
	completed  int
}

// ParallelScatterBranchBuilder can only hold the same dag executed in parallel in each branch
type ParallelScatterBranchBuilder struct {
	dagBuilder    DagBuilder
	completed     int
	terminalNodes []DagNode
}

// ParallelBroadcastBranchBuilder can hold different dags executed in parallel
type ParallelBroadcastBranchBuilder struct {
	dagBuilder    DagBuilder
	completed     int
	terminalNodes []DagNode
}

func NewDagBuilder() DagBuilder {
	db := DagBuilder{
		dag:      NewDAG(),
		branches: 1,
		errors:   make([]error, 0),
	}
	db.prevNode = db.dag.Start
	return db
}

// AddSimpleNode connects a simple node to the previous node
func (b DagBuilder) AddSimpleNode(f *function.Function) DagBuilder {
	simpleNode := NewSimpleNode(f.Name)
	err := b.dag.Chain(b.prevNode, simpleNode)
	if err != nil {
		b.appendError(err)
		return b
	}
	b.dag.addNode(simpleNode)
	b.prevNode = simpleNode
	fmt.Println("Added simple node to Dag")
	return b
}

// AddChoiceNode connects a choice node to the previous node. From the choice node, multiple branch are created and each one of those must be fully defined
func (b DagBuilder) AddChoiceNode(conditions ...Condition) ChoiceBranchBuilder {
	nErrors := len(b.errors)
	if nErrors > 0 {
		fmt.Printf("NextBranch skipped, because of %d error(s) in dagBuilder\n", nErrors)
		return ChoiceBranchBuilder{dagBuilder: b, completed: 0}
	}

	fmt.Println("Added choice node to Dag")
	choiceNode := NewChoiceNode(conditions)

	b.branches = len(conditions)
	err := b.dag.Chain(b.prevNode, choiceNode)
	if err != nil {
		b.appendError(err)
		return ChoiceBranchBuilder{dagBuilder: b, completed: 0}
	}
	b.dag.addNode(choiceNode)
	b.prevNode = choiceNode
	b.dag.Width = len(conditions)
	emptyBranches := make([]DagNode, 0, b.branches)
	choiceNode.Alternatives = emptyBranches
	// we construct a new slice with capacity (b.branches) and size 0
	// Here we cannot chain directly, because we do not know which alternative to chain to which node
	// so we return a ChoiceBranchBuilder
	return ChoiceBranchBuilder{dagBuilder: b, completed: 0}
}

// AddScatterFanOutNode connects a fanout node to the previous node. From the fanout node, multiple branch are created and each one of those must be fully defined, eventually ending in a FanInNode
func (b DagBuilder) AddScatterFanOutNode(fanOutDegree int) ParallelScatterBranchBuilder {
	nErrors := len(b.errors)
	if nErrors > 0 {
		fmt.Printf("NextBranch skipped, because of %d error(s) in dagBuilder\n", nErrors)
		return ParallelScatterBranchBuilder{dagBuilder: b, terminalNodes: make([]DagNode, 0)}
	}
	if fanOutDegree <= 1 {
		b.appendError(errors.New("fanOutDegree should be at least 1"))
		return ParallelScatterBranchBuilder{dagBuilder: b, terminalNodes: make([]DagNode, 0)}
	}
	fanOut := NewFanOutNode(fanOutDegree, Scatter)

	err := b.dag.Chain(b.prevNode, fanOut)
	if err != nil {
		b.appendError(err)
		return ParallelScatterBranchBuilder{dagBuilder: b, completed: 0, terminalNodes: make([]DagNode, 0)}
	}
	b.dag.addNode(fanOut)
	fmt.Println("Added fan out node to Dag")
	b.branches = fanOutDegree
	b.prevNode = fanOut
	b.dag.Width = fanOutDegree

	return ParallelScatterBranchBuilder{dagBuilder: b, completed: 0, terminalNodes: make([]DagNode, 0)}
}

// AddBroadcastFanOutNode connects a fanout node to the previous node. From the fanout node, multiple branch are created and each one of those must be fully defined, eventually ending in a FanInNode
func (b DagBuilder) AddBroadcastFanOutNode(fanOutDegree int) ParallelBroadcastBranchBuilder {
	nErrors := len(b.errors)
	if nErrors > 0 {
		fmt.Printf("NextBranch skipped, because of %d error(s) in dagBuilder\n", nErrors)
		return ParallelBroadcastBranchBuilder{dagBuilder: b, completed: 0, terminalNodes: make([]DagNode, 0)}
	}
	fanOut := NewFanOutNode(fanOutDegree, Broadcast)

	err := b.dag.Chain(b.prevNode, fanOut)
	if err != nil {
		b.appendError(err)
		return ParallelBroadcastBranchBuilder{dagBuilder: b, completed: 0, terminalNodes: make([]DagNode, 0)}
	}
	b.dag.addNode(fanOut)
	b.branches = fanOutDegree
	b.prevNode = fanOut
	b.dag.Width = fanOutDegree

	return ParallelBroadcastBranchBuilder{dagBuilder: b, completed: 0, terminalNodes: make([]DagNode, 0)}
}

// NextBranch is used to chain the next branch to a Dag and then returns the ChoiceBranchBuilder.
// Tip: use a NewDagBuilder() as a parameter, instead of manually creating the Dag!
// Internally, NextBranch replaces the StartNode of the input dag with the choice alternative
// and chains the last node of the dag to the EndNode of the building dag
func (c ChoiceBranchBuilder) NextBranch(dagToChain *Dag, err1 error) ChoiceBranchBuilder {
	if err1 != nil {
		c.dagBuilder.appendError(err1)
	}
	nErrors := len(c.dagBuilder.errors)
	if nErrors > 0 {
		fmt.Printf("NextBranch skipped, because of %d error(s) in dagBuilder\n", nErrors)
		return c
	}

	fmt.Println("Added simple node to a branch in choice node of Dag")
	if c.HasNextBranch() {
		// adds the nodes to the building dag
		for i, n := range dagToChain.Nodes {
			if i == 0 {
				// chains the alternative to the input dag, which is already connected to a whole series of nodes
				err := c.dagBuilder.dag.Chain(c.dagBuilder.prevNode.(*ChoiceNode), dagToChain.Start.Next)
				if err != nil {
					c.dagBuilder.appendError(err)
				}
			}
			c.dagBuilder.dag.addNode(n)
			// chain the last node(s) of the input dag to the end node of the building dag
			if n.GetNext() != nil && len(n.GetNext()) > 0 && n.GetNext()[0] == dagToChain.End {
				switch n.(type) {
				case *FanOutNode:
					errFanout := fmt.Errorf("you're trying to chain a fanout node to an end node. This will interrupt the execution immediately after the fanout is reached")
					c.dagBuilder.appendError(errFanout)
					continue
				default:
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
		panic("There is not a NextBranch. Use EndChoiceAndBuild to end the choiceNode.")
	}
	return c
}

// EndNextBranch is used to chain the next choice branch to the end node of the dag
func (c ChoiceBranchBuilder) EndNextBranch() ChoiceBranchBuilder {
	nErrors := len(c.dagBuilder.errors)
	if nErrors > 0 {
		fmt.Printf("NextBranch skipped, because of %d error(s) in dagBuilder\n", nErrors)
		return c
	}

	if c.HasNextBranch() {
		fmt.Println("Ending branch for Dag")
		// chain the alternative of the choice node to the end node of the building dag
		err := c.dagBuilder.dag.ChainToEndNode(c.dagBuilder.prevNode.(*ChoiceNode).Alternatives[c.completed])
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

func (c ChoiceBranchBuilder) HasNextBranch() bool {
	return c.dagBuilder.branches > 0
}

// EndChoiceAndBuild connects all remaining branches to the end node and builds the dag
func (c ChoiceBranchBuilder) EndChoiceAndBuild() (*Dag, error) {
	for c.HasNextBranch() {
		c.EndNextBranch()
		if len(c.dagBuilder.errors) > 0 {
			return nil, fmt.Errorf("build failed with errors:\n%v", c.dagBuilder.errors)
		}
	}

	return &c.dagBuilder.dag, nil
}

// ForEachBranch chains each (remaining) output of a choice node to the same subsequent node, then returns the DagBuilder
func (c ChoiceBranchBuilder) ForEachBranch(dagger func() (*Dag, error)) ChoiceBranchBuilder {
	choiceNode := c.dagBuilder.prevNode.(*ChoiceNode)
	// we suppose the branches 0, ..., (completed-1) are already completed
	// once := true
	remainingBranches := c.dagBuilder.branches
	for i := c.completed; i < remainingBranches; i++ {
		fmt.Printf("Adding dag to branch %d\n", i)
		// recreates a dag executing the same function
		dagCopy, errDag := dagger()
		if errDag != nil {
			c.dagBuilder.appendError(errDag)
		}
		err := c.dagBuilder.dag.Chain(choiceNode, dagCopy.Start.Next)
		if err != nil {
			c.dagBuilder.appendError(errDag)
		}
		// adds the nodes to the building dag, but only once!
		for _, n := range dagCopy.Nodes {
			c.dagBuilder.dag.addNode(n)
			// chain the last node(s) of the input dag to the end node of the building dag
			if n.GetNext() != nil && len(n.GetNext()) > 0 && n.GetNext()[0] == dagCopy.End {
				switch n.(type) {
				case *FanOutNode:
					errFanout := fmt.Errorf("you're trying to chain a fanout node to an end node. This will interrupt the execution immediately after the fanout is reached")
					c.dagBuilder.appendError(errFanout)
					continue
				default:
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

func (p ParallelBroadcastBranchBuilder) ForEachParallelBranch(dagger func() (*Dag, error)) ParallelBroadcastBranchBuilder {
	fanOutNode := p.dagBuilder.prevNode.(*FanOutNode)
	// we suppose the branches 0, ..., (completed-1) are already completed
	remainingBranches := p.dagBuilder.branches
	for i := p.completed; i < remainingBranches; i++ {
		fmt.Printf("Adding dag to branch %d\n", i)
		// recreates a dag executing the same function
		dagCopy, errDag := dagger()
		if errDag != nil {
			p.dagBuilder.appendError(errDag)
		}
		err := p.dagBuilder.dag.Chain(fanOutNode, dagCopy.Start.Next)
		if err != nil {
			p.dagBuilder.appendError(err)
		}
		// adds the nodes to the building dag, but only once!
		for _, n := range dagCopy.Nodes {
			p.dagBuilder.dag.addNode(n)
			// chain the last node(s) of the input dag to the end node of the building dag
			if n.GetNext() != nil && len(n.GetNext()) > 0 && n.GetNext()[0] == dagCopy.End {
				switch n.(type) {
				case *FanOutNode:
					p.dagBuilder.appendError(fmt.Errorf("you're trying to chain a branch of a fanout node to an end node. This will interrupt the execution immediately after the fanout is reached"))
					continue
				default:
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

func (p ParallelScatterBranchBuilder) ForEachParallelBranch(dagger func() (*Dag, error)) ParallelScatterBranchBuilder {
	fanOutNode := p.dagBuilder.prevNode.(*FanOutNode)
	// we suppose the branches 0, ..., (completed-1) are already completed
	remainingBranches := p.dagBuilder.branches
	for i := p.completed; i < remainingBranches; i++ {
		fmt.Printf("Adding dag to branch %d\n", i)
		// recreates a dag executing the same function
		dagCopy, errDag := dagger()
		if errDag != nil {
			p.dagBuilder.appendError(errDag)
		}
		err := p.dagBuilder.dag.Chain(fanOutNode, dagCopy.Start.Next)
		if err != nil {
			p.dagBuilder.appendError(err)
		}
		// adds the nodes to the building dag, but only once!
		for _, n := range dagCopy.Nodes {
			p.dagBuilder.dag.addNode(n)
			// chain the last node(s) of the input dag to the end node of the building dag
			if n.GetNext() != nil && len(n.GetNext()) > 0 && n.GetNext()[0] == dagCopy.End {
				switch n.(type) {
				case *FanOutNode:
					p.dagBuilder.appendError(fmt.Errorf("you're trying to chain a branch of a fanout node to an end node. This will interrupt the execution immediately after the fanout is reached"))
					continue
				default:
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

func (p ParallelScatterBranchBuilder) AddFanInNode(mergeMode MergeMode) DagBuilder {
	fmt.Println("Added fan in node after fanout in Dag")
	fanInNode := NewFanInNode(mergeMode, nil)
	fanInNode.FanInDegree = p.dagBuilder.prevNode.Width()
	for _, n := range p.terminalNodes {
		// terminal nodes
		errAdd := n.AddOutput(fanInNode)
		if errAdd != nil {
			p.dagBuilder.appendError(errAdd)
			return p.dagBuilder
		}
	}
	p.dagBuilder.dag.addNode(fanInNode)
	p.dagBuilder.prevNode = fanInNode
	return p.dagBuilder
}

func (p ParallelBroadcastBranchBuilder) AddFanInNode(mergeMode MergeMode) DagBuilder {
	fmt.Println("Added fan in node after fanout in Dag")
	fanInNode := NewFanInNode(mergeMode, nil)
	fanInNode.FanInDegree = p.dagBuilder.prevNode.Width()
	for _, n := range p.terminalNodes {
		// terminal nodes
		errAdd := n.AddOutput(fanInNode)
		if errAdd != nil {
			p.dagBuilder.appendError(errAdd)
			return p.dagBuilder
		}
	}
	p.dagBuilder.dag.addNode(fanInNode)
	p.dagBuilder.prevNode = fanInNode
	return p.dagBuilder
}

func (p ParallelBroadcastBranchBuilder) NextFanOutBranch(dagToChain *Dag, err1 error) ParallelBroadcastBranchBuilder {
	if err1 != nil {
		p.dagBuilder.appendError(err1)
	}
	nErrors := len(p.dagBuilder.errors)
	if nErrors > 0 {
		fmt.Printf("NextBranch skipped, because of %d error(s) in dagBuilder\n", nErrors)
		return p
	}

	fmt.Println("Added simple node to a branch in choice node of Dag")
	if p.HasNextBranch() {
		// adds the nodes to the building dag
		for i, n := range dagToChain.Nodes {
			if i == 0 {
				// chains the alternative to the input dag, which is already connected to a whole series of nodes
				err := p.dagBuilder.dag.Chain(p.dagBuilder.prevNode, dagToChain.Start.Next)
				if err != nil {
					p.dagBuilder.appendError(err)
				}
			}
			p.dagBuilder.dag.addNode(n)
			// chain the last node(s) of the input dag to the end node of the building dag
			if n.GetNext() != nil && len(n.GetNext()) > 0 && n.GetNext()[0] == dagToChain.End {
				switch n.(type) {
				case *FanOutNode:
					errFanout := fmt.Errorf("you're trying to chain a fanout node to an end node. This will interrupt the execution immediately after the fanout is reached")
					p.dagBuilder.appendError(errFanout)
					continue
				default:
					// TODO: non così errEnd := p.dagBuilder.dag.ChainToEndNode(n)
					p.terminalNodes = append(p.terminalNodes, n)
				}
			}
		}

		// so we completed a branch
		p.completed++
		p.dagBuilder.branches--
	} else {
		p.dagBuilder.appendError(errors.New("there is not a Next ParallelBranch. Use AddFanInNode to end the FanOutNode."))
	}

	return p
}

func (p ParallelBroadcastBranchBuilder) HasNextBranch() bool {
	return p.dagBuilder.branches > 0
}

// Build ends the single branch with an EndNode. If there is more than one branch, it panics!
func (b DagBuilder) Build() (*Dag, error) {

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

// CreateChoiceDag if successful, returns a dag with one Choice Node with each branch consisting of the same sub-dag
func CreateChoiceDag(dagger func() (*Dag, error), condArr ...Condition) (*Dag, error) {
	return NewDagBuilder().
		AddChoiceNode(condArr...).
		ForEachBranch(dagger).
		EndChoiceAndBuild()
}

// CreateScatterSingleFunctionDag if successful, returns a dag with one fan out, N simple node with the same function
// and then a fan in node that merges all the result in an array. TODO: The number N is defined by the input cardinality (i.e. each map key is sent to different nodes / each entry in an array map key is ent to a different node)
func CreateScatterSingleFunctionDag(fun *function.Function, fanOutdegree int) (*Dag, error) {
	return NewDagBuilder().
		AddScatterFanOutNode(fanOutdegree).
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

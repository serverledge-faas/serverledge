package fc

// void is used to avoid consuming memory in the map (set)
type void struct{}

var null void

// NodeSet is a utility struct that defines a simple ordered Set of DagNodes
type NodeSet struct {
	elements map[DagNode]void
	ordered  []DagNode
}

func NewNodeSet() NodeSet {
	return NodeSet{elements: make(map[DagNode]void)}
}

func NewNodeSetFrom(slice []DagNode) NodeSet {
	ns := NewNodeSet()
	ns.AddAll(slice)
	return ns
}

func (set *NodeSet) Contains(node DagNode) bool {
	_, exists := set.elements[node]
	return exists
}

// AddIfNotExists adds the node to the set if exists. Returns true if node is added, false if already exists
func (set *NodeSet) AddIfNotExists(node DagNode) bool {
	_, ok := set.elements[node]
	// If the key does not exist, then add to the set
	if !ok {
		set.elements[node] = null
		set.ordered = append(set.ordered, node)
		return true
	}
	return false
}

// not tested, not used
func (set *NodeSet) removeIfExists(node DagNode) bool {
	if set.Contains(node) {
		delete(set.elements, node)
		return true
	}
	return false
}

// GetNodes returns an ordered list with all Nodes in the set
func (set *NodeSet) GetNodes() []DagNode {
	return set.ordered
}

func (set *NodeSet) AddAll(nodes []DagNode) int {
	nAdded := 0
	for _, node := range nodes {
		added := set.AddIfNotExists(node)
		if added {
			nAdded++
		}
	}
	return nAdded
}

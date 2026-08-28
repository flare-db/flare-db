use std::collections::HashSet;

use petgraph::{Direction, graph::NodeIndex};

use crate::fusion::pipeline::{ConsumerMetaData, ExecutableGraph, ExecutableNode};

/// Scheduler that manages execution state for an `ExecutableGraph`.
///
/// Tracks executed and in-flight nodes and provides helpers to query ready
/// nodes and edge metadata.
pub struct NodeScheduler {
    graph: ExecutableGraph,
    executed: HashSet<NodeIndex>,
    in_flight: HashSet<NodeIndex>,
}

impl NodeScheduler {
    pub fn new(graph: ExecutableGraph) -> Self {
        Self {
            graph,
            executed: HashSet::new(),
            in_flight: HashSet::new(),
        }
    }

    /// Returns the next ready nodes to execute.
    ///
    /// A node is considered ready when it is not in the `executed` or
    /// `in_flight` sets and all of its predecessor nodes have been executed.
    /// Nodes returned by this method are marked as in-flight.
    pub fn next_nodes(&mut self) -> Vec<(NodeIndex, ExecutableNode)> {
        let mut next = Vec::new();

        for idx in self.graph.get_executable_graph().node_indices() {
            if self.executed.contains(&idx) || self.in_flight.contains(&idx) {
                continue;
            }

            let all_predecessors_executed = self
                .graph
                .get_executable_graph()
                .neighbors_directed(idx, Direction::Incoming)
                .all(|pred| self.executed.contains(&pred));

            if all_predecessors_executed {
                self.in_flight.insert(idx);
                next.push((idx, self.graph.get_executable_graph()[idx].clone()));
            }
            // is
        }

        next
    }

    /// Marks a node as completed.
    ///
    /// Removes the node from the in-flight set and inserts it into the
    /// executed set.
    pub fn mark_complete(&mut self, idx: NodeIndex) {
        self.in_flight.remove(&idx);
        self.executed.insert(idx);
    }

    /// Returns metadata for an incoming edge to `idx`, if present.
    ///
    /// - If the node has an incoming edge, returns that edge's
    ///   `ConsumerMetaData`.
    /// - If the node has no predecessors (i.e. it's a root), returns the
    ///   graph's root metadata.
    /// - Otherwise returns `None`.
    pub fn input_edge_metadata(&self, idx: NodeIndex) -> Option<ConsumerMetaData> {
        let graph = self.graph.get_executable_graph();

        if let Some(edge) = graph.edges_directed(idx, Direction::Incoming).next() {
            return Some(edge.weight().clone());
        }

        if graph
            .neighbors_directed(idx, Direction::Incoming)
            .next()
            .is_none()
        {
            return Some(self.root_metadata().clone());
        }

        None
    }

    /// Returns metadata for the first outgoing edge from `idx`, if any.
    pub fn output_edge_metadata(&self, idx: NodeIndex) -> Option<ConsumerMetaData> {
        self.graph
            .get_executable_graph()
            .edges_directed(idx, Direction::Outgoing)
            .next()
            .map(|edge| edge.weight().clone())
    }

    /// Returns the root `ConsumerMetaData` for the graph.
    pub fn root_metadata(&self) -> &ConsumerMetaData {
        self.graph.get_root_metadata()
    }

    /// Returns `true` when every node in the graph has been executed.
    pub fn is_complete(&self) -> bool {
        self.executed.len() == self.graph.get_executable_graph().node_count()
    }
    // set_job_store
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use petgraph::Graph;

    use crate::engine::scheduler::NodeScheduler;
    use crate::fusion::pipeline::{ConsumerMetaData, ExecutableGraph, ExecutableNode};
    use crate::jobservice::urns::beam_urns;
    use crate::transforms::from_urn;

    fn runner_node(name: &str) -> ExecutableNode {
        ExecutableNode::Runner(from_urn(
            beam_urns::IMPULSE_TRANSFORM,
            name.to_string(),
            HashMap::new(),
            HashMap::new(),
        ))
    }

    fn dummy_metadata(name: &str) -> ConsumerMetaData {
        ConsumerMetaData {
            producer_transform_id: format!("producer-{}", name),
            produced_pcol_id: format!("pcol-{}", name),
            coder_id: format!("coder-{}", name),
            component_coder: None,
            consumer_transfrom_id: format!("consumer-{}", name),
        }
    }

    fn graph_for_test(
        graph: Graph<ExecutableNode, ConsumerMetaData>,
        root_metadata: ConsumerMetaData,
    ) -> ExecutableGraph {
        ExecutableGraph::from_graph_for_test(graph, root_metadata)
    }

    #[test]
    fn next_nodes_linear_chain() {
        let mut graph = Graph::<ExecutableNode, ConsumerMetaData>::new();
        let a = graph.add_node(runner_node("A"));
        let b = graph.add_node(runner_node("B"));
        let c = graph.add_node(runner_node("C"));

        graph.add_edge(a, b, dummy_metadata("ab"));
        graph.add_edge(b, c, dummy_metadata("bc"));

        let executable_graph = graph_for_test(graph, dummy_metadata("root"));
        let mut scheduler = NodeScheduler::new(executable_graph);

        let ready = scheduler.next_nodes();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].0, a);

        scheduler.mark_complete(a);
        let ready = scheduler.next_nodes();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].0, b);

        scheduler.mark_complete(b);
        let ready = scheduler.next_nodes();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].0, c);
    }

    #[test]
    fn next_nodes_fan_out() {
        let mut graph = Graph::<ExecutableNode, ConsumerMetaData>::new();
        let a = graph.add_node(runner_node("A"));
        let b = graph.add_node(runner_node("B"));
        let c = graph.add_node(runner_node("C"));

        graph.add_edge(a, b, dummy_metadata("ab"));
        graph.add_edge(a, c, dummy_metadata("ac"));

        let executable_graph = graph_for_test(graph, dummy_metadata("root"));
        let mut scheduler = NodeScheduler::new(executable_graph);

        let ready = scheduler.next_nodes();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].0, a);

        scheduler.mark_complete(a);
        let ready = scheduler.next_nodes();
        assert_eq!(ready.len(), 2);
        let ready_indices: HashSet<_> = ready.into_iter().map(|(idx, _)| idx).collect();
        assert!(ready_indices.contains(&b));
        assert!(ready_indices.contains(&c));
    }

    #[test]
    fn next_nodes_fan_in_requires_both_parents() {
        let mut graph = Graph::<ExecutableNode, ConsumerMetaData>::new();
        let a = graph.add_node(runner_node("A"));
        let b = graph.add_node(runner_node("B"));
        let c = graph.add_node(runner_node("C"));

        graph.add_edge(a, c, dummy_metadata("ac"));
        graph.add_edge(b, c, dummy_metadata("bc"));

        let executable_graph = graph_for_test(graph, dummy_metadata("root"));
        let mut scheduler = NodeScheduler::new(executable_graph);

        let ready = scheduler.next_nodes();
        assert_eq!(ready.len(), 2);
        let ready_indices: HashSet<_> = ready.into_iter().map(|(idx, _)| idx).collect();
        assert!(ready_indices.contains(&a));
        assert!(ready_indices.contains(&b));

        scheduler.mark_complete(a);
        let ready = scheduler.next_nodes();
        assert_eq!(ready.len(), 0);

        scheduler.mark_complete(b);
        let ready = scheduler.next_nodes();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].0, c);
    }
}

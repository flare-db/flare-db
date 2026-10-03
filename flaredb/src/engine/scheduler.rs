use std::collections::HashMap;

use petgraph::{Direction, graph::NodeIndex};

use crate::engine::watermark::WatermarkManager;
use crate::fusion::pipeline::{ConsumerMetaData, ExecutableGraph, ExecutableNode};

/// Scheduler that manages execution state for an `ExecutableGraph`.
///
/// Eligibility is delegated to [`WatermarkManager`], the single owner of
/// per-stage pending/in-flight and watermark state. In the bounded case a node
/// is ready once every main input PCollection has been produced by a completing
/// predecessor; this replaces the previous all-predecessors-executed check,
/// whose executed/in-flight bookkeeping it duplicated.
pub struct NodeScheduler {
    graph: ExecutableGraph,
    bookkeeping: WatermarkManager,
    /// Stage id to graph node index.
    index_of: HashMap<String, NodeIndex>,
}

impl NodeScheduler {
    pub fn new(graph: ExecutableGraph) -> Self {
        let bookkeeping = WatermarkManager::from_executable_graph(&graph);
        let mut index_of = HashMap::new();
        for index in graph.get_executable_graph().node_indices() {
            index_of.insert(graph.get_executable_graph()[index].id(), index);
        }
        Self {
            graph,
            bookkeeping,
            index_of,
        }
    }

    /// Returns the next ready nodes to execute, ordered by stage id.
    ///
    /// A node is ready when all of its main input PCollections have been
    /// produced and it has no bundle in flight. Nodes returned by this method
    /// are marked as in-flight.
    pub fn next_nodes(&mut self) -> Vec<(NodeIndex, ExecutableNode)> {
        let mut next = Vec::new();

        for stage in self.bookkeeping.ready_stages() {
            let Some(&index) = self.index_of.get(&stage) else {
                continue;
            };
            if self.bookkeeping.start_bundle(&stage).is_err() {
                continue;
            }
            next.push((index, self.graph.get_executable_graph()[index].clone()));
        }

        next
    }

    /// Marks a node as completed, propagating its outputs to consumers.
    pub fn mark_complete(&mut self, idx: NodeIndex) {
        let stage = self.graph.get_executable_graph()[idx].id();
        let _ = self.bookkeeping.complete_bundle(&stage);
    }

    /// The shared per-stage watermark and eligibility state.
    pub fn bookkeeping(&self) -> &WatermarkManager {
        &self.bookkeeping
    }

    /// Mutable access to the shared per-stage watermark and eligibility state.
    pub fn bookkeeping_mut(&mut self) -> &mut WatermarkManager {
        &mut self.bookkeeping
    }

    /// Returns metadata for every incoming edge to `idx`.
    ///
    /// - If the node has incoming edges, returns each edge's `ConsumerMetaData`
    ///   (a fan-in node such as a Flatten has one entry per input).
    /// - If the node has no predecessors (i.e. it's a root), returns a
    ///   single-element vector holding the graph's root metadata.
    pub fn input_edge_metadata(&self, idx: NodeIndex) -> Vec<ConsumerMetaData> {
        let graph = self.graph.get_executable_graph();

        let incoming: Vec<ConsumerMetaData> = graph
            .edges_directed(idx, Direction::Incoming)
            .map(|edge| edge.weight().clone())
            .collect();

        if incoming.is_empty() {
            // sometimes, a graph might have multiple roots
            // we need to identify the apporiate inpluse/root for a pirticular transfrom and return it
            vec![self.root_metadata().clone()]
        } else {
            incoming
        }
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
        self.bookkeeping.is_complete()
    }
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

package com.alibaba.polardbx.executor.ddl.newengine.dag;

import com.alibaba.polardbx.common.IdGenerator;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.util.HashSet;
import java.util.List;

public class DirectedAcyclicGraphTest {

    private static final TestCase[] TEST_CASES = new TestCase[] {
        // No cycle
        new TestCase(new String[] {"1->2", "2->3", "3->4", "4->5"}, false, "3", "2", "4"),
        // No cycle
        new TestCase(new String[] {"1->2", "2->3", "2->4", "3->5", "4->6", "5->7", "6->7", "7->8"}, false, "7", "5, 6",
            "8"),
        // No cycle
        new TestCase(new String[] {"1->3", "2->3", "3->4", "4->7", "5->7", "6->7"}, false, "3", "1, 2", "4"),
        // Has one cycle
        new TestCase(new String[] {"1->2", "2->3", "3->4", "4->5", "5->1"}, true, "", "", ""),
        // Has one cycle
        new TestCase(new String[] {"1->2", "2->3", "3->4", "4->5", "3->1"}, true, "", "", ""),
        // Has two cycles
        new TestCase(new String[] {"1->2", "2->3", "3->1", "3->4", "4->5", "5->3", "5->6", "6->7"}, true, "", "", ""),
        // No cycle
        new TestCase(new String[] {"1->2", "2->3", "3->4", "5->6", "6->7", "7->8"}, false, "6", "5", "7"),
        new TestCase(new String[] {
            "1->11", "1->12", "11->111", "11->112", "11->113", "12->121", "12->122", "12->123", "111->1111",
            "1111->1112", "1112->1113", "1113->114", "112->1121", "1121->1122", "1122->1123", "1123->115", "113->1131",
            "1131->1132", "1132->1133", "1133->116", "121->1211", "1211->1212", "1212->1213", "1213->124", "122->1221",
            "1221->1222", "1222->1223", "1223->125", "123->1231", "1231->1232", "1232->1233", "1233->126", "114->13",
            "115->13", "116->13", "124->14", "125->14", "126->14", "13->2", "14->2"}, false, "14", "124, 125, 126",
            "2")};

    private static final DirectedAcyclicGraph[] DAGS = new DirectedAcyclicGraph[TEST_CASES.length];
    private static final Boolean[] HAS_CYCLES = new Boolean[TEST_CASES.length];

    protected static TestEmptyTask newTask(String str) {
        return new TestEmptyTask(str);
    }

    @BeforeClass
    public static void init() {
        // Build all DAGs.
        for (int i = 0; i < TEST_CASES.length; i++) {
            DAGS[i] = buildDag(TEST_CASES[i].edges);
            HAS_CYCLES[i] = DAGS[i].hasCycle();
        }
    }

    public static DirectedAcyclicGraph buildDag(String[] dagStr) {
        DirectedAcyclicGraph dag = DirectedAcyclicGraph.create();
        for (String edge : dagStr) {
            String[] vertexes = edge.split("->");
            dag.addEdge(newTask(vertexes[0]), newTask(vertexes[1]));
        }

        return dag;
    }

    @Test
    public void testCycle() {
        for (int i = 0; i < TEST_CASES.length; i++) {
            printTestHead(i);
            // Check if there is a cycle.
            System.out.println(HAS_CYCLES[i] ? "Found cycles" : "OK - no cycle");
            Assert.assertTrue(TEST_CASES[i].hasCycle == HAS_CYCLES[i]);
            printTestTail(i);
        }
    }

    @Test
    public void testClone() {
        for (int i = 0; i < DAGS.length; i++) {
            printTestHead(i);
            DirectedAcyclicGraph dag = DAGS[i];
            DirectedAcyclicGraph copied = dag.clone();

            Assert.assertEquals(dag, copied);

            for (DirectedAcyclicGraph.Vertex vertex : dag.getVertexes()) {
                DirectedAcyclicGraph.Vertex vertexCopied = copied.findVertex(vertex.object);

                // equal but not same
                Assert.assertEquals(vertex, vertexCopied);
                Assert.assertNotSame(vertex, vertexCopied);

                Assert.assertEquals(vertex.inDegree, vertexCopied.inDegree);
                Assert.assertEquals(new HashSet<>(vertex.outgoingEdges), new HashSet<>(vertexCopied.outgoingEdges));
            }

            for (DirectedAcyclicGraph.Edge edge : dag.getEdges()) {
                DirectedAcyclicGraph.Edge edgeCopied = copied.findEdge(edge.source.object, edge.target.object);

                // equal but not same
                Assert.assertEquals(edge, edgeCopied);
                Assert.assertNotSame(edge, edgeCopied);
            }
        }
    }

    @Test
    public void testPredecessors() {
        for (int i = 0; i < TEST_CASES.length; i++) {
            printTestHead(i);
            if (!HAS_CYCLES[i]) {
                // Check predecessors of a selected vertex.
                List<DdlTask> predecessors = DAGS[i].getPredecessors(newTask(TEST_CASES[i].selectedVertex));
                System.out.print(TEST_CASES[i].selectedVertex + "'s Predecessors: ");
                StringBuilder predecessorsBuf = new StringBuilder();
                for (DdlTask predecessor : predecessors) {
                    predecessorsBuf.append(predecessor).append(", ");
                }
                String actualPredecessors = predecessorsBuf.substring(0, predecessorsBuf.length() - 2);
                System.out.println(actualPredecessors);
                Assert.assertTrue(actualPredecessors.equals(TEST_CASES[i].expectedPredecessors));
            } else {
                System.out.println("Found cycles, ignored.");
            }
            printTestTail(i);
        }
    }

    @Test
    public void testSuccessors() {
        for (int i = 0; i < TEST_CASES.length; i++) {
            printTestHead(i);
            if (!HAS_CYCLES[i]) {
                // Check successors of a selected vertex.
                List<DdlTask> successors = DAGS[i].getDirectSuccessors(newTask(TEST_CASES[i].selectedVertex));
                System.out.print(TEST_CASES[i].selectedVertex + "'s Successors: ");
                StringBuilder successorsBuf = new StringBuilder();
                for (DdlTask successor : successors) {
                    successorsBuf.append(successor).append(", ");
                }
                String actualSuccessors = successorsBuf.substring(0, successorsBuf.length() - 2);
                System.out.println(actualSuccessors);
                Assert.assertTrue(actualSuccessors.equals(TEST_CASES[i].expectedSuccessors));
            } else {
                System.out.println("Found cycles, ignored.");
            }
            printTestTail(i);
        }
    }

    private static void printTestHead(int i) {
        System.out.println("Test Case " + i + ": ");
        System.out.println("---------------------");
    }

    private static void printTestTail(int i) {
        System.out.println();
    }

    private static class TestCase {

        final String[] edges;
        final boolean hasCycle;
        final String selectedVertex;
        final String expectedPredecessors;
        final String expectedSuccessors;

        public TestCase(String[] edges, boolean hasCycle, String selectedVertex, String expectedPredecessors,
                        String expectedSuccessors) {
            this.edges = edges;
            this.hasCycle = hasCycle;
            this.selectedVertex = selectedVertex;
            this.expectedPredecessors = expectedPredecessors;
            this.expectedSuccessors = expectedSuccessors;
        }

    }

    public static class TestEmptyTask extends BaseGmsTask {

        static IdGenerator idGenerator = IdGenerator.getIdGenerator();

        private TestEmptyTask(String schemaName) {
            super(schemaName, "");
            this.taskId = idGenerator.nextId();
        }

        @Override
        protected void executeImpl(Connection connection, ExecutionContext executionContext) {
        }

        @Override
        protected void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {

        }

        @Override
        public String toString() {
            return schemaName;
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof TestEmptyTask)) {
                return false;
            }
            return schemaName.equals(((TestEmptyTask) obj).schemaName);
        }

        @Override
        public int hashCode() {
            return schemaName.hashCode();
        }
    }
}

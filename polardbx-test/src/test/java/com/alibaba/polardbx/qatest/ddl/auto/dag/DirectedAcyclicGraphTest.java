/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.qatest.ddl.auto.dag;

import com.alibaba.polardbx.executor.ddl.newengine.dag.DirectedAcyclicGraph;
import com.alibaba.polardbx.executor.ddl.newengine.dag.TopologicalSorter;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;

@Ignore
public class DirectedAcyclicGraphTest {

    private static final TestCase[] TEST_CASES = new TestCase[] {
        // No cycle
        new TestCase(
            new String[] {"1->2", "2->3", "3->4", "4->5"},
            false,
            "1, 2, 3, 4, 5",
            "[1], [2], [3], [4], [5]",
            "3",
            "2",
            "4"
        ),
        // No cycle
        new TestCase(
            new String[] {"1->2", "2->3", "2->4", "3->5", "4->6", "5->7", "6->7", "7->8"},
            false,
            "1, 2, 3, 4, 5, 6, 7, 8",
            "[1], [2], [3, 4], [5, 6], [7], [8]",
            "7",
            "5, 6",
            "8"
        ),
        // No cycle
        new TestCase(
            new String[] {"1->3", "2->3", "3->4", "4->7", "5->7", "6->7"},
            false,
            "1, 2, 5, 6, 3, 4, 7",
            "[1, 2, 5, 6], [3], [4], [7]",
            "3",
            "1, 2",
            "4"
        ),
        // Has one cycle
        new TestCase(
            new String[] {"1->2", "2->3", "3->4", "4->5", "5->1"},
            true,
            "",
            "",
            "",
            "",
            ""
        ),
        // Has one cycle
        new TestCase(
            new String[] {"1->2", "2->3", "3->4", "4->5", "3->1"},
            true,
            "",
            "",
            "",
            "",
            ""
        ),
        // Has two cycles
        new TestCase(
            new String[] {"1->2", "2->3", "3->1", "3->4", "4->5", "5->3", "5->6", "6->7"},
            true,
            "",
            "",
            "",
            "",
            ""
        ),
        // No cycle
        new TestCase(
            new String[] {"1->2", "2->3", "3->4", "5->6", "6->7", "7->8"},
            false,
            "1, 5, 2, 6, 3, 7, 4, 8",
            "[1, 5], [2, 6], [3, 7], [4, 8]",
            "6",
            "5",
            "7"
        ),
        new TestCase(
            new String[] {
                "1->11", "1->12",
                "11->111", "11->112", "11->113",
                "12->121", "12->122", "12->123",
                "111->1111", "1111->1112", "1112->1113", "1113->114",
                "112->1121", "1121->1122", "1122->1123", "1123->115",
                "113->1131", "1131->1132", "1132->1133", "1133->116",
                "121->1211", "1211->1212", "1212->1213", "1213->124",
                "122->1221", "1221->1222", "1222->1223", "1223->125",
                "123->1231", "1231->1232", "1232->1233", "1233->126",
                "114->13", "115->13", "116->13",
                "124->14", "125->14", "126->14",
                "13->2", "14->2"
            },
            false,
            "1, 11, 12, " +
                "111, 112, 113, 121, 122, 123, " +
                "1111, 1121, 1131, 1211, 1221, 1231, " +
                "1112, 1122, 1132, 1212, 1222, 1232, " +
                "1113, 1123, 1133, 1213, 1223, 1233, " +
                "114, 115, 116, 124, 125, 126, " +
                "13, 14, 2",
            "[1], [11, 12], " +
                "[111, 112, 113, 121, 122, 123], " +
                "[1111, 1121, 1131, 1211, 1221, 1231], " +
                "[1112, 1122, 1132, 1212, 1222, 1232], " +
                "[1113, 1123, 1133, 1213, 1223, 1233], " +
                "[114, 115, 116, 124, 125, 126], " +
                "[13, 14], [2]",
            "14",
            "124, 125, 126",
            "2"
        )
    };

    private static final DirectedAcyclicGraph[] DAGS = new DirectedAcyclicGraph[TEST_CASES.length];
    private static final Boolean[] HAS_CYCLES = new Boolean[TEST_CASES.length];

    private static TestEmptyTask newTask(String str) {
        return new TestEmptyTask(str);
    }

    private static void assertSort(String str, List<List<DdlTask>> taskList) {

    }

    @BeforeClass
    public static void init() {
        // Build all DAGs.
        for (int i = 0; i < TEST_CASES.length; i++) {
            DirectedAcyclicGraph daGraph = DirectedAcyclicGraph.create();
            for (String edge : TEST_CASES[i].edges) {
                String[] vertexes = edge.split("->");
                daGraph.addEdge(newTask(vertexes[0]), newTask(vertexes[1]));
            }
            DAGS[i] = daGraph;
            HAS_CYCLES[i] = TopologicalSorter.hasCycle(daGraph.clone());
        }
    }

    @Test
    public void testCycle() {
        for (int i = 0; i < TEST_CASES.length; i++) {
            printTestHead(i);
            // Check if there is a cycle.
            System.out.println(HAS_CYCLES[i] ? "Found cycles" : "OK - no cycle");
            Assert.assertTrue(TEST_CASES[i].hasCycles == HAS_CYCLES[i]);
            printTestTail(i);
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

    @Test
    public void testTraversalOneByOne() {
        for (int i = 0; i < TEST_CASES.length; i++) {
            printTestHead(i);
            if (!HAS_CYCLES[i]) {
                // Output topological sorting result.
                System.out.print("One by One: ");
                StringBuilder oneByOneBuf = new StringBuilder();
                TopologicalSorter sorterOne = TopologicalSorter.create(DAGS[i].clone());
                sorterOne.init();
                while (sorterOne.hasNext()) {
                    DdlTask vertex = sorterOne.next();
                    oneByOneBuf.append(vertex).append(", ");
                }
                String actualOneByOne = oneByOneBuf.substring(0, oneByOneBuf.length() - 2);
                System.out.println(actualOneByOne);
                Assert.assertEquals(TEST_CASES[i].expectedOneByOne, actualOneByOne);
            } else {
                System.out.println("Found cycles, ignored.");
            }
            printTestTail(i);
        }
    }

    @Test
    public void testTraversalBatch() {
        for (int i = 0; i < TEST_CASES.length; i++) {
            printTestHead(i);
            if (!HAS_CYCLES[i]) {
                // Output batch topological sorting result.
                System.out.print("Batch: ");
                StringBuilder batchBuf = new StringBuilder();
                TopologicalSorter sorterBatch = TopologicalSorter.create(DAGS[i].clone());
                sorterBatch.init();
                while (sorterBatch.hasNext()) {
                    List<DdlTask> vertexes = sorterBatch.nextBatch();
                    batchBuf.append("[");
                    for (DdlTask vertex : vertexes) {
                        batchBuf.append(vertex).append(", ");
                    }
                    int len = batchBuf.length();
                    batchBuf.delete(len - 2, len).append("], ");
                }
                String actualBatch = batchBuf.substring(0, batchBuf.length() - 2);
                System.out.println(actualBatch);
                Assert.assertEquals(TEST_CASES[i].expectedBatch, actualBatch);
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
        final boolean hasCycles;
        final String expectedOneByOne;
        final String expectedBatch;
        final String selectedVertex;
        final String expectedPredecessors;
        final String expectedSuccessors;

        public TestCase(String[] edges, boolean hasCycles, String expectedOneByOne, String expectedBatch,
                        String selectedVertex, String expectedPredecessors, String expectedSuccessors) {
            this.edges = edges;
            this.hasCycles = hasCycles;
            this.expectedOneByOne = expectedOneByOne;
            this.expectedBatch = expectedBatch;
            this.selectedVertex = selectedVertex;
            this.expectedPredecessors = expectedPredecessors;
            this.expectedSuccessors = expectedSuccessors;
        }

    }

}

package com.alibaba.polardbx.executor.ddl.newengine.dag;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static com.alibaba.polardbx.executor.ddl.newengine.dag.DirectedAcyclicGraphTest.newTask;

public class DirectedAcyclicGraphHasCycleTest {

    private DirectedAcyclicGraph dag;
    private DirectedAcyclicGraph.Vertex v1, v2, v3, v4, v5, v6, v7, v8, v9, v10;

    @Before
    public void init() {
        dag = DirectedAcyclicGraph.create();
        v1 = DirectedAcyclicGraph.Vertex.create(newTask("A"));
        v2 = DirectedAcyclicGraph.Vertex.create(newTask("B"));
        v3 = DirectedAcyclicGraph.Vertex.create(newTask("C"));
        v4 = DirectedAcyclicGraph.Vertex.create(newTask("D"));
        v5 = DirectedAcyclicGraph.Vertex.create(newTask("E"));
        v6 = DirectedAcyclicGraph.Vertex.create(newTask("F"));
        v7 = DirectedAcyclicGraph.Vertex.create(newTask("G"));
        v8 = DirectedAcyclicGraph.Vertex.create(newTask("H"));
        v9 = DirectedAcyclicGraph.Vertex.create(newTask("I"));
        v10 = DirectedAcyclicGraph.Vertex.create(newTask("J"));

    }

    @Test
    public void testEmptyGraph() {
        Assert.assertFalse(dag.hasCycle());
    }

    @Test
    public void testSingleVertex() {
        dag.addVertex(v1);
        Assert.assertFalse(dag.hasCycle());
    }

    @Test
    public void testTwoVertices() {
        dag.addEdge(v1, v2);
        Assert.assertFalse(dag.hasCycle());
        dag.addEdge(v2, v1);
        Assert.assertTrue(dag.hasCycle());
    }

    @Test
    public void testManyVertices() {
        DirectedAcyclicGraph dag = DirectedAcyclicGraph.create();
        DirectedAcyclicGraph.Vertex vertexs[] = new DirectedAcyclicGraph.Vertex[100000];

        for (int i = 0; i < vertexs.length; i++) {
            vertexs[i] = DirectedAcyclicGraph.Vertex.create(newTask(String.valueOf(i)));
        }

        for (int i = 0; i < vertexs.length / 2; i++) {
            dag.addEdge(vertexs[i],
                vertexs[60000]);
            dag.addEdge(vertexs[i],
                vertexs[70000]);
            dag.addEdge(vertexs[i],
                vertexs[80000]);
            dag.addEdge(vertexs[i],
                vertexs[90000]);
        }

        for (int i = vertexs.length - 1; i > 0; i--) {
            dag.addEdge(vertexs[i - 1],
                vertexs[i]);
        }

        for (int i = 0; i < vertexs.length - 2; i++) {
            dag.addEdge(vertexs[i],
                vertexs[i + 2]);
        }

        for (int i = 1; i < vertexs.length / 5; i++) {
            dag.addEdge(vertexs[i],
                vertexs[i * 5]);
        }

        Assert.assertFalse(dag.hasCycle());
        dag.addEdge(vertexs[vertexs.length - 1], vertexs[0]);
        Assert.assertTrue(dag.hasCycle());

    }

    @Test
    public void testThreeVertices() {
        dag.addEdge(v1, v2);
        dag.addEdge(v2, v3);
        Assert.assertFalse(dag.hasCycle());
        dag.addEdge(v3, v1);
        Assert.assertTrue(dag.hasCycle());
    }

    @Test
    public void testThreeVertices2() {
        dag.addEdge(v1, v2);
        dag.addEdge(v2, v3);
        Assert.assertFalse(dag.hasCycle());
        dag.addEdge(v3, v2);
        Assert.assertTrue(dag.hasCycle());
    }

    @Test
    public void testFourVertices() {
        dag.addEdge(v1, v2);
        dag.addEdge(v2, v3);
        dag.addEdge(v3, v4);
        Assert.assertFalse(dag.hasCycle());
        dag.addEdge(v4, v1);
        Assert.assertTrue(dag.hasCycle());
    }

    @Test
    public void testFourVertices2() {
        dag.addEdge(v1, v2);
        dag.addEdge(v1, v3);
        dag.addEdge(v2, v4);
        dag.addEdge(v3, v4);
        Assert.assertFalse(dag.hasCycle());
    }

    @Test
    public void testFiveVertices() {
        dag.addEdge(v1, v2);
        dag.addEdge(v2, v3);
        dag.addEdge(v2, v4);
        dag.addEdge(v4, v5);
        Assert.assertFalse(dag.hasCycle());
        dag.addEdge(v5, v3);
        Assert.assertFalse(dag.hasCycle());
        dag.addEdge(v5, v2);
        Assert.assertTrue(dag.hasCycle());
    }

    @Test
    public void testSixVertices() {
        dag.addEdge(v1, v2);
        dag.addEdge(v2, v3);
        dag.addEdge(v2, v4);
        dag.addEdge(v4, v5);
        dag.addEdge(v5, v6);
        Assert.assertFalse(dag.hasCycle());
        dag.addEdge(v6, v3);
        Assert.assertFalse(dag.hasCycle());
        dag.addEdge(v6, v2);
        Assert.assertTrue(dag.hasCycle());
    }

    @Test
    public void testSevenVertices() {
        dag.addEdge(v1, v2);
        dag.addEdge(v2, v3);
        dag.addEdge(v2, v4);
        dag.addEdge(v4, v5);
        dag.addEdge(v4, v6);
        dag.addEdge(v4, v7);
        dag.addEdge(v5, v6);
        dag.addEdge(v6, v7);
        Assert.assertFalse(dag.hasCycle());
        dag.addEdge(v7, v4);
        Assert.assertTrue(dag.hasCycle());
    }

    @Test
    public void testEightVertices() {
        dag.addEdge(v1, v2);
        dag.addEdge(v2, v3);
        dag.addEdge(v2, v4);
        dag.addEdge(v4, v5);
        dag.addEdge(v5, v6);
        dag.addEdge(v6, v7);
        dag.addEdge(v7, v8);
        Assert.assertFalse(dag.hasCycle());
        dag.addEdge(v8, v4);
        Assert.assertTrue(dag.hasCycle());
    }

    @Test
    public void testNineVertices() {
        dag.addEdge(v1, v2);
        dag.addEdge(v2, v3);
        dag.addEdge(v2, v4);
        dag.addEdge(v4, v5);
        dag.addEdge(v5, v6);
        dag.addEdge(v6, v7);
        dag.addEdge(v7, v8);
        dag.addEdge(v8, v9);
        Assert.assertFalse(dag.hasCycle());
        dag.addEdge(v9, v4);
        Assert.assertTrue(dag.hasCycle());
    }

    @Test
    public void testTenVertices() {
        dag.addEdge(v1, v2);
        dag.addEdge(v2, v3);
        dag.addEdge(v2, v4);
        dag.addEdge(v4, v5);
        dag.addEdge(v5, v6);
        dag.addEdge(v6, v7);
        dag.addEdge(v7, v8);
        dag.addEdge(v8, v9);
        dag.addEdge(v9, v10);
        Assert.assertFalse(dag.hasCycle());
        dag.addEdge(v10, v4);
        Assert.assertTrue(dag.hasCycle());
    }
}

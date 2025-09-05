package com.alibaba.polardbx.executor.function.calc;

import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;

public class CartesianTest {
    private Cartesian cartesian;
    private ExecutionContext executionContext;

    @Before
    public void setUp() {
        cartesian = new Cartesian(Arrays.asList(null,null),null);
        executionContext = new ExecutionContext();
        // You need to configure the execution context here, it's omitted because
        // the details are not provided. You would typically set chunk size and other parameters.
    }

    @After
    public void tearDown() {
        cartesian = null;
        executionContext = null;
    }

    @Test
    public void testCartesianProductWithNormalLists() {
        List<Object> list1 = Arrays.asList(1, 2);
        List<Object> list2 = Arrays.asList("a", "b");
        List<Object> list3 = Arrays.asList(true, false);

        // Set up your arguments
        Object[] args = new Object[] {list1, list2, list3};

        // Execute the function
        Chunk resultChunk = (Chunk) cartesian.compute(args, executionContext);

        // Test the received Chunk content
        List<Object[]> expectedResults = Arrays.asList(
            new Object[] {1, "a", true},
            new Object[] {1, "a", false},
            new Object[] {1, "b", true},
            new Object[] {1, "b", false},
            new Object[] {2, "a", true},
            new Object[] {2, "a", false},
            new Object[] {2, "b", true},
            new Object[] {2, "b", false}
        );

        Object[][] actualResults = extractResultsFromChunk(resultChunk);
        assertArrayEquals(expectedResults.toArray(), actualResults);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCartesianProductWithNonListArguments() {
        Object[] args = new Object[] {Arrays.asList(1, 2), "InvalidArgument"};
        cartesian.compute(args, executionContext);
    }

    // Helper method to extract results from Chunk
    private Object[][] extractResultsFromChunk(Chunk chunk) {

        Object[][] results = new Object[chunk.getPositionCount()][chunk.getBlockCount()];
        for (int i = 0; i < chunk.getPositionCount(); i++) {
            for (int j = 0; j < chunk.getBlockCount(); j++) {
                results[i][j] = chunk.rowAt(i).getJavaValues().get(j);
            }
        }
        return results; // Placeholder, replace with actual extraction code
    }
}

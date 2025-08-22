package com.alibaba.polardbx.executor.function.calc;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static com.alibaba.polardbx.common.utils.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

public class ListTest {

    private List listFunction;
    private ExecutionContext ec; // Mock or create a dummy instance as per requirement

    @Before
    public void setUp() {
        listFunction = new List(Arrays.asList(null,null),null);
        ec = new ExecutionContext(); // Initialize as needed or use a mock
    }

    @Test
    public void testComputeWithSingleListArgument() {
        Object[] args = {Arrays.asList(1, 2, 3)};
        Object result = listFunction.compute(args, ec);

        assertTrue(result instanceof java.util.List);
        assertEquals(Arrays.asList(1, 2, 3), result);
    }

    @Test
    public void testComputeWithMultipleArguments() {
        Object[] args = {1, Arrays.asList(2, 3), 4};
        Object result = listFunction.compute(args, ec);

        assertTrue(result instanceof java.util.List);
        assertEquals(Arrays.asList(1, 2, 3, 4), result);
    }

    @Test
    public void testComputeWithEmptyArgument() {
        Object[] args = {};
        Object result = listFunction.compute(args, ec);

        assertTrue(result instanceof java.util.List);
        assertEquals(Collections.emptyList(), result);
    }

    @Test
    public void testComputeWithNestedLists() {
        Object[] args = {Arrays.asList(Arrays.asList(1, 2), 3), 4};
        Object result = listFunction.evaluateResult(args, ec);

        assertTrue(result instanceof java.util.List);
        assertEquals(Arrays.asList(Arrays.asList(1, 2), 3, 4), result);
    }

    @Test
    public void testComputeWithNullArguments() {
        Object[] args = {null, Arrays.asList(1, 2)};
        Object result = listFunction.evaluateResult(args, ec);

        assertTrue(result instanceof java.util.List);
        assertEquals(Arrays.asList(null, 1, 2), result);
    }
}

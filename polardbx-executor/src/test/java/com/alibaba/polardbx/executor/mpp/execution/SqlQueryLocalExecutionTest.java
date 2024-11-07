package com.alibaba.polardbx.executor.mpp.execution;

import com.alibaba.polardbx.executor.mpp.metadata.Split;
import org.junit.Test;
import org.junit.Before;
import org.mockito.Mock;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertTrue;

import org.mockito.MockitoAnnotations;

public class SqlQueryLocalExecutionTest {

    @Mock
    SqlQueryLocalExecution sqlQueryLocalExecution;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testGetRandomStartWithNonEmptyList() {
        List<Split> splits = new ArrayList<>();
        splits.add(Split.EMPTY_SPLIT);
        splits.add(new Split(false, null));

        int randomStart = sqlQueryLocalExecution.getRandomStart(splits);
        assertTrue("Expected randomStart to be within the bounds of the list",
            randomStart >= 0 && randomStart < splits.size());
    }

    @Test
    public void testGetRandomStartWithEmptyList() {
        List<Split> splits = Collections.emptyList();

        int randomStart = sqlQueryLocalExecution.getRandomStart(splits);
        assertTrue("Expected randomStart to be 0 for an empty list", randomStart == 0);
    }
}

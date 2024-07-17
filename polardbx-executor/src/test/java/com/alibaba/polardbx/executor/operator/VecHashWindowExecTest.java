package com.alibaba.polardbx.executor.operator;

import org.junit.Before;

public class VecHashWindowExecTest extends HashWindowExecTest {
    @Before
    public void before() {
        prepareParams(true);
    }
}

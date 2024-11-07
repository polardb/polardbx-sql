package com.alibaba.polardbx.optimizer;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * @author fangwu
 */
public class PlannerContextTest {

    /**
     * Test Case 1: Encoding extended parameters to JSON under normal conditions.
     * Design Idea:
     * - Create a PlannerContext instance and set its internal state (such as isUseColumnar and getColumnarMaxShardCnt)
     * - Invoke the encodeExtendedParametersToJson method
     * - Verify that the toJsonString method of the mocked JsonBuilder object was correctly invoked
     * - Ensure the returned value is the expected JSON string
     */
    @Test
    public void testEncodeExtendedParametersToJsonNormalCase() {
        // Preparation
        PlannerContext plannerContext = new PlannerContext();
        plannerContext.setUseColumnar(true);
        plannerContext.setColumnarMaxShardCnt(10);

        // Execution
        String result = plannerContext.encodeExtendedParametersToJson();

        // Verification
        assertTrue(result.contains("columnarMaxShardCnt") && result.contains("useColumnar"));

        PlannerContext plannerContext1 = new PlannerContext();
        plannerContext1.decodeArguments(result);

        assertTrue(plannerContext1.isUseColumnar() && plannerContext1.getColumnarMaxShardCnt() == 10);
    }
}

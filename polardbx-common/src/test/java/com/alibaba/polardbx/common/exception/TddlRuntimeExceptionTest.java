package com.alibaba.polardbx.common.exception;

import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.Assert;
import org.junit.Test;

/**
 * @author fangwu
 */
public class TddlRuntimeExceptionTest {
    @Test
    public void test() throws InterruptedException {
        TddlRuntimeException nestRuntimeException = new TddlRuntimeException(ErrorCode.ERR_DUPLICATE_KEY, "test msg5");
        TddlRuntimeException tddlRuntimeException4 =
            new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_FAILED, nestRuntimeException, "test msg6");
        System.out.println(tddlRuntimeException4.getMessage());
        tddlRuntimeException4.printStackTrace();
        Assert.assertTrue(tddlRuntimeException4.getMessage().contains("ERR_DDL_JOB_FAILED"));
    }
}

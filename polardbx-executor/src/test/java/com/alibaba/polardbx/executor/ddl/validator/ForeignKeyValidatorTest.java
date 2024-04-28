package com.alibaba.polardbx.executor.ddl.validator;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.executor.ddl.job.validator.ForeignKeyValidator;
import org.junit.Test;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
public class ForeignKeyValidatorTest {
    private String srcSchemaName = "test1";
    private String dstSchemaName = "test2";
    private String srcTableName = "t1";
    private String dstTableName = "t2";

    @Test(expected = TddlRuntimeException.class)
    public void testValidateCharset() {
        ForeignKeyValidator.validateCharset(srcSchemaName, srcTableName, dstSchemaName, dstTableName, "utf8", "gbk");
    }

    @Test
    public void testValidateCharsetOk() {
        ForeignKeyValidator.validateCharset(srcSchemaName, srcTableName, dstSchemaName, dstTableName, "utf8mb3",
            "utf8");
    }

    @Test(expected = TddlRuntimeException.class)
    public void testValidateCollate() {
        ForeignKeyValidator.validateCollate(srcSchemaName, srcTableName, dstSchemaName, dstTableName, "utf8_general_ci",
            "gbk_chinese_ci");
    }

    @Test
    public void testValidateCollateOk() {
        ForeignKeyValidator.validateCollate(srcSchemaName, srcTableName, dstSchemaName, dstTableName, "utf8_general_ci",
            "utf8mb3_general_ci");
    }

}

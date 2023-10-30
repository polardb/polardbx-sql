package com.alibaba.polardbx.qatest.ddl.auto.partition;

import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * @author chenghui.lch
 */

public class PartitionColumnTypeForKey3Test extends PartitionColumnTypePrepStmtTestBase {

    public TestParameter parameter;

    public PartitionColumnTypeForKey3Test(TestParameter parameter) {
        super(parameter);
        this.testDbName = this.testDbName + "_k3";
        this.testQueryByPrepStmt = true;
        this.testInsertByPrepStmt = true;
        this.testDbName = this.testDbName + "_k3";
    }

    @Parameterized.Parameters(name = "{index}: partColTypeTestCase {0}")
    public static List<TestParameter> parameters() {
        // partition strategy: range/list/hash/range column/list column/key
        // data type: numeric/string/time
        return Arrays.asList(

            /**
             * ========= Key, varbinary ===========
             */
            /* varbinary */
            new PartitionColumnTypePrepStmtTestBase.TestParameter(
                "key_varbinary",
                new String[] {"c1"}/*col*/,
                new String[] {"varbinary(1024) default null "}/*data types*/,
                new String[] {"set sql_mode='';set names utf8;", "", ""}/*prepStmts*/,
                "key"/*strategy*/,
                new String[] {"16"}/*bndVal*/,
                new String[] {
                    "('a')", "('b')", "(\'\')", "('世界')", "('☺')", "('12345678')"
                    , "(null)"
                }/*insertValues*/
                ,
                new String[] {
                    "('A')", "('B')", "('a')", "('b')", "('a  ')", "('b   ')", "(\'\')",
                    "('世界')",
                    "('☺')", "('12345678')"
                    , "(null)"
                }/*selectValues*/
                ,
                new String[] {
                    "('A')", "('B')", "('a')", "('b')", "('a  ')", "('b   ')", "(\'\')",
                    "('世界')",
                    "('☺')", "('12345678')"
                    , "(null)"
                }/*rngSortValues*/
            )
        );

    }

    @Test
    public void runTest() throws SQLException {
        super.testInsertAndSelect();
    }

}

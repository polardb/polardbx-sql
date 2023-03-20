package com.alibaba.polardbx.qatest.ddl.auto.partition;

import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * @author chenghui.lch
 */

public class PartitionColumnTypeForKey2Test extends PartitionColumnTypeTestBase {

    public TestParameter parameter;

    public PartitionColumnTypeForKey2Test(TestParameter parameter) {
        super(parameter);
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
            new TestParameter(
                "key_varbin",
                new String[] {"c1"}/*col*/,
                new String[] {"varbinary(1024) default null "}/*data types*/,
                new String[] {"set sql_mode='';set names utf8;", "", ""}/*prepStmts*/,
                "key"/*strategy*/,
                new String[] {"16"}/*bndVal*/,
                new String[] {
                    "('a')", "('b')", "(\'\')", "('世界')", "('☺')", "('12345678')",
                    "(null)"
                }/*insertValues*/
                ,
                new String[] {
                    "('A')", "('B')", "('a')", "('b')", "('a  ')", "('b   ')", "(\'\')", "('世界')",
                    "('☺')", "('12345678')", "(null)"}/*selectValues*/
                ,
                new String[] {
                    "('A')", "('B')", "('a')", "('b')", "('a  ')", "('b   ')", "(\'\')", "('世界')",
                    "('☺')", "('12345678')", "(null)"
                }/*rngSortValues*/
            )
            ,
            /* varbinary */
            new TestParameter(
                "key_varbin2",
                new String[] {"c1"}/*col*/,
                new String[] {"varbinary(1024) default null "}/*data types*/,
                new String[] {"set sql_mode='';set names utf8;", "", ""}/*prepStmts*/,
                "key"/*strategy*/,
                new String[] {"16"}/*bndVal*/,
                new String[] {
                    genStringHexBinaryByCharset("", "utf8mb4", false, false),
                    genStringHexBinaryByCharset("a", "utf8mb4", false, false),
                    genStringHexBinaryByCharset("b", "utf8mb4", false, false),
                    genStringHexBinaryByCharset("世界", "utf8mb4", false, false),
                    genStringHexBinaryByCharset("☺", "utf8mb4", false, false),
                    genStringHexBinaryByCharset("12345678", "utf8mb4", false, false),
                    //genStringHexBinaryByCharset("null", "utf8mb4", false, false)
                }/*insertValues*/
                ,
                new String[] {
                    genStringHexBinaryByCharset("", "utf8mb4", false, false),
                    genStringHexBinaryByCharset("B", "utf8mb4", false, false),
                    genStringHexBinaryByCharset("a", "utf8mb4", false, false),
                    genStringHexBinaryByCharset("b", "utf8mb4", false, false),
                    genStringHexBinaryByCharset("a  ", "utf8mb4", false, false),
                    genStringHexBinaryByCharset("b   ", "utf8mb4", false, false),
                    genStringHexBinaryByCharset("世界", "utf8mb4", false, false),
                    genStringHexBinaryByCharset("☺", "utf8mb4", false, false),
                    genStringHexBinaryByCharset("12345678", "utf8mb4", false, false),
                    //genStringHexBinaryByCharset("null", "utf8mb4", false, false)
                }/*selectValues*/
                ,
                new String[] {
                    genStringHexBinaryByCharset("", "utf8mb4", false, false),
                    genStringHexBinaryByCharset("B", "utf8mb4", false, false),
                    genStringHexBinaryByCharset("a", "utf8mb4", false, false),
                    genStringHexBinaryByCharset("b", "utf8mb4", false, false),
                    genStringHexBinaryByCharset("a  ", "utf8mb4", false, false),
                    genStringHexBinaryByCharset("b   ", "utf8mb4", false, false),
                    genStringHexBinaryByCharset("世界", "utf8mb4", false, false),
                    genStringHexBinaryByCharset("☺", "utf8mb4", false, false),
                    genStringHexBinaryByCharset("12345678", "utf8mb4", false, false),
                    //genStringHexBinaryByCharset("null", "utf8mb4", false, false)
                }/*rngValues*/
            )
            ,
            /* varbinary */
            new TestParameter(
                "key_varbin3",
                new String[] {"c1"}/*col*/,
                new String[] {"varbinary(1024) default null "}/*data types*/,
                new String[] {"set sql_mode='';set names utf8;", "", ""}/*prepStmts*/,
                "key"/*strategy*/,
                new String[] {"16"}/*bndVal*/,
                new String[] {
                    //genStringHexBinaryByCharset("", "utf8mb4", false, false, true),
                    genStringHexBinaryByCharset("a", "utf8mb4", false, false, true),
                    genStringHexBinaryByCharset("b", "utf8mb4", false, false, true),
                    genStringHexBinaryByCharset("世界", "utf8mb4", false, false, true),
                    genStringHexBinaryByCharset("☺", "utf8mb4", false, false, true),
                    genStringHexBinaryByCharset("12345678", "utf8mb4", false, false, true),
                    //genStringHexBinaryByCharset("null", "utf8mb4", false, false)
                }/*insertValues*/
                ,
                new String[] {
                    //genStringHexBinaryByCharset("", "utf8mb4", false, false, true),
                    genStringHexBinaryByCharset("B", "utf8mb4", false, false, true),
                    genStringHexBinaryByCharset("a", "utf8mb4", false, false, true),
                    genStringHexBinaryByCharset("b", "utf8mb4", false, false, true),
                    genStringHexBinaryByCharset("a  ", "utf8mb4", false, false, true),
                    genStringHexBinaryByCharset("b   ", "utf8mb4", false, false, true),
                    genStringHexBinaryByCharset("世界", "utf8mb4", false, false, true),
                    genStringHexBinaryByCharset("☺", "utf8mb4", false, false, true),
                    genStringHexBinaryByCharset("12345678", "utf8mb4", false, false, true),
                    //genStringHexBinaryByCharset("null", "utf8mb4", false, false)
                }/*selectValues*/
                ,
                new String[] {
                    //genStringHexBinaryByCharset("", "utf8mb4", false, false, true),
                    genStringHexBinaryByCharset("B", "utf8mb4", false, false, true),
                    genStringHexBinaryByCharset("a", "utf8mb4", false, false, true),
                    genStringHexBinaryByCharset("b", "utf8mb4", false, false, true),
                    genStringHexBinaryByCharset("a  ", "utf8mb4", false, false, true),
                    genStringHexBinaryByCharset("b   ", "utf8mb4", false, false, true),
                    genStringHexBinaryByCharset("世界", "utf8mb4", false, false, true),
                    genStringHexBinaryByCharset("☺", "utf8mb4", false, false, true),
                    genStringHexBinaryByCharset("12345678", "utf8mb4", false, false, true),
                    //genStringHexBinaryByCharset("null", "utf8mb4", false, false)
                }/*rngValues*/
            )
            ,

            /**
             * ========= Key, varbinary ===========
             */
            /* binary */
            /* varbinary */
            new TestParameter(
                "key_bin128",
                new String[] {"c1"}/*col*/,
                new String[] {"binary(16) default null "}/*data types*/,
                new String[] {"set sql_mode='';set names utf8;", "", ""}/*prepStmts*/,
                "key"/*strategy*/,
                new String[] {"16"}/*bndVal*/,
                new String[] {
                    "('a')", "('b')", "(\'\')", "('世界')", "('☺')", "('12345678')",
                    "(null)"
                }/*insertValues*/
                ,
                new String[] {
                    "('A')", "('B')", "('a')", "('b')", "('a  ')", "('b   ')", "(\'\')", "('世界')",
                    "('☺')", "('12345678')", "(null)"}/*selectValues*/
                ,
                new String[] {
                    "('A')", "('B')", "('a')", "('b')", "('a  ')", "('b   ')", "(\'\')", "('世界')",
                    "('☺')", "('12345678')", "(null)"
                }/*rngSortValues*/
            )
        );

    }

    @Test
    public void runTest() throws SQLException {
        super.testInsertAndSelect();
    }

}

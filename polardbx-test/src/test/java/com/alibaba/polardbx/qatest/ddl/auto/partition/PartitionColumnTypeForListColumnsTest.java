/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.qatest.ddl.auto.partition;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * @author chenghui.lch
 */

public class PartitionColumnTypeForListColumnsTest extends PartitionColumnTypeTestBase {

    public TestParameter parameter;

    public PartitionColumnTypeForListColumnsTest(TestParameter parameter) {
        super(parameter);
        this.testDbName = this.testDbName + "_l1";
        //this.testQueryByPrepStmt = true;
        this.testDbName = this.testDbName + "_l1";
    }

    @Before
    public void setUp() {

    }

    @After
    public void tearDown() {
    }

    @Parameterized.Parameters(name = "{index}: partColTypeTestCase {0}")
    public static List<TestParameter> parameters() {
        // partition strategy: range/list/hash/range column/list column/key
        // data type: numeric/string/time
        return Arrays.asList(

            /**
             * ========= List Columns =========== 
             */
            /**
             * List Columns
             */
            /* varchar */
            new PartitionColumnTypeTestBase.TestParameter(
                "lc_varchar8",
                new String[] {"c1"}/*col*/,
                new String[] {"varchar(8) CHARACTER SET utf8 COLLATE utf8_general_ci default null "}/*data types*/,
                new String[] {"set sql_mode='';set names utf8;", "set names utf8;", "set names utf8;"},
                "list columns"/*strategy*/,
                new String[] {
                    "(null,'')",
                    "('a','b')",
                    "('z')",
                    "('☺')",
                    "('ccc')",
                    "('12345678','1234')",
                    genListStringHexBinaryByCharset(
                        "utf8",
                        new String[] {
                            "世界",
                            "世界人民"
                        }),
                    genListStringHexBinaryByCharset("gbk", new String[] {"世界人民万岁"})
                }/*bndVal*/,
                new String[] {
                    "(null)",
                    "('')",
                    "('a')",
                    "('b')",
                    "('A')",
                    "('B')",
                    "('☺')",
                    "('Z     ')",
                    "('z  ')",
                    "('z')",
                    "('ccc')",
                    "('CcC')",
                    "('12345678')",
                    "('1234567889ABC')",
                    "('1234567889EFG')",
                    "('世界')",
                    genStringHexBinaryByCharset("世界人民", "utf8"),
                    genStringHexBinaryByCharset("世界", "utf8"),
                    genStringHexBinaryByCharset("世界人民万岁", "utf8")
                }/*insertValues*/
                ,
                new String[] {
                    "(null)",
                    "('')",
                    "('a')",
                    "('b')",
                    "('A')",
                    "('B')",
                    "('ccc')",
                    "('CcC')",
                    "('Z     ')",
                    "('z  ')",
                    "('z')",
                    "('☺')",
                    "('12345678')",
                    "('1234567889ABC')",
                    "('1234567889EFG')",
                    "('世界')",
                    "( x'E4B896E7958C' )",
                    genStringHexBinaryByCharset("世界人民", "utf8"),
                    genStringHexBinaryByCharset("世界", "utf8"),
                    genStringHexBinaryByCharset("世界人民万岁", "gbk")}/*selectValues*/
                , new String[] {
                "('12345677')", "('12345679')", "('A')", "('B')", "('a')", "('a  ')", "('b')", "('b   ')", "('世界')",
                "('世界人民')"}/*rngSortValues*/
            )

            /* char */
            , new PartitionColumnTypeTestBase.TestParameter(
                "lc_char8",
                new String[] {"c1"}/*col*/,
                new String[] {"char(8) CHARACTER SET utf8 COLLATE utf8_bin default null "}/*data types*/,
                new String[] {"set sql_mode='';set names utf8;", "set names utf8;", "set names utf8;"},
                "list columns"/*strategy*/,
                new String[] {
                    "(null,'')",
                    "('a','b')",
                    "('A','B')",
                    "('z')",
                    "('Z')",
                    "('☺')",
                    "('aBc')",
                    "('Abc')",
                    "('CcC')",
                    "('ccc')",
                    "('12345678','1234')",
                    genListStringHexBinaryByCharset(
                        "utf8",
                        new String[] {
                            "世界",
                            "世界人民"
                        }),
                    genListStringHexBinaryByCharset("gbk", new String[] {"世界人民万岁"})
                }/*bndVal*/,
                new String[] {
                    "(null)",
                    "('')",
                    "('a')",
                    "('b')",
                    "('A')",
                    "('B')",
                    "('A ')",
                    "('B ')",
                    "('☺')",
                    "('Z')",
                    "('z')",
                    "('ccc')",
                    "('CcC')",
                    "('aBc')",
                    "('Abc')",
                    "('12345678')",
                    "('1234567889ABC')",
                    "('1234567889EFG')",
                    "('世界')",
                    genStringHexBinaryByCharset("世界人民", "utf8"),
                    genStringHexBinaryByCharset("世界", "utf8"),
                    genStringHexBinaryByCharset("世界人民万岁", "utf8")
                }/*insertValues*/
                ,
                new String[] {
                    "(null)",
                    "('')",
                    "('a')",
                    "('b')",
                    "('A')",
                    "('B')",
                    "('A ')",
                    "('B ')",
                    "('ccc')",
                    "('CcC')",
                    "('Z     ')",
                    "('z  ')",
                    "('z')",
                    "('aBc')",
                    "('Abc')",
                    "('☺')",
                    "('12345678')",
                    "('1234567889ABC')",
                    "('1234567889EFG')",
                    "('世界')",
                    "( x'E4B896E7958C' )",
                    genStringHexBinaryByCharset("世界人民", "utf8"),
                    genStringHexBinaryByCharset("世界", "utf8"),
                    genStringHexBinaryByCharset("世界人民万岁", "gbk")}/*selectValues*/
                , new String[] {
                "('12345677')", "('12345679')", "('A')", "('B')", "('a')", "('a  ')", "('b')", "('b   ')", "('世界')",
                "('世界人民')"}/*rngSortValues*/
            )
        );
    }

    @Test
    public void runTest() throws SQLException {
        super.testInsertAndSelect();
    }

}

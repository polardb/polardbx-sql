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

import com.alibaba.polardbx.qatest.BinlogIgnore;
import org.junit.Ignore;
import org.junit.runners.Parameterized;

import java.util.List;

/**
 * @version 1.0
 */
@BinlogIgnore(ignoreReason = "用例涉及很多主键冲突问题，即不同分区有相同主键，复制到下游Mysql时出现Duplicate Key")
public class AutoPartitionTest extends PartitionAutoLoadSqlTestBase {

    public AutoPartitionTest(AutoLoadSqlTestCaseParams parameter) {
        super(parameter);
    }

    @Parameterized.Parameters(name = "{index}: SubTestCase {0}")
    public static List<AutoLoadSqlTestCaseParams> parameters() {
        return getParameters(AutoPartitionTest.class, 3, true);
    }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.qatest.ddl.auto.columnar;

import com.alibaba.polardbx.qatest.ddl.auto.partition.PartitionAutoLoadSqlTestBase;
import org.junit.runners.Parameterized;

import java.util.List;

public class CreateTableWithCciTest extends PartitionAutoLoadSqlTestBase {

    public CreateTableWithCciTest(AutoLoadSqlTestCaseParams parameter) {
        super(parameter);
        // Generate test.yml and .result
//        new CreateTableWithCciPartitionByHashCaseGenerator().run();
//        new CreateTableWithCciPartitionByRangeCaseGenerator().run();
    }

    @Parameterized.Parameters(name = "{index}: SubTestCase {0}")
    public static List<AutoLoadSqlTestCaseParams> parameters() {
        return getParameters(CreateTableWithCciTest.class, 3, true);
    }
}

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

import com.alibaba.polardbx.qatest.ddl.datamigration.locality.LocalityTestBase;
import com.alibaba.polardbx.qatest.ddl.datamigration.locality.LocalityTestCaseUtils.LocalityTestCaseTask;
import org.junit.Test;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.FileNotFoundException;
import java.sql.SQLException;

@NotThreadSafe
public class LocalityCciTest extends LocalityTestBase {

    public void runTestCase(String resourceFile) throws FileNotFoundException, InterruptedException, SQLException {

        /**
         * Ignore this case for debug ddl qatest
         */
        String resourceDir = "partition/env/LocalityTest/cci/" + resourceFile;
        String fileDir = getClass().getClassLoader().getResource(resourceDir).getPath();
        LocalityTestCaseTask localityTestCaseTask = new LocalityTestCaseTask(fileDir);
        localityTestCaseTask.execute(tddlConnection);
    }

    @Test
    public void testCreateCciOnLocalityTable()
        throws FileNotFoundException, InterruptedException, SQLException {
        runTestCase("create_cci_on_locality_table.test.yml");
    }

    @Test
    public void testAlterLocalityTableAddCci()
        throws FileNotFoundException, InterruptedException, SQLException {
        runTestCase("alter_locality_table_add_cci.test.yml");
    }

    @Test
    public void testCreateLocalityTableWithCci()
        throws FileNotFoundException, InterruptedException, SQLException {
        runTestCase("create_locality_table_with_cci.test.yml");
    }
}

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

package com.alibaba.polardbx.qatest.ddl.auto.tablegroup;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.ddl.auto.tablegroup.ListDefaultTestCaseUtils.ListDefaultTestCaseBean;
import com.alibaba.polardbx.qatest.ddl.auto.tablegroup.ListDefaultTestCaseUtils.ListDefaultTestCaseUtils;
import com.alibaba.polardbx.qatest.ddl.auto.tablegroup.ListDefaultTestCaseUtils.ListDefaultTestTask;
import org.junit.Test;

import java.io.FileNotFoundException;

public class ListDefaultPartitionTest extends DDLBaseNewDBTestCase {
    public void runTestCaseFromResources(String resourceFile) throws FileNotFoundException {
        String dir = "partition/env/TableGroupTest/" + resourceFile;
        String fileDir = getClass().getClassLoader().getResource(dir).getPath();
        ListDefaultTestCaseBean listDefaultTestCaseBean = ListDefaultTestCaseUtils.readFromYml(fileDir);
        ListDefaultTestTask listDefaultTestTask = new ListDefaultTestTask(listDefaultTestCaseBean);
        listDefaultTestTask.execute(tddlConnection);
        System.out.println(listDefaultTestCaseBean.dbName);
    }

    @Test
    public void testCreateAndAlterTableGroup() throws FileNotFoundException {
        runTestCaseFromResources("test_table_group_list_default.test.yml");
    }

    @Test
    public void testDML() throws FileNotFoundException {
        runTestCaseFromResources("test_table_group_list_default_dml.test.yml");
    }

}

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

package com.alibaba.polardbx.qatest.ddl.auto.locality.LocalityTestCaseUtils;


import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.version.Version;
import com.alibaba.polardbx.qatest.ddl.auto.locality.LocalityTestBase;
import com.alibaba.polardbx.qatest.ddl.auto.locality.LocalityTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class LocalityTestCaseTask {

    public LocalityTestCaseBean localityTestCaseBean;
    public Map<String, String> nodeMap;
    public Map<String, String> tableGroupMap;

    public LocalityTestCaseTask(String fileDir) throws FileNotFoundException {
        this.localityTestCaseBean = new LocalityTestCaseReader(fileDir).readTestCase();
        this.nodeMap = new HashMap<>();
        this.tableGroupMap = new HashMap<>();
     }


    public void executeCleanupDdls(Connection tddlConnection, List<String> cleanupDDls){
        for(String cleanupDDl: cleanupDDls) {
            JdbcUtil.executeUpdate(tddlConnection, cleanupDDl);
        }
    }

    public void execute(Connection tddlConnection){
        List<String> storageList = LocalityTestUtils.getDatanodes(tddlConnection);
        if(storageList.size() < this.localityTestCaseBean.getStorageList().size()){
            return;
        }
        this.nodeMap = LocalityTestUtils.generateNodeMap(storageList, this.localityTestCaseBean.getStorageList());
        for (LocalityTestCaseBean.SingleTestCase singleTestCase : localityTestCaseBean.testCases) {
            LocalitySingleTaskCaseTask singleTestCaseTask = LocalitySingleTaskCaseTask.loadFrom(singleTestCase);
            singleTestCaseTask.dbName = localityTestCaseBean.dbName;
            singleTestCaseTask.applyNodeMap(nodeMap);
            singleTestCaseTask.applyTableGroupMap(tableGroupMap);
            singleTestCaseTask.execute(tddlConnection);
            tableGroupMap.putAll(singleTestCaseTask.groupNameMap);
        }
        executeCleanupDdls(tddlConnection, this.localityTestCaseBean.getCleanupDDls());
    }
}

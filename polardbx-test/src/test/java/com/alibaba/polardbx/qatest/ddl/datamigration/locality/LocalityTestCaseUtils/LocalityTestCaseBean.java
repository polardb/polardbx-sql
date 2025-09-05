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

package com.alibaba.polardbx.qatest.ddl.datamigration.locality.LocalityTestCaseUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class LocalityTestCaseBean {
    public static class CheckActions{
        public List<String> getLocalityValueCheck() {
            return localityValueCheck;
        }

        public void setLocalityValueCheck(List<String> localityValueCheck) {
            this.localityValueCheck = localityValueCheck;
        }

        public List<String> getTableGroupMatchCheck() {
            return tableGroupMatchCheck;
        }

        public void setTableGroupMatchCheck(List<String> tableGroupMatchCheck) {
            this.tableGroupMatchCheck = tableGroupMatchCheck;
        }

        public List<String> getPartitionLocalityCheck() {
            return partitionLocalityCheck;
        }

        public void setPartitionLocalityCheck(List<String> partitionLocalityCheck) {
            this.partitionLocalityCheck = partitionLocalityCheck;
        }

        public List<String> getTopologyCheck() {
            return topologyCheck;
        }

        public void setTopologyCheck(List<String> topologyCheck) {
            this.topologyCheck = topologyCheck;
        }

        public List<String> localityValueCheck = new ArrayList<>();
        public List<String> tableGroupMatchCheck = new ArrayList<>();
        public List<String> partitionLocalityCheck = new ArrayList<>();
        public List<String> topologyCheck = new ArrayList<>();
    }

    public static class RejectDdl{
        public String getDdl() {
            return ddl;
        }

        public void setDdl(String ddl) {
            this.ddl = ddl;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }

        public String ddl;
        public String message;
    }

    public static class SingleTestCase{
        public SingleTestCase(){

        }
        public List<String> getPrepareDDls() {
            return prepareDDls;
        }

        public void setPrepareDDls(List<String> prepareDDls) {
            this.prepareDDls = prepareDDls;
        }

        public List<RejectDdl> getRejectDDls() {
            return rejectDDls;
        }

        public void setRejectDDls(List<RejectDdl> rejectDDls) {
            this.rejectDDls = rejectDDls;
        }

        public List<String> getCheckTriggers() {
            return checkTriggers;
        }

        public void setCheckTriggers(List<String> checkTriggers) {
            this.checkTriggers = checkTriggers;
        }

        public CheckActions getCheckActions() {
            return checkActions;
        }

        public void setCheckActions(CheckActions checkActions) {
            this.checkActions = checkActions;
        }


        public List<String> getCleanupDDls() {
            return cleanupDDls;
        }

        public void setCleanupDDls(List<String> cleanupDDls) {
            this.cleanupDDls = cleanupDDls;
        }

        public List<String> prepareDDls = new ArrayList<>();
        public List<String> checkTriggers = new ArrayList<>();
        public List<String> cleanupDDls = new ArrayList<>();
        public List<RejectDdl> rejectDDls = new ArrayList<>();
        public CheckActions checkActions = new CheckActions();
    }

    public List<SingleTestCase> getTestCases() {
        return testCases;
    }

    public void setTestCases(List<SingleTestCase> testCases) {
        this.testCases = testCases;
    }


    public List<String> getStorageList() {
        return storageList;
    }

    public void setStorageList(List<String> storageList) {
        this.storageList = storageList;
    }


    public List<String> getCleanupDDls() {
        return cleanupDDls;
    }

    public void setCleanupDDls(List<String> cleanupDDls) {
        this.cleanupDDls = cleanupDDls;
    }

    public List<SingleTestCase> testCases = new ArrayList<>();
    public List<String> storageList = new ArrayList<>();
    public String dbName;
    public List<String> cleanupDDls = new ArrayList<>();
}

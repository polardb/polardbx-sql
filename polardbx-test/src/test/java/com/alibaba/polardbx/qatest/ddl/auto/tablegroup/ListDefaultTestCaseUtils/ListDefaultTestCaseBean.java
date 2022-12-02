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

package com.alibaba.polardbx.qatest.ddl.auto.tablegroup.ListDefaultTestCaseUtils;

import java.util.List;

public class ListDefaultTestCaseBean {
    public List<SingleTestCase> testCases;
    public String dbName;

    public List<SingleTestCase> getTestCases() {
        return testCases;
    }

    public void setTestCases(
        List<SingleTestCase> testCases) {
        this.testCases = testCases;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public static class CheckActionDesc {
        public List<String> prepares;
        public String insertData;
        public List<CheckAnswerDesc> checkers;
        public List<CheckAnswerDesc> metaCreateTableContainsBeforeChecker;
        public List<CheckAnswerDesc> metaInfoSchemaBoundValueBeforeChecker;
        public String alterTableGroup;
        public String insertAgain;
        public List<CheckAnswerDesc> afterAlterCheckers;
        public List<CheckAnswerDesc> metaCreateTableContainsAfterChecker;
        public List<CheckAnswerDesc> metaInfoSchemaBoundValueAfterChecker;
        public String clean;

        public List<String> getPrepares() {
            return prepares;
        }

        public void setPrepares(List<String> prepares) {
            this.prepares = prepares;
        }

        public String getInsertData() {
            return insertData;
        }

        public void setInsertData(String insertData) {
            this.insertData = insertData;
        }

        public List<CheckAnswerDesc> getCheckers() {
            return checkers;
        }

        public void setCheckers(
            List<CheckAnswerDesc> checkers) {
            this.checkers = checkers;
        }

        public List<CheckAnswerDesc> getMetaCreateTableContainsBeforeChecker() {
            return metaCreateTableContainsBeforeChecker;
        }

        public void setMetaCreateTableContainsBeforeChecker(
            List<CheckAnswerDesc> metaCreateTableContainsBeforeChecker) {
            this.metaCreateTableContainsBeforeChecker = metaCreateTableContainsBeforeChecker;
        }

        public List<CheckAnswerDesc> getMetaInfoSchemaBoundValueBeforeChecker() {
            return metaInfoSchemaBoundValueBeforeChecker;
        }

        public void setMetaInfoSchemaBoundValueBeforeChecker(
            List<CheckAnswerDesc> metaInfoSchemaBoundValueBeforeChecker) {
            this.metaInfoSchemaBoundValueBeforeChecker = metaInfoSchemaBoundValueBeforeChecker;
        }

        public String getAlterTableGroup() {
            return alterTableGroup;
        }

        public void setAlterTableGroup(String alterTableGroup) {
            this.alterTableGroup = alterTableGroup;
        }

        public String getInsertAgain() {
            return insertAgain;
        }

        public void setInsertAgain(String insertAgain) {
            this.insertAgain = insertAgain;
        }

        public List<CheckAnswerDesc> getAfterAlterCheckers() {
            return afterAlterCheckers;
        }

        public void setAfterAlterCheckers(
            List<CheckAnswerDesc> afterAlterCheckers) {
            this.afterAlterCheckers = afterAlterCheckers;
        }

        public List<CheckAnswerDesc> getMetaCreateTableContainsAfterChecker() {
            return metaCreateTableContainsAfterChecker;
        }

        public void setMetaCreateTableContainsAfterChecker(
            List<CheckAnswerDesc> metaCreateTableContainsAfterChecker) {
            this.metaCreateTableContainsAfterChecker = metaCreateTableContainsAfterChecker;
        }

        public List<CheckAnswerDesc> getMetaInfoSchemaBoundValueAfterChecker() {
            return metaInfoSchemaBoundValueAfterChecker;
        }

        public void setMetaInfoSchemaBoundValueAfterChecker(
            List<CheckAnswerDesc> metaInfoSchemaBoundValueAfterChecker) {
            this.metaInfoSchemaBoundValueAfterChecker = metaInfoSchemaBoundValueAfterChecker;
        }

        public String getClean() {
            return clean;
        }

        public void setClean(String clean) {
            this.clean = clean;
        }
    }

    public static class CheckAnswerDesc {
        public String query;
        public String answer;

        public String getQuery() {
            return query;
        }

        public void setQuery(String query) {
            this.query = query;
        }

        public String getAnswer() {
            return answer;
        }

        public void setAnswer(String answer) {
            this.answer = answer;
        }
    }

    public static class SingleTestCase {
        public List<String> prepareDDLs;
        public List<String> syntaxWrongDDLs;
        public List<CheckActionDesc> checkActions;
        public String cleanDDL;

        public List<String> getPrepareDDLs() {
            return prepareDDLs;
        }

        public void setPrepareDDLs(List<String> prepareDDL) {
            this.prepareDDLs = prepareDDL;
        }

        public List<String> getSyntaxWrongDDLs() {
            return syntaxWrongDDLs;
        }

        public void setSyntaxWrongDDLs(List<String> syntaxWrongDDLs) {
            this.syntaxWrongDDLs = syntaxWrongDDLs;
        }

        public List<CheckActionDesc> getCheckActions() {
            return checkActions;
        }

        public void setCheckActions(
            List<CheckActionDesc> checkActions) {
            this.checkActions = checkActions;
        }

        public String getCleanDDL() {
            return cleanDDL;
        }

        public void setCleanDDL(String cleanDDL) {
            this.cleanDDL = cleanDDL;
        }
    }
}

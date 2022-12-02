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

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.qatest.util.JdbcUtil;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static com.google.common.truth.Truth.assertWithMessage;

public class ListDefaultTestTask {
    public static final Logger LOG = LoggerFactory.getLogger(ListDefaultTestTask.class);
    private ListDefaultTestCaseBean listDefaultTestCaseBean;

    public ListDefaultTestTask(ListDefaultTestCaseBean listDefaultTestCaseBean) {
        this.listDefaultTestCaseBean = listDefaultTestCaseBean;
    }

    public void execute(Connection tddlConnection) {
        List<ListDefaultTestCaseBean.SingleTestCase> testCases = listDefaultTestCaseBean.getTestCases();
        if (testCases != null) {
            for (ListDefaultTestCaseBean.SingleTestCase testcase : testCases) {
                executeSingleTestCase(testcase, tddlConnection);
            }
        }
    }

    private boolean isDML(String sql) {
        return sql.contains("replace") || sql.contains("update") || sql.contains("delete") || sql.contains("insert");
    }

    private void executeSingleTestCase(ListDefaultTestCaseBean.SingleTestCase testCase, Connection tddlConnection) {
        if (testCase.getPrepareDDLs() != null && !testCase.getPrepareDDLs().isEmpty()) {
            for (String prepareDDL : testCase.getPrepareDDLs()) {
                LOG.info(String.format("prepare [%s]", prepareDDL));
                JdbcUtil.executeUpdateSuccess(tddlConnection, prepareDDL);
            }
        }
        if (testCase.getSyntaxWrongDDLs() != null) {
            for (String syntaxWrongDDL : testCase.getSyntaxWrongDDLs()) {
                LOG.info(String.format("syntaxWrongDDL [%s]", syntaxWrongDDL));
                JdbcUtil.executeUpdateFailed(tddlConnection, syntaxWrongDDL, "ERR-CODE:");
            }
        }
        if (testCase.getCheckActions() != null) {
            for (ListDefaultTestCaseBean.CheckActionDesc checkAction : testCase.getCheckActions()) {
                List<String> prepares = checkAction.getPrepares();
                if (prepares != null) {
                    for (String prepare : prepares) {
                        LOG.info(String.format("CheckAction prepare [%s]", prepare));
                        JdbcUtil.executeUpdateSuccess(tddlConnection, prepare);
                    }
                }

                String insert = checkAction.getInsertData();
                if (insert != null) {
                    LOG.info(String.format("CheckAction insert [%s]", insert));
                    JdbcUtil.executeUpdateSuccess(tddlConnection, insert);
                }

                List<ListDefaultTestCaseBean.CheckAnswerDesc> checkers = checkAction.getCheckers();
                boolean enableGsiCheck = true;
                if (checkers != null && checkers.size() != 0) {
                    for (ListDefaultTestCaseBean.CheckAnswerDesc checker : checkers) {
                        if (isDML(checker.query)) {
                            enableGsiCheck = false;
                        }
                        //check table data
                        executeQueryAndCheckAnswerForTable(checker, tddlConnection);
                        //check GSI data
                        if (enableGsiCheck) {
                            executeQueryAndCheckAnswerForGsi(checker, tddlConnection);
                        }
                    }
                }

                /**
                 * check create table before alter
                 * */
                List<ListDefaultTestCaseBean.CheckAnswerDesc> metaCreateTableContainsBeforeChecker =
                    checkAction.getMetaCreateTableContainsBeforeChecker();
                if (metaCreateTableContainsBeforeChecker != null && metaCreateTableContainsBeforeChecker.size() != 0) {
                    LOG.info(String.format("CheckAction metaCreateTableContainsBeforeChecker [%s]",
                        metaCreateTableContainsBeforeChecker));
                    for (ListDefaultTestCaseBean.CheckAnswerDesc checker : metaCreateTableContainsBeforeChecker) {
                        executeQueryAndCheckForMetaCreateTable(checker, tddlConnection);
                    }
                }

                /**
                 * check infoSchema before alter
                 * */
                List<ListDefaultTestCaseBean.CheckAnswerDesc> metaInfoSchemaBoundValueBeforeChecker =
                    checkAction.getMetaInfoSchemaBoundValueBeforeChecker();
                if (metaInfoSchemaBoundValueBeforeChecker != null
                    && metaInfoSchemaBoundValueBeforeChecker.size() != 0) {
                    LOG.info(String.format("CheckAction metaInfoSchemaBoundValueBeforeChecker [%s]",
                        metaInfoSchemaBoundValueBeforeChecker));
                    for (ListDefaultTestCaseBean.CheckAnswerDesc checker : metaInfoSchemaBoundValueBeforeChecker) {
                        executeQueryAndCheckForBoundValueFromInfoSchema(checker, tddlConnection);
                    }
                }

                String alterTableGroup = checkAction.getAlterTableGroup();
                if (alterTableGroup != null) {
                    LOG.info(String.format("CheckAction alterTableGroup [%s]", alterTableGroup));
                    JdbcUtil.executeUpdateSuccess(tddlConnection, alterTableGroup);
                }

                String insertAgain = checkAction.getInsertAgain();
                if (insertAgain != null) {
                    LOG.info(String.format("CheckAction insertAgain [%s]", insertAgain));
                    JdbcUtil.executeUpdateSuccess(tddlConnection, insertAgain);
                }

                List<ListDefaultTestCaseBean.CheckAnswerDesc> afterAlterCheckers = checkAction.getAfterAlterCheckers();
                if (afterAlterCheckers != null && afterAlterCheckers.size() != 0) {
                    for (ListDefaultTestCaseBean.CheckAnswerDesc checker : afterAlterCheckers) {
                        //check table data
                        executeQueryAndCheckAnswerForTable(checker, tddlConnection);
                        //check GSI data
                        executeQueryAndCheckAnswerForGsi(checker, tddlConnection);
                    }
                }

                String clean = checkAction.getClean();
                if (clean != null) {
                    LOG.info(String.format("CheckAction clean[%s]", clean));
                    JdbcUtil.executeUpdateSuccess(tddlConnection, clean);
                }
            }
        }

        if (testCase.getCleanDDL() != null) {
            LOG.info(String.format("clean [%s]", testCase.getCleanDDL()));
            JdbcUtil.executeUpdateSuccess(tddlConnection, testCase.getCleanDDL());
        }
    }

    private void executeQueryAndCheckAnswerForTable(ListDefaultTestCaseBean.CheckAnswerDesc checker,
                                                    Connection tddlConnection) {
        String query = checker.getQuery();
        String answer = checker.getAnswer();
        if (query == null) {
            return;
        }
        Set<String> queryAnswers = new HashSet<>();
        try {
            if (isDML(checker.query)) {
                JdbcUtil.executeUpdate(tddlConnection, query);
            } else {
                ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, query);
                while (rs.next()) {
                    String data = rs.getString("a");
                    queryAnswers.add(data);
                }
            }
        } catch (SQLException ex) {
            assertWithMessage("语句并未按照预期执行成功:" + ex.getMessage()).fail();
        }
        if (isDML(checker.query)) {
            LOG.info(String.format("executeQueryAndCheckForTable: query[%s] ", query));
            return;
        }
        Set<String> groundTruth = ListDefaultTestCaseUtils.parseAnswer(answer);

        LOG.info(String.format("executeQueryAndCheckForTable: query[%s] groundTruth[%s] answer[%s]", query, groundTruth,
            queryAnswers));
        Assert.assertTrue(groundTruth.equals(queryAnswers));
    }

    private List<String> getGsiTableNames(String tableName, Connection tddlConnection) {
        List<String> GsiNames = new ArrayList<>();
        String sql = "show global index from " + tableName + ";";
        try {
            ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
            while (rs.next()) {
                String data = rs.getString("KEY_NAME");
                GsiNames.add(data);
            }
        } catch (SQLException ex) {
            assertWithMessage("语句并未按照预期执行成功:" + ex.getMessage()).fail();
        }
        return GsiNames;
    }

    private void executeQueryAndCheckAnswerForGsi(ListDefaultTestCaseBean.CheckAnswerDesc checker,
                                                  Connection tddlConnection) {
        String query = checker.getQuery();
        String answer = checker.getAnswer();
        if (query == null || isDML(checker.query)) {
            return;
        }
        /**
         * GSI not support: select * from GSI partition(p0);
         * */
        if (query.contains("partition")) {
            return;
        }

        List<String> GsiNames = getGsiTableNames("tb", tddlConnection);
        for (String tableName : GsiNames) {
            query = query.replace("tb", tableName);
            Set<String> queryAnswers = new HashSet<>();
            try {
                ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, query);
                while (rs.next()) {
                    String data = rs.getString("a");
                    queryAnswers.add(data);
                }
            } catch (SQLException ex) {
                assertWithMessage("语句并未按照预期执行成功:" + ex.getMessage()).fail();
            }
            Set<String> groundTruth = ListDefaultTestCaseUtils.parseAnswer(answer);

            LOG.info(
                String.format("executeQueryAndCheckForGsi: query[%s] groundTruth[%s] answer[%s]", query, groundTruth,
                    queryAnswers));
            Assert.assertTrue(groundTruth.equals(queryAnswers));
        }
    }

    private void executeQueryAndCheckForMetaCreateTable(
        ListDefaultTestCaseBean.CheckAnswerDesc metaCreateTableContainsBeforeChecker, Connection tddlConnection) {
        String query = metaCreateTableContainsBeforeChecker.query;
        String answer = metaCreateTableContainsBeforeChecker.answer;
        if (query == null || answer == null) {
            return;
        }
        String data = null;
        try {
            ResultSet rs = JdbcUtil.executeQuery(query, tddlConnection);
            while (rs.next()) {
                data = rs.getString("CREATE TABLE");
            }
        } catch (SQLException ex) {
            assertWithMessage("语句并未按照预期执行成功:" + ex.getMessage()).fail();
        }
        LOG.info(String.format("executeQueryAndCheckForMetaCreateTable: groundTruth[%s], answer[%s]", answer, data));
        Assert.assertTrue(data != null && data.toLowerCase(Locale.ROOT).contains(answer.toLowerCase(Locale.ROOT)));
    }

    private void executeQueryAndCheckForBoundValueFromInfoSchema(
        ListDefaultTestCaseBean.CheckAnswerDesc metaCreateTableContainsBeforeChecker, Connection tddlConnection) {
        String query = metaCreateTableContainsBeforeChecker.query;
        String answer = metaCreateTableContainsBeforeChecker.answer;
        if (query == null || answer == null) {
            return;
        }
        String data = null;
        try {
            ResultSet rs = JdbcUtil.executeQuery(query, tddlConnection);
            while (rs.next()) {
                data = rs.getString("BOUND_VALUE");
            }
        } catch (SQLException ex) {
            assertWithMessage("语句并未按照预期执行成功:" + ex.getMessage()).fail();
        }
        LOG.info(String.format("executeQueryAndCheckForBoundValueFromInfoSchema: groundTruth[%s], answer[%s]", answer,
            data));
        Assert.assertTrue(data != null && data.equalsIgnoreCase(answer));
    }
}

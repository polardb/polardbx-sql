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

package com.alibaba.polardbx.qatest.validator;

import com.alibaba.polardbx.qatest.data.ColumnDataGenerator;
import com.alibaba.polardbx.qatest.entity.ColumnEntity;
import com.alibaba.polardbx.qatest.entity.TableEntity;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeBatchOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataOperator.executeMutilValueOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlOrTddl;

/**
 * 数据准备类
 *
 * @author zhuoxue.yll
 * <p>
 * 2016年12月10日
 */
public class PrepareData {

    /**
     * 数据准备,从主键我0开始插入，插入count条
     */
    public static void tableDataPrepare(String tableName, int count,
                                        List<ColumnEntity> columns, String pkColumnName, Connection mysqlConnection,
                                        Connection tddlConnection, ColumnDataGenerator columnDataGenerator) {
        tableDataPrepare(tableName, 0, count, columns, pkColumnName, mysqlConnection,
            tddlConnection, columnDataGenerator);
    }

    /**
     * 数据准备,从主键start 开始，插入count条
     */
    public static void tableDataPrepare(String tableName, int start, int count,
                                        List<ColumnEntity> columns, String pkColumnName, Connection mysqlConnection,
                                        Connection tddlConnection,
                                        ColumnDataGenerator columnDataGenerator) {
        tableDataPrepareWithMultiValue(
            tableName, start, count, columns, pkColumnName, mysqlConnection, tddlConnection, columnDataGenerator);
    }

    /**
     * 数据准备,从主键start 开始，插入count条
     */
    public static void tableDataPrepareWithMultiValue(String tableName, int start, int count,
                                                      List<ColumnEntity> columns, String pkColumnName,
                                                      Connection mysqlConnection,
                                                      Connection tddlConnection,
                                                      ColumnDataGenerator columnDataGenerator) {
        String sql = null;
        sql = "delete from  " + tableName;
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

        sql = columnDataGenerator.getInsertSqlTemplateWithMultiValue(new TableEntity(tableName, columns));
        List<List<Object>> params = new ArrayList<List<Object>>();
        for (int i = start; i < count; i++) {
            List<Object> param = columnDataGenerator.getAllColumnValue(columns, pkColumnName, i);
            params.add(param);
        }
        executeMutilValueOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, params);
    }

    /**
     * 数据准备,从主键start 开始，插入count条
     */
    public static void tableDataPrepareWithBatch(String tableName, int start, int count,
                                                 List<ColumnEntity> columns, String pkColumnName,
                                                 Connection mysqlConnection,
                                                 Connection tddlConnection,
                                                 ColumnDataGenerator columnDataGenerator) {
        String sql = null;
        sql = "delete from  " + tableName;
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

        sql = columnDataGenerator.getInsertSqlTemplate(new TableEntity(tableName, columns));
        List<List<Object>> params = new ArrayList<List<Object>>();
        for (int i = start; i < start + count; i++) {
            List<Object> param = columnDataGenerator.getAllColumnValue(columns, pkColumnName, i);
            params.add(param);
        }
        executeBatchOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, params);
    }

    public static void tableDataPrepare(String tableName, int count, List<ColumnEntity> columns, String pkColumnName,
                                        Connection tddlConnection, ColumnDataGenerator columnDataGenerator) {
        String sql = "delete from  " + tableName;
        executeOnMysqlOrTddl(tddlConnection, sql, null);

        sql = columnDataGenerator.getInsertSqlTemplate(new TableEntity(tableName, columns));

        List<List<Object>> params = new ArrayList<List<Object>>();
        for (int i = 0; i < count; i++) {
            List<Object> param = columnDataGenerator.getAllColumnValue(columns, pkColumnName, i);
            params.add(param);
            executeOnMysqlOrTddl(tddlConnection, sql, param);
        }
    }

    public static void tableDataPrepareRD(String tableName, int count, List<ColumnEntity> columns, String pkColumnName,
                                          Connection tddlConnection, ColumnDataGenerator columnDataGenerator) {
        String sql = "delete from  " + tableName;
        executeOnMysqlOrTddl(tddlConnection, sql, null);

        sql = columnDataGenerator.getInsertSqlTemplate(new TableEntity(tableName, columns));

        List<List<Object>> params = new ArrayList<List<Object>>();
        for (int i = 0; i < count; i++) {
            List<Object> param = columnDataGenerator.getAllColumnValueRD(columns, pkColumnName, i);
            params.add(param);
            executeOnMysqlOrTddl(tddlConnection, sql, param);
        }
    }
}

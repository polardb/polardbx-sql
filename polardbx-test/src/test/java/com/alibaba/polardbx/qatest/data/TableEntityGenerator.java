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

package com.alibaba.polardbx.qatest.data;

import com.alibaba.polardbx.qatest.entity.ColumnEntity;
import com.alibaba.polardbx.qatest.entity.TableEntity;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by xiaowen.guoxw on 2017/3/26.
 * 这里仅仅定义最常用的表如何获取
 */
public class TableEntityGenerator {

    public static List<TableEntity> getFiveTypeTableEntity(List<ColumnEntity> columns, String tbName,
                                                           String dbPartitionKey, String tbPartitionKey,
                                                           int tbPartitionNum) {

        return getFiveTypeTableEntity(columns, tbName, dbPartitionKey, "hash", tbPartitionKey, "hash", tbPartitionNum);
    }

    public static List<TableEntity> getFiveTypeTableEntity(List<ColumnEntity> columns, String tbName,
                                                           String dbPartitionKey, String dbPartitionFunction,
                                                           String tbPartitionKey, String tbPartitionFunction,
                                                           int tbPartitionNum) {

        List<TableEntity> resultTableList = new ArrayList<TableEntity>();

        if (!(tbName.trim().endsWith("_"))) {
            tbName = tbName + "_";
        }

        //单表
        TableEntity tableEntityOneDbOneTb = new TableEntity();
        tableEntityOneDbOneTb.setTbName(tbName + ExecuteTableName.ONE_DB_ONE_TB_SUFFIX);
        tableEntityOneDbOneTb.setColumnInfos(columns);
        resultTableList.add(tableEntityOneDbOneTb);

        //广播表
        TableEntity tableEntityBroadcast = new TableEntity();
        tableEntityBroadcast.setTbName(tbName + ExecuteTableName.BROADCAST_TB_SUFFIX);
        tableEntityBroadcast.setColumnInfos(columns);
        tableEntityBroadcast.setBroadcast(true);
        resultTableList.add(tableEntityBroadcast);

        resultTableList.addAll(
            getThreePartitionTableEntity(columns, tbName, dbPartitionKey, dbPartitionFunction, tbPartitionKey,
                tbPartitionFunction, tbPartitionNum));

        return resultTableList;
    }

    public static List<TableEntity> getFiveTypeCHNTableEntity(List<ColumnEntity> columns, String tbName,
                                                              String dbPartitionKey, String tbPartitionKey,
                                                              int tbPartitionNum) {

        List<TableEntity> resultTableList = new ArrayList<TableEntity>();

        if (!(tbName.trim().endsWith("_"))) {
            tbName = tbName + "_";
        }

        //单表
        TableEntity tableEntityOneDbOneTb = new TableEntity();
        tableEntityOneDbOneTb.setTbName(tbName + ExecuteCHNTableName.ONE_DB_ONE_TB_SUFFIX);
        tableEntityOneDbOneTb.setColumnInfos(columns);
        resultTableList.add(tableEntityOneDbOneTb);

        //广播表
        TableEntity tableEntityBroadcast = new TableEntity();
        tableEntityBroadcast.setTbName(tbName + ExecuteCHNTableName.BROADCAST_TB_SUFFIX);
        tableEntityBroadcast.setColumnInfos(columns);
        tableEntityBroadcast.setBroadcast(true);
        resultTableList.add(tableEntityBroadcast);

        resultTableList
            .addAll(getThreePartitionCHNTableEntity(columns, tbName, dbPartitionKey, tbPartitionKey, tbPartitionNum));

        return resultTableList;
    }

    public static List<TableEntity> getThreePartitionTableEntity(List<ColumnEntity> columns, String tbName,
                                                                 String dbPartitionKey, String tbPartitionKey,
                                                                 int tbPartitionNum) {

        return getThreePartitionTableEntity(columns, tbName, dbPartitionKey, "hash", tbPartitionKey, "hash",
            tbPartitionNum);
    }

    public static List<TableEntity> getThreePartitionTableEntity(List<ColumnEntity> columns, String tbName,
                                                                 String dbPartitionKey, String dbPartitionFunction,
                                                                 String tbPartitionKey, String tbPartitionFunction,
                                                                 int tbPartitionNum) {

        List<TableEntity> resultTableList = new ArrayList<TableEntity>();

        if (!(tbName.trim().endsWith("_"))) {
            tbName = tbName + "_";
        }

        //单库多表
        TableEntity tableEntityOneDbMultiTb = new TableEntity();
        tableEntityOneDbMultiTb.setTbName(tbName + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX);
        tableEntityOneDbMultiTb.setColumnInfos(columns);
        tableEntityOneDbMultiTb.setTbpartitionKey(tbPartitionKey);
        tableEntityOneDbMultiTb.setTbpartitionNum(tbPartitionNum);
        tableEntityOneDbMultiTb.setTbpartitionFunction(tbPartitionFunction);
        resultTableList.add(tableEntityOneDbMultiTb);

        //多库多表
        TableEntity tableEntityMultiDbMultiTb = new TableEntity();
        tableEntityMultiDbMultiTb.setTbName(tbName + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX);
        tableEntityMultiDbMultiTb.setColumnInfos(columns);
        tableEntityMultiDbMultiTb.setDbpartitionKey(dbPartitionKey);
        tableEntityMultiDbMultiTb.setTbpartitionKey(tbPartitionKey);
        tableEntityMultiDbMultiTb.setTbpartitionNum(tbPartitionNum);
        tableEntityMultiDbMultiTb.setDbpartitionFunction(dbPartitionFunction);
        tableEntityMultiDbMultiTb.setTbpartitionFunction(tbPartitionFunction);
        resultTableList.add(tableEntityMultiDbMultiTb);

        //多库单表
        TableEntity tableEntityMultiDbOneTb = new TableEntity();
        tableEntityMultiDbOneTb.setTbName(tbName + ExecuteTableName.MULTI_DB_ONE_TB_SUFFIX);
        tableEntityMultiDbOneTb.setColumnInfos(columns);
        tableEntityMultiDbOneTb.setDbpartitionKey(dbPartitionKey);
        tableEntityMultiDbOneTb.setDbpartitionFunction(dbPartitionFunction);
        resultTableList.add(tableEntityMultiDbOneTb);

        return resultTableList;
    }

    public static List<TableEntity> getThreePartitionCHNTableEntity(List<ColumnEntity> columns, String tbName,
                                                                    String dbPartitionKey, String tbPartitionKey,
                                                                    int tbPartitionNum) {

        List<TableEntity> resultTableList = new ArrayList<TableEntity>();

        if (!(tbName.trim().endsWith("_"))) {
            tbName = tbName + "_";
        }

        //单库多表
        TableEntity tableEntityOneDbMultiTb = new TableEntity();
        tableEntityOneDbMultiTb.setTbName(tbName + ExecuteCHNTableName.ONE_DB_MUTIL_TB_SUFFIX);
        tableEntityOneDbMultiTb.setColumnInfos(columns);
        tableEntityOneDbMultiTb.setTbpartitionKey(tbPartitionKey);
        tableEntityOneDbMultiTb.setTbpartitionNum(tbPartitionNum);
        resultTableList.add(tableEntityOneDbMultiTb);

        //多库多表
        TableEntity tableEntityMultiDbMultiTb = new TableEntity();
        tableEntityMultiDbMultiTb.setTbName(tbName + ExecuteCHNTableName.MUlTI_DB_MUTIL_TB_SUFFIX);
        tableEntityMultiDbMultiTb.setColumnInfos(columns);
        tableEntityMultiDbMultiTb.setDbpartitionKey(dbPartitionKey);
        tableEntityMultiDbMultiTb.setTbpartitionKey(tbPartitionKey);
        tableEntityMultiDbMultiTb.setTbpartitionNum(tbPartitionNum);
        resultTableList.add(tableEntityMultiDbMultiTb);

        //多库单表
        TableEntity tableEntityMultiDbOneTb = new TableEntity();
        tableEntityMultiDbOneTb.setTbName(tbName + ExecuteCHNTableName.MULTI_DB_ONE_TB_SUFFIX);
        tableEntityMultiDbOneTb.setColumnInfos(columns);
        tableEntityMultiDbOneTb.setDbpartitionKey(dbPartitionKey);
        resultTableList.add(tableEntityMultiDbOneTb);

        return resultTableList;
    }

    public static List<TableEntity> getFiveTypeTableEntity(String columnSql, String tbName, String dbPartitionKey,
                                                           String tbPartitionKey, int tbPartitionNum) {

        List<TableEntity> resultTableList = new ArrayList<TableEntity>();

        if (!(tbName.trim().endsWith("_"))) {
            tbName = tbName + "_";
        }

        //单表
        TableEntity tableEntityOneDbOneTb = new TableEntity();
        tableEntityOneDbOneTb.setTbName(tbName + ExecuteTableName.ONE_DB_ONE_TB_SUFFIX);
        tableEntityOneDbOneTb.setColumnSql(columnSql);
        resultTableList.add(tableEntityOneDbOneTb);

        //广播表
        TableEntity tableEntityBroadcast = new TableEntity();
        tableEntityBroadcast.setTbName(tbName + ExecuteTableName.BROADCAST_TB_SUFFIX);
        tableEntityBroadcast.setColumnSql(columnSql);
        tableEntityBroadcast.setBroadcast(true);
        resultTableList.add(tableEntityBroadcast);

        resultTableList
            .addAll(getThreePartitionTableEntity(columnSql, tbName, dbPartitionKey, tbPartitionKey, tbPartitionNum));

        return resultTableList;
    }

    public static List<TableEntity> getThreePartitionTableEntity(String columnSql, String tbName, String dbPartitionKey,
                                                                 String tbPartitionKey, int tbPartitionNum) {

        List<TableEntity> resultTableList = new ArrayList<TableEntity>();

        if (!(tbName.trim().endsWith("_"))) {
            tbName = tbName + "_";
        }

        //单库多表
        TableEntity tableEntityOneDbMultiTb = new TableEntity();
        tableEntityOneDbMultiTb.setTbName(tbName + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX);
        tableEntityOneDbMultiTb.setColumnSql(columnSql);
        tableEntityOneDbMultiTb.setTbpartitionKey(tbPartitionKey);
        tableEntityOneDbMultiTb.setTbpartitionNum(tbPartitionNum);
        resultTableList.add(tableEntityOneDbMultiTb);

        //多库多表
        TableEntity tableEntityMultiDbMultiTb = new TableEntity();
        tableEntityMultiDbMultiTb.setTbName(tbName + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX);
        tableEntityMultiDbMultiTb.setColumnSql(columnSql);
        tableEntityMultiDbMultiTb.setDbpartitionKey(dbPartitionKey);
        tableEntityMultiDbMultiTb.setTbpartitionKey(tbPartitionKey);
        tableEntityMultiDbMultiTb.setTbpartitionNum(tbPartitionNum);
        resultTableList.add(tableEntityMultiDbMultiTb);

        //多库单表
        TableEntity tableEntityMultiDbOneTb = new TableEntity();
        tableEntityMultiDbOneTb.setTbName(tbName + ExecuteTableName.MULTI_DB_ONE_TB_SUFFIX);
        tableEntityMultiDbOneTb.setColumnSql(columnSql);
        tableEntityMultiDbOneTb.setDbpartitionKey(dbPartitionKey);
        resultTableList.add(tableEntityMultiDbOneTb);

        return resultTableList;
    }

    /**
     * 测试常用的表, 不带自增列
     */
    public static TableEntity getNormalTableEntity(String tbName) {
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        TableEntity tableEntity = new TableEntity(tbName, columns);
        return tableEntity;
    }

    /**
     * 测试常用的自增表
     */
    public static TableEntity getNormalAutonicTableEntity(String tbName) {
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColumAutonic();
        TableEntity tableEntity = new TableEntity(tbName, columns);
        return tableEntity;

    }

    /**
     * 测试常用的中文表， 不带自增列
     */
    public static TableEntity getNormalCHNTableEntity(String tbName) {
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeCHNColum();
        TableEntity tableEntity = new TableEntity(tbName, columns);
        return tableEntity;
    }

    /**
     * 测试常用的自增中文表
     */
    public static TableEntity getNormalAutonicCHNTableEntity(String tbName) {
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeCHNColumAutonic();
        TableEntity tableEntity = new TableEntity(tbName, columns);
        return tableEntity;

    }

    /**
     * TDDL使用的最全类型的表
     */
    public static TableEntity getTDDLMableEntity(String tbName) {
        List<ColumnEntity> columns = TableColumnGenerator.allTDDLMTypeColumn();
        TableEntity tableEntity = new TableEntity(tbName, columns);
        return tableEntity;
    }

}

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

package com.alibaba.polardbx.gms.migration;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by zhuqiwei.
 */
public class TableMigrationTaskInfo {
    public String tableSchemaSrc;
    public String tableSchemaDst;
    public String tableName;
    public String createSqlSrc;
    public String createSqlDst;
    public boolean succeed;
    public String errorInfo;

    public Map<String, String> errorInfoForSequence;

    public TableMigrationTaskInfo() {
    }

    @JSONCreator
    public TableMigrationTaskInfo(String tableSchemaSrc, String tableSchemaDst, String tableName, boolean succeed,
                                  String errorInfo, String createSqlSrc, String createSqlDst) {
        this.tableSchemaSrc = tableSchemaSrc;
        this.tableSchemaDst = tableSchemaDst;
        this.tableName = tableName;
        this.succeed = succeed;
        this.errorInfo = errorInfo;
        this.createSqlSrc = createSqlSrc;
        this.createSqlDst = createSqlDst;
        this.errorInfoForSequence = new TreeMap<>();
    }

    public String getTableSchemaSrc() {
        return tableSchemaSrc;
    }

    public void setTableSchemaSrc(String tableSchemaSrc) {
        this.tableSchemaSrc = tableSchemaSrc;
    }

    public String getTableSchemaDst() {
        return tableSchemaDst;
    }

    public void setTableSchemaDst(String tableSchemaDst) {
        this.tableSchemaDst = tableSchemaDst;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public boolean isSucceed() {
        return succeed;
    }

    public void setSucceed(boolean succeed) {
        this.succeed = succeed;
    }

    public String getErrorInfo() {
        return errorInfo;
    }

    public void setErrorInfo(String errorInfo) {
        this.errorInfo = errorInfo;
    }

    public String getCreateSqlSrc() {
        return createSqlSrc;
    }

    public void setCreateSqlSrc(String createSqlSrc) {
        this.createSqlSrc = createSqlSrc;
    }

    public String getCreateSqlDst() {
        return createSqlDst;
    }

    public void setCreateSqlDst(String createSqlDst) {
        this.createSqlDst = createSqlDst;
    }

    public void setErrorInfoForSequence(Map<String, String> errorInfoForSequence) {
        this.errorInfoForSequence = errorInfoForSequence;
    }

    public Map<String, String> getErrorInfoForSequence() {
        return this.errorInfoForSequence;
    }

}

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

package com.alibaba.polardbx.qatest.entity;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.List;

public class PartTableEntity implements Cloneable {

    private static final Log log = LogFactory.getLog(PartTableEntity.class);

    private String dbName = null;
    private String tbName = null;

    private List<ColumnEntity> columnInfos = null;
    private String columnSql = null;

    private String partitionFunction = "hash";
    private String partitionKey = null;
    private int partitionNum = -1;

    private String primaryKey = null;
    private String uniqueKey = null;

    // 如果包含自增的话，指定自增的起始值
    private String autoIncrementValue = null;

    private boolean isSingle;
    private boolean isBroadcast;

    private String charset = "utf8mb4";
    private String engine = "InnoDB";

    public PartTableEntity() {
    }

    public PartTableEntity(String tbName,
                           List<ColumnEntity> columnInfos) {
        this.tbName = tbName;
        this.columnInfos = columnInfos;
    }

    public PartTableEntity(String tbName,
                           String columnSql) {
        this.tbName = tbName;
        this.columnSql = columnSql;
    }

    public PartTableEntity(String tbName, List<ColumnEntity> columnInfos, String partitionKey, int partitionNum) {
        this.tbName = tbName;
        this.columnInfos = columnInfos;
        this.partitionKey = partitionKey;
        this.partitionNum = partitionNum;
    }

    public PartTableEntity(String tbName, List<ColumnEntity> columnInfos, boolean isSingle, boolean isBroadcast) {
        this.tbName = tbName;
        this.columnInfos = columnInfos;
        this.isSingle = isSingle;
        this.isBroadcast = isBroadcast;
    }

    public String getColumnSql() {
        return columnSql;
    }

    public void setColumnSql(String columnSql) {
        this.columnSql = columnSql;
    }

    public String getEngine() {
        return engine;
    }

    public void setEngine(String engine) {
        this.engine = engine;
    }

    public String getCharset() {
        return charset;
    }

    public void setCharset(String charset) {
        this.charset = charset;
    }

    public String getAutoIncrementValue() {
        return autoIncrementValue;
    }

    public void setAutoIncrementValue(String autoIncrementValue) {
        this.autoIncrementValue = autoIncrementValue;
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(String primaryKey) {
        this.primaryKey = primaryKey;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getTbName() {
        return tbName;
    }

    public void setTbName(String tbName) {
        this.tbName = tbName;
    }

    public List<ColumnEntity> getColumnInfos() {
        return columnInfos;
    }

    public void setColumnInfos(List<ColumnEntity> columnInfos) {
        this.columnInfos = columnInfos;
    }

    public String getPartitionKey() {
        return partitionKey;
    }

    public void setPartitionKey(String partitionKey) {
        this.partitionKey = partitionKey;
    }

    public int getPartitionNum() {
        return partitionNum;
    }

    public void setPartitionNum(int partitionNum) {
        this.partitionNum = partitionNum;
    }

    public boolean isSingle() {
        return isSingle;
    }

    public void setSingle(boolean single) {
        isSingle = single;
    }

    public boolean isBroadcast() {
        return isBroadcast;
    }

    public void setBroadcast(boolean isBroadcast) {
        this.isBroadcast = isBroadcast;
    }

    public String getPartitionFunction() {
        return partitionFunction;
    }

    public void setPartitionFunction(String partitionFunction) {
        this.partitionFunction = partitionFunction;
    }

    public String getUniqueKey() {
        return uniqueKey;
    }

    public void setUniqueKey(String uniqueKey) {
        this.uniqueKey = uniqueKey;
    }

    public String getCreateTableDdl() {
        return getCreateTableDdl(false);
    }

    public String getCreateTableDdl(boolean ifNotExists) {
        //表名为空，返回null
        if (StringUtils.isBlank(tbName)) {
            return null;
        }

        //没有任何列定义，返回null
        if ((StringUtils.isBlank(columnSql)) && ((columnInfos == null) || (columnInfos.size() == 0))) {
            return null;
        }

        StringBuilder ddlSql = new StringBuilder("create table").append(" ");

        if (ifNotExists) {
            ddlSql.append("if not exists").append(" ");
        }

        ddlSql.append(getTbName()).append(" ");

        ddlSql.append("(");
        //优先读取columnSql
        String columnDefinition = columnSql;
        if (StringUtils.isBlank(columnDefinition)) {
            columnDefinition = columnEntityListToString(columnInfos);
        }

        ddlSql.append(columnDefinition).append(" ");

        if ((!StringUtils.isBlank(getPrimaryKey()) && (!columnDefinition.toUpperCase().contains("PRIMARY KEY")))) {
            ddlSql.append(", primary key (").append(getPrimaryKey()).append(")");
        }

        if ((!StringUtils.isBlank(getUniqueKey()) && (!columnDefinition.toUpperCase().contains("UNIQUE KEY")))) {
            ddlSql.append(", unique key(").append(getUniqueKey()).append(")");
        }

        ddlSql.append(")").append(" ");

        if (!StringUtils.isBlank(engine)) {
            ddlSql.append("ENGINE=").append(engine).append(" ");
        }

        if (!StringUtils.isBlank(charset)) {
            ddlSql.append("DEFAULT CHARSET=").append(charset).append(" ");
        }

        if (!StringUtils.isBlank(autoIncrementValue)) {
            ddlSql.append("AUTO_INCREMENT=").append(autoIncrementValue).append(" ");
        }

        if (isSingle) {
            ddlSql.append("single").append(" ");
        } else if (isBroadcast()) {
            ddlSql.append("broadcast").append(" ");
        } else {
            if ((!StringUtils.isBlank(partitionKey)) && (!StringUtils.isBlank(partitionFunction))) {
                ddlSql.append("partition by ").append(partitionFunction).append("(").append(partitionKey)
                    .append(") ");
                if (partitionNum > 0) {
                    ddlSql.append("partitions ").append(partitionNum).append(" ");
                }
            }
        }

        log.info(ddlSql.toString());

        return ddlSql.toString();
    }

    private String columnEntityListToString(List<ColumnEntity> list) {

        StringBuffer listString = new StringBuffer();
        for (Object content : list) {
            if (content instanceof ColumnEntity) {
                listString.append(((ColumnEntity) content)
                    .columnEntityToString(isSingle));
            } else {
                listString.append(content);
            }
            listString.append(",");
        }
        return listString.substring(0, listString.length() - 1);
    }

    public Object clone() {
        PartTableEntity tableEntity = null;
        try {
            tableEntity = (PartTableEntity) super.clone();
            for (ColumnEntity columnEntity : columnInfos) {
                tableEntity.columnInfos.add((ColumnEntity) columnEntity.clone());
            }
        } catch (CloneNotSupportedException e) {
            log.error(e.getMessage(), e);
        }
        return tableEntity;
    }
}

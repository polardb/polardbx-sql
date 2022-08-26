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

public class TableEntity implements Cloneable {

    private static final Log log = LogFactory.getLog(TableEntity.class);

    private String dbName = null;

    private String tbName = null;
    // 列属性
    private List<ColumnEntity> columnInfos = null;

    // 优先读取columnsql来定义表
    private String columnSql = null;

    private String dbpartitionFunction = "hash";
    // 库分区键
    private String dbpartitionKey = null;
    // 库分区数
    private int dbpartitionNum = -1;

    private String tbpartitionFunction = "hash";
    // 表分区键
    private String tbpartitionKey = null;
    // 表分区数
    private int tbpartitionNum = -1;

    // 主键
    private String primaryKey = null;

    private String uniqueKey = null;

    // 如果包含自增的话，指定自增的起始值
    private String autoIncrementValue = null;

    // 是否是广播表
    private boolean isBroadcast;

    private String extpartition = "";

    private String charset = "gbk";
    //    private String auto_shard_key;
    private String engine = "InnoDB";

    public TableEntity() {

    }

    /**
     * 新建一个普通的单表
     */
    public TableEntity(String tbName,
                       List<ColumnEntity> columnInfos) {
        this.tbName = tbName;
        this.columnInfos = columnInfos;
    }

    public TableEntity(String tbName,
                       String columnSql) {
        this.tbName = tbName;
        this.columnSql = columnSql;
    }

    /**
     * 新建一个普通的单库分表, 默认按照hash拆分
     */
    public TableEntity(String tbName, List<ColumnEntity> columnInfos, String dbpartitionKey, int dbpartitionNum,
                       String tbpartitionKey, int tbpartitionNum) {
        this.tbName = tbName;
        this.columnInfos = columnInfos;
        this.dbpartitionKey = dbpartitionKey;
        this.dbpartitionNum = dbpartitionNum;
        this.tbpartitionKey = tbpartitionKey;
        this.tbpartitionNum = tbpartitionNum;
    }

    /**
     * 新建一个普通的广播表
     */
    public TableEntity(String tbName, List<ColumnEntity> columnInfos, boolean isBroadcast) {
        this.tbName = tbName;
        this.columnInfos = columnInfos;
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

    public String getDbpartitionKey() {
        return dbpartitionKey;
    }

    public void setDbpartitionKey(String dbpartitionKey) {
        this.dbpartitionKey = dbpartitionKey;
    }

    public int getDbpartitionNum() {
        return dbpartitionNum;
    }

    public void setDbpartitionNum(int dbpartitionNum) {
        this.dbpartitionNum = dbpartitionNum;
    }

    public String getTbpartitionKey() {
        return tbpartitionKey;
    }

    public void setTbpartitionKey(String tbpartitionKey) {
        this.tbpartitionKey = tbpartitionKey;
    }

    public int getTbpartitionNum() {
        return tbpartitionNum;
    }

    public void setTbpartitionNum(int tbpartitionNum) {
        this.tbpartitionNum = tbpartitionNum;
    }

    public boolean isBroadcast() {
        return isBroadcast;
    }

    public void setBroadcast(boolean isBroadcast) {
        this.isBroadcast = isBroadcast;
    }

    public String getDbpartitionFunction() {
        return dbpartitionFunction;
    }

    public void setDbpartitionFunction(String dbpartitionFunction) {
        this.dbpartitionFunction = dbpartitionFunction;
    }

    public String getTbpartitionFunction() {
        return tbpartitionFunction;
    }

    public void setTbpartitionFunction(String tbpartitionFunction) {
        this.tbpartitionFunction = tbpartitionFunction;
    }

    public String getUniqueKey() {
        return uniqueKey;
    }

    public void setUniqueKey(String uniqueKey) {
        this.uniqueKey = uniqueKey;
    }

    public String getCreateTableDdl() {
        return getCreateTableDdl(false, false);
    }

    public String getExtpartition() {
        return extpartition;
    }

    public void setExtpartition(String extpartition) {
        this.extpartition = extpartition;
    }

    public String getCreateTableDdl(boolean isSingle, boolean ifNotExists) {
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
            columnDefinition = columnEntityListToString(columnInfos, isSingle);
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

        //广播表一定为单表，不会出现dbPartition，即使用户定义了
        if (!isSingle) {
            if (isBroadcast()) {
                ddlSql.append("broadcast").append(" ");
            } else {

                if ((!StringUtils.isBlank(dbpartitionKey)) && (!StringUtils.isBlank(dbpartitionFunction))) {
                    ddlSql.append("dbpartition by ").append(dbpartitionFunction).append("(").append(dbpartitionKey)
                        .append(") ");
                    if (dbpartitionNum > 0) {
                        ddlSql.append("dbpartitions ").append(dbpartitionNum).append(" ");
                    }
                }

                if ((!StringUtils.isBlank(tbpartitionKey)) && (!StringUtils.isBlank(tbpartitionFunction))) {
                    ddlSql.append("tbpartition by ").append(tbpartitionFunction).append("(").append(tbpartitionKey)
                        .append(") ");
                    if (tbpartitionNum > 0) {
                        ddlSql.append("tbpartitions ").append(tbpartitionNum).append(" ");
                    }
                }
            }
            ddlSql.append(extpartition);
        }

        log.info(ddlSql.toString());

        return ddlSql.toString();
    }

    private String columnEntityListToString(List<ColumnEntity> list, boolean isSingle) {

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
        TableEntity tableEntity = null;
        try {
            tableEntity = (TableEntity) super.clone();
            for (ColumnEntity columnEntity : columnInfos) {
                tableEntity.columnInfos.add((ColumnEntity) columnEntity.clone());
            }
        } catch (CloneNotSupportedException e) {
            log.error(e.getMessage(), e);
        }
        return tableEntity;
    }
}
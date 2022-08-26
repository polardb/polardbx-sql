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

/**
 * 列的定义，包括列名类型，列的数据生成规则
 */
public class ColumnEntity implements Cloneable {
    private static Log log = LogFactory.getLog(ColumnEntity.class);
    // 列名
    String name;
    // 列的类型
    String type;
    // 列的限制
    String columnDefinition;

    // 列的生成规则
    String dataRule;

    //是否是自增列
    boolean isAutoIncrement = false;

    //是否是主键
    boolean isPrimaryKey = false;

    //自增的类型，主要是tddl提供了group， simple等autoIncrement类型
    String autoIncrementType = null;

    /**
     * 一个完整的列定义
     */
    public ColumnEntity(String name, String typeName, String columnDefinition,
                        String dataRule, boolean isPrimaryKey, boolean isAutoIncrement, String autoIncrementType) {
        this.name = name;
        this.type = typeName;
        this.columnDefinition = columnDefinition;
        this.dataRule = dataRule;
        this.isAutoIncrement = isAutoIncrement;
        this.isPrimaryKey = isPrimaryKey;
        this.autoIncrementType = autoIncrementType;
    }

    /**
     * 定义一个包含dataRule的普通列
     */
    public ColumnEntity(String name, String typeName, String columnDefinition, String dataRule) {
        this(name, typeName, columnDefinition, dataRule, false, false, null);
    }

    /**
     * 定义一个不包含dataRule的普通列
     */
    public ColumnEntity(String name, String typeName, String defaultValue) {
        this(name, typeName, defaultValue, null);
    }

    /**
     * 一般用于定义主键
     */
    public ColumnEntity(String name, String typeName, String columnDefinition, String dataRule, boolean isPrimaryKey) {
        this(name, typeName, columnDefinition, dataRule, isPrimaryKey, false, null);
    }

    /**
     * 一般用于定义主键
     */
    public ColumnEntity(String name, String typeName, String columnDefinition, boolean isPrimaryKey) {
        this(name, typeName, columnDefinition, null, isPrimaryKey, false, null);
    }

    /**
     * 一般用于定义自增列
     */
    public ColumnEntity(String name, String typeName, String defaultValue,
                        String dataRule, boolean isAutoIncrement, String autoIncrementType) {
        this(name, typeName, defaultValue, dataRule, isAutoIncrement, false, autoIncrementType);
    }

    /**
     * 一般用于定义自增列
     */
    public ColumnEntity(String name, String typeName, String defaultValue, boolean isAutoIncrement,
                        String autoIncrementType) {
        this(name, typeName, defaultValue, null, isAutoIncrement, false, autoIncrementType);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getDataRule() {
        return dataRule;
    }

    public void setDataRule(String dataRule) {
        this.dataRule = dataRule;
    }

    public String getColumnDefinition() {
        return columnDefinition;
    }

    public void setColumnDefinition(String columnDefinition) {
        this.columnDefinition = columnDefinition;
    }

    public boolean isAutoIncrement() {
        return isAutoIncrement;
    }

    public void setAutoIncrement(boolean autoIncrement) {
        isAutoIncrement = autoIncrement;
    }

    public boolean isPrimaryKey() {
        return isPrimaryKey;
    }

    public void setPrimaryKey(boolean primaryKey) {
        isPrimaryKey = primaryKey;
    }

    public String getAutoIncrementType() {
        return autoIncrementType;
    }

    public void setAutoIncrementType(String autoIncrementType) {
        this.autoIncrementType = autoIncrementType;
    }

    public String columnEntityToString(boolean isSingle) {
        StringBuffer columnString = new StringBuffer();
        columnString.append("`" + getName() + "`");
        columnString.append(" ");
        columnString.append(getType());
        columnString.append(" ");
        columnString.append(getColumnDefinition());

        //如果定位为自增列，而且列定义中没有自增的定义，则增加自增定义，否则不变
        if ((isAutoIncrement) && !getColumnDefinition().toUpperCase().contains("AUTO_INCREMENT")) {
            columnString.append(" AUTO_INCREMENT ");
            if ((!StringUtils.isBlank(autoIncrementType) && !isSingle)) {
                columnString.append(" BY ").append(autoIncrementType).append(" ");
            }
        }

        //如果定位为主键，而且列定义中没有主键的定义，则增加主键定义，否则不变
        if ((isPrimaryKey) && !getColumnDefinition().toUpperCase().contains("PRIMARY")) {
            columnString.append(" PRIMARY KEY ");
        }

        return columnString.toString();
    }

    public Object clone() {
        ColumnEntity columnEntity = null;
        try {
            columnEntity = (ColumnEntity) super.clone();
        } catch (CloneNotSupportedException e) {
            log.error(e.getMessage(), e);
        }
        return columnEntity;
    }

}

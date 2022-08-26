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

import com.alibaba.polardbx.qatest.constant.TableConstant;
import com.alibaba.polardbx.qatest.entity.ColumnEntity;
import com.alibaba.polardbx.qatest.entity.TableEntity;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.ReflectUtil;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

/**
 * 测试过程中，一些列需要随机数据，这里定义常用的随机数据
 */
public class ColumnDataGenerator {
    private static Log log = LogFactory.getLog(ColumnDataGenerator.class);

    public final static long pkValue = 1l;
    public final static long pkMaxDataSize = 1000l;

    public final static Object varchar_tesLikeValueOne = "%e%";
    public final static Object varchar_tesLikeValueTwo = "%ee%";
    public final static Object varchar_tesLikeValueThree = "%ee%d%";

    private ReflectUtil reflectUtil;

    public Object varchar_testValue;
    public Object varchar_testValueTwo;

    public Object char_testValue;
    public Object blob_testValue;
    public Object integer_testValue;
    public Object tinyint_testValue;
    public Object tinyint_1bit_testValue;
    public Object smallint_testValue;
    public Object mediumint_testValue;
    public Object bit_testValue;
    public Object bigint_testValue;
    public Object float_testValue;
    public Object double_testValue;
    public Object decimal_testValue;
    public Object date_testValue;
    public Object time_testValue;
    public Object datetime_testValue;
    public Object timestamp_testValue;
    public Object year_testValue;

    public Object timestamp_testStartValue;
    public Object timestamp_testEndValue;

    public Object date_testStartValue;
    public Object date_testEndValue;

    public ColumnDataGenerator() {
        //public ReflectUtil(ColumnDataGenerateRule c, ColumnDataRandomGenerateRule d)
        ColumnDataGenerateRule columnDataGenerateRule = new ColumnDataGenerateRule();
        ColumnDataRandomGenerateRule columnDataRandomGenerateRule = new ColumnDataRandomGenerateRule();
        this.reflectUtil = new ReflectUtil(columnDataGenerateRule, columnDataRandomGenerateRule);
        init();
    }

    private void init() {
        this.varchar_testValue = getValueByColumnName(TableConstant.VARCHAR_TEST_COLUMN);
        this.varchar_testValueTwo = getTwoValueByColumnName(TableConstant.VARCHAR_TEST_COLUMN);

        this.char_testValue = getValueByColumnName(TableConstant.CHAR_TEST_COLUMN);
        this.blob_testValue = getValueByColumnName(TableConstant.BLOB_TEST_COLUMN);
        this.integer_testValue = getValueByColumnName(TableConstant.INTEGER_TEST_COLUMN);
        this.tinyint_testValue = getValueByColumnName(TableConstant.TINYINT_TEST_COLUMN);
        this.tinyint_1bit_testValue = getValueByColumnName(TableConstant.TINYINT_1BIT_TEST_COLUMN);
        this.smallint_testValue = getValueByColumnName(TableConstant.SMALLINT_TEST_COLUMN);
        this.mediumint_testValue = getValueByColumnName(TableConstant.MEDIUMINT_TEST_COLUMN);
        this.bit_testValue = getValueByColumnName(TableConstant.BIT_TEST_COLUMN);
        this.bigint_testValue = getValueByColumnName(TableConstant.BIGINT_TEST_COLUMN);
        this.float_testValue = getValueByColumnName(TableConstant.FLOAT_TEST_COLUMN);
        this.double_testValue = getValueByColumnName(TableConstant.DOUBLE_TEST_COLUMN);
        this.decimal_testValue = getValueByColumnName(TableConstant.DECIMAL_TEST_COLUMN);
        this.date_testValue = getValueByColumnName(TableConstant.DATE_TEST_COLUMN);
        this.time_testValue = getValueByColumnName(TableConstant.TIME_TEST_COLUMN);
        this.datetime_testValue = getValueByColumnName(TableConstant.DATETIME_TEST_COLUMN);
        this.timestamp_testValue = getValueByColumnName(TableConstant.TIMESTAMP_TEST_COLUMN);
        this.year_testValue = getValueByColumnName(TableConstant.YEAR_TEST_COLUMN);

        this.timestamp_testStartValue = reflectUtil.timestampStartValue();
        this.timestamp_testEndValue = reflectUtil.timestampEndValue();

        this.date_testStartValue = reflectUtil.date_testStartValue();
        this.date_testEndValue = reflectUtil.date_testEndValue();
    }

    public void getAllColumnValueExceptPk(List<Object> param) {
        param.add(getValueByColumnName(TableConstant.VARCHAR_TEST_COLUMN));
        param.add(getValueByColumnName(TableConstant.CHAR_TEST_COLUMN));
        param.add(getValueByColumnName(TableConstant.BLOB_TEST_COLUMN));
        param.add(getValueByColumnName(TableConstant.INTEGER_TEST_COLUMN));
        param.add(getValueByColumnName(TableConstant.TINYINT_TEST_COLUMN));
        param.add(getValueByColumnName(TableConstant.TINYINT_1BIT_TEST_COLUMN));

        param.add(getValueByColumnName(TableConstant.SMALLINT_TEST_COLUMN));
        param.add(getValueByColumnName(TableConstant.MEDIUMINT_TEST_COLUMN));
        param.add(getValueByColumnName(TableConstant.BIT_TEST_COLUMN));
        param.add(getValueByColumnName(TableConstant.BIGINT_TEST_COLUMN));
        param.add(getValueByColumnName(TableConstant.FLOAT_TEST_COLUMN));
        param.add(getValueByColumnName(TableConstant.DOUBLE_TEST_COLUMN));

        param.add(getValueByColumnName(TableConstant.DECIMAL_TEST_COLUMN));
        param.add(getValueByColumnName(TableConstant.DATE_TEST_COLUMN));
        param.add(getValueByColumnName(TableConstant.TIME_TEST_COLUMN));
        param.add(getValueByColumnName(TableConstant.DATETIME_TEST_COLUMN));
        param.add(getValueByColumnName(TableConstant.TIMESTAMP_TEST_COLUMN));
        param.add(getValueByColumnName(TableConstant.YEAR_TEST_COLUMN));
    }

    public void getAllColumnValueExceptPkAndVarchar_Test(
        List<Object> param) {
        param.add(getValueByColumnName(TableConstant.CHAR_TEST_COLUMN));
        param.add(getValueByColumnName(TableConstant.BLOB_TEST_COLUMN));
        param.add(getValueByColumnName(TableConstant.INTEGER_TEST_COLUMN));
        param.add(getValueByColumnName(TableConstant.TINYINT_TEST_COLUMN));
        param.add(getValueByColumnName(TableConstant.TINYINT_1BIT_TEST_COLUMN));

        param.add(getValueByColumnName(TableConstant.SMALLINT_TEST_COLUMN));
        param.add(getValueByColumnName(TableConstant.MEDIUMINT_TEST_COLUMN));
        param.add(getValueByColumnName(TableConstant.BIT_TEST_COLUMN));
        param.add(getValueByColumnName(TableConstant.BIGINT_TEST_COLUMN));
        param.add(getValueByColumnName(TableConstant.FLOAT_TEST_COLUMN));
        param.add(getValueByColumnName(TableConstant.DOUBLE_TEST_COLUMN));

        param.add(getValueByColumnName(TableConstant.DECIMAL_TEST_COLUMN));
        param.add(getValueByColumnName(TableConstant.DATE_TEST_COLUMN));
        param.add(getValueByColumnName(TableConstant.TIME_TEST_COLUMN));
        param.add(getValueByColumnName(TableConstant.DATETIME_TEST_COLUMN));
        param.add(getValueByColumnName(TableConstant.TIMESTAMP_TEST_COLUMN));
        param.add(getValueByColumnName(TableConstant.YEAR_TEST_COLUMN));
    }

    public List<Object> getAllColumnValue(List<ColumnEntity> columns,
                                          String pkColumnName, int pkValue) {
        List<Object> cloumnValue = new ArrayList<Object>();
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).getName().equalsIgnoreCase(pkColumnName)) {
                cloumnValue.add(pkValue);
                reflectUtil.setPk(pkValue);
            } else {
                cloumnValue.add(reflectUtil.getResult(columns.get(i)
                    .getDataRule()));
            }
        }
        return cloumnValue;
    }

    public List<Object> getAllColumnValueRD(List<ColumnEntity> columns,
                                            String pkColumnName, int pkValue) {
        List<Object> cloumnValue = new ArrayList<Object>();
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).getName().equalsIgnoreCase(pkColumnName)) {
                cloumnValue.add(pkValue);
                reflectUtil.setPkRD(pkValue);
            } else {
                cloumnValue.add(reflectUtil.getResultRD(columns.get(i)
                    .getDataRule()));
            }
        }
        return cloumnValue;
    }

    public List<Object> getAllColumnValue(List<Object> cloumnValue,
                                          List<ColumnEntity> columns, String pkColumnName, int pkValue) {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).getName().equalsIgnoreCase(pkColumnName)) {
                cloumnValue.add(pkValue);
            } else {
                cloumnValue.add(reflectUtil.getResult(columns.get(i)
                    .getDataRule()));
            }
        }
        return cloumnValue;
    }

    public List<Object> getAllColumnSomeFieldCostantsValue(
        List<ColumnEntity> columns, int pkValue) {
        long base = Long.valueOf(RandomStringUtils.randomNumeric(8));
        List<Object> cloumnValue = new ArrayList<Object>();
        cloumnValue.add(base + pkValue);
        for (int i = 1; i < columns.size(); i++) {
            String column = columns.get(i).getName();
            // 其中两列值固定
            if (column.equals("integer_test")) {
                cloumnValue.add(1);
            } else if (column.equals("varchar_test")) {
                cloumnValue.add("nihao");
            } else {
                cloumnValue.add(reflectUtil.getResult(columns.get(i)
                    .getDataRule()));
            }
        }
        return cloumnValue;
    }

    public String getInsertSqlTemplate(TableEntity tableEntity, boolean exceptAutoIncrement) {
        List<ColumnEntity> columns = tableEntity.getColumnInfos();
        if ((columns == null) || (columns.size() == 0)) {
            return null;
        }
        StringBuilder insertSql = new StringBuilder();
        insertSql.append("insert into ").append(tableEntity.getTbName()).append("(");
        StringBuilder values = new StringBuilder("values (");
        for (int i = 0; i < columns.size(); i++) {
            if (exceptAutoIncrement) {
                if (columns.get(i).isAutoIncrement()) {
                    continue;
                }
            }
            insertSql.append(columns.get(i).getName()).append(", ");
            values.append("?").append(", ");
        }

        insertSql.deleteCharAt(insertSql.length() - 2);
        values.deleteCharAt(values.length() - 2);

        return insertSql.append(") ").append(values).append(")").toString();

    }

    public String getInsertSqlTemplateWithMultiValue(TableEntity tableEntity) {
        List<ColumnEntity> columns = tableEntity.getColumnInfos();
        if ((columns == null) || (columns.size() == 0)) {
            return null;
        }
        StringBuilder insertSql = new StringBuilder();
        insertSql.append("insert ignore into ").append(tableEntity.getTbName()).append("(");
        for (ColumnEntity columnEntity : columns) {
            if (!columnEntity.isAutoIncrement()) {
                insertSql.append(columnEntity.getName()).append(",");
            }
        }
        insertSql.deleteCharAt(insertSql.length() - 1);
        insertSql.append(") values ");
        return insertSql.toString();

    }

    public String getInsertSqlTemplate(TableEntity tableEntity) {
        return getInsertSqlTemplate(tableEntity, false);

    }

    public List<Object> getColumnValue(List<ColumnEntity> columns) {
        return getColumnValue(columns, false);
    }

    public List<Object> getColumnValue(List<ColumnEntity> columns, boolean exceptAutoIncrement) {
        List<Object> cloumnValue = new ArrayList<Object>();
        for (ColumnEntity columnEntity : columns) {
            if (columnEntity.isAutoIncrement() && exceptAutoIncrement) {
                continue;
            }
            cloumnValue.add(reflectUtil.getResult(columnEntity.getDataRule()));
        }
        return cloumnValue;
    }

    /**
     * 向表的列中插入数据
     */
    public void insertDataToTable(TableEntity tableEntity, int count, Connection... conns) {

        insertDataToTable(tableEntity, count, false, conns);
    }

    /**
     * 向表的列中插入数据
     */
    public void insertDataToTable(TableEntity tableEntity, int count, boolean exceptAutoIncrement,
                                  Connection... conns) {
        log.info("表：" + tableEntity.getTbName() + " 开始:" + count + " 数据的插入");

        String insertSql = getInsertSqlTemplate(tableEntity, exceptAutoIncrement);
        log.info("sql: " + insertSql);

        if (insertSql != null) {
            for (int i = 0; i < count; i++) {
                List<Object> cloumnValue =
                    getColumnValue(tableEntity.getColumnInfos(), exceptAutoIncrement);
                for (Connection conn : conns) {
                    JdbcUtil.updateData(conn, insertSql, cloumnValue);
                }
            }
        }
        log.info("数据表" + tableEntity.getTbName() + "数据插入完毕");
        reflectUtil.resetPk();
    }

    /**
     * 通过列名获取到列的随机值,
     */
    public Object getValueByColumnName(String columnName) {
        Object columnValue = null;
        while (columnValue == null) {
            columnValue = reflectUtil.getResult(columnName + "OneValue()");
        }
        return columnValue;
    }

    /**
     * 通过列名获取到列的随机值,
     */
    public Object getTwoValueByColumnName(String columnName) {
        Object columnValue = null;
        while (columnValue == null) {
            columnValue = reflectUtil.getResult(columnName + "TwoValue()");
        }
        return columnValue;
    }

    /**
     * 通过列名获取到列的随机值,
     */
    public Object getValueByColumnNameWithNull(String columnName) {
        Object columnValue = null;
        while (columnValue == null) {
            columnValue = reflectUtil.getResult(columnName + "OneValue(true)");
        }
        return columnValue;
    }

    public void clearTestValues() {
        reflectUtil.clearTestValues();
    }

    public void clearTestValuesRD() {
        reflectUtil.clearTestValuesRD();
    }
}

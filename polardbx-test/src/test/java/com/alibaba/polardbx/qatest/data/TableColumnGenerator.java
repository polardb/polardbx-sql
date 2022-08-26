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

import java.util.ArrayList;
import java.util.List;

/**
 * 获得建表需要的列定义的方法，这里定义tddl测试最常用的列
 * 构建建表语句的时候会用到
 */
public class TableColumnGenerator {

    /**
     * 测试最常用的全类型表, 不设置主键
     */
    public static List<ColumnEntity> getAllTypeColumWithNoPrimayKey() {
        List<ColumnEntity> columns = new ArrayList<ColumnEntity>();
        columns.add(new ColumnEntity("pk", "bigint(11) ", " NOT NULL",
            "getPk()"));
        allTypeColumnExceptPk(columns);
        return columns;
    }

    /**
     * 测试最常用的全类型表, 不设置主键
     */
    public static List<ColumnEntity> getAllTypeXDBColumWithNoPrimayKey() {
        List<ColumnEntity> columns = new ArrayList<ColumnEntity>();
        columns.add(new ColumnEntity("pk", "bigint(11) ", " NOT NULL",
            "getPk()"));
        allTypeXDBColumnExceptPk(columns);
        return columns;
    }

    /**
     * 测试最常用的全类型表, 不设置主键
     */
    public static List<ColumnEntity> getAllTypeColumPkAndIntegerNotNull() {
        List<ColumnEntity> columns = new ArrayList<ColumnEntity>();
        columns.add(new ColumnEntity("pk", "bigint(11) ", " NOT NULL",
            "getPk()"));
        columns.add(new ColumnEntity("integer_test", "int(11) ",
            "NOT NULL", "integer_testRandom()"));

        allTypeColumnExceptPkAndInteger(columns);

        return columns;
    }

    /**
     * 测试最常用的全类型表, 不设置主键
     */
    public static List<ColumnEntity> getAllTypeXDBColumPkAndIntegerNotNull() {
        List<ColumnEntity> columns = new ArrayList<ColumnEntity>();
        columns.add(new ColumnEntity("pk", "bigint(11) ", " NOT NULL",
            "getPk()"));
        columns.add(new ColumnEntity("integer_test", "int(11) ",
            "NOT NULL", "integer_testRandom()"));

        allTypeXDBColumnExceptPkAndInteger(columns);

        return columns;
    }

    /**
     * 测试最常用的全类型表
     */
    public static List<ColumnEntity> getAllTypeColum() {
        List<ColumnEntity> columns = new ArrayList<ColumnEntity>();
        columns.add(new ColumnEntity("pk", "bigint(11) ", " NOT NULL",
            "getPk()", true));
        allTypeColumnExceptPk(columns);
        return columns;
    }

    /**
     * 测试最常用的中文全类型表
     */
    public static List<ColumnEntity> getAllTypeCHNColum() {
        List<ColumnEntity> columns = new ArrayList<ColumnEntity>();
        columns.add(new ColumnEntity("中文_pk_测试", "bigint(11) ", " NOT NULL",
            "getPk()", true));
        allTypeCHNColumnExceptPk(columns);
        return columns;
    }

    /**
     * 测试最常用的全类型表，pk是主键，pk也是自增字段
     *
     * @param autoIncrementType 自增类型，包括Group，Simple 等
     */
    public static List<ColumnEntity> getAllTypeColumAutonic(String autoIncrementType) {
        List<ColumnEntity> columns = new ArrayList<ColumnEntity>();

        columns.add(new ColumnEntity("pk", "bigint(11) ", " NOT NULL",
            "getPk()", true, true, autoIncrementType));

        allTypeColumnExceptPk(columns);
        return columns;
    }

    /**
     * 测试最常用的全类型表，pk是主键，pk也是自增字段
     *
     * @param autoIncrementType 自增类型，包括Group，Simple 等
     */
    public static List<ColumnEntity> getAllTypeXDBColumAutonic(String autoIncrementType) {
        List<ColumnEntity> columns = new ArrayList<ColumnEntity>();

        columns.add(new ColumnEntity("pk", "bigint(11) ", " NOT NULL",
            "getPk()", true, true, autoIncrementType));

        allTypeXDBColumnExceptPk(columns);
        return columns;
    }

    /**
     * 测试最常用的中文全类型表，pk是主键，pk也是自增字段
     *
     * @param autoIncrementType 自增类型，包括Group，Simple 等
     */
    public static List<ColumnEntity> getAllTypeCHNColumAutonic(String autoIncrementType) {
        List<ColumnEntity> columns = new ArrayList<ColumnEntity>();

        columns.add(new ColumnEntity("中文_pk_测试", "bigint(11) ", " NOT NULL",
            "getPk()", true, true, autoIncrementType));

        allTypeCHNColumnExceptPk(columns);
        return columns;
    }

    /**
     * 测试最常用的中文全类型表，pk是主键，pk也是自增字段，用默认的自增类型
     */
    public static List<ColumnEntity> getAllTypeColumAutonic() {

        return getAllTypeColumAutonic("");
    }

    /**
     * 测试最常用的中文全类型表，pk是主键，pk也是自增字段，用默认的自增类型
     */
    public static List<ColumnEntity> getAllTypeXDBColumAutonic() {

        return getAllTypeXDBColumAutonic("");
    }

    /**
     * 测试最常用的全类型表，pk是主键，pk也是自增字段，用默认的自增类型
     */
    public static List<ColumnEntity> getAllTypeCHNColumAutonic() {

        return getAllTypeCHNColumAutonic("");
    }

    /**
     * 测试最常用表的最小类型集合
     */
    public static List<ColumnEntity> getBaseMinColum() {
        List<ColumnEntity> columns = new ArrayList<ColumnEntity>();
        columns.add(new ColumnEntity("pk", "bigint(11) ", " NOT NULL",
            "getPk()", true));
        getBaseMinColumnExcepPk(columns);
        return columns;
    }

    /**
     * 测试最常用表的最小类型集合
     */
    public static List<ColumnEntity> getBaseMinColumAutonic() {
        return getBaseMinColumAutonic("");
    }

    /**
     * 测试最常用表的最小类型集合
     */
    public static List<ColumnEntity> getBaseMinColumAutonic(String autoIncrementType) {
        List<ColumnEntity> columns = new ArrayList<ColumnEntity>();

        columns.add(new ColumnEntity("pk", "bigint(11)  NOT NULL ", " NOT NULL",
            "getPk()", true, true, autoIncrementType));
        getBaseMinColumnExcepPk(columns);
        return columns;
    }

    private static void getBaseMinColumnExcepPk(List<ColumnEntity> columns) {
        columns.add(new ColumnEntity("integer_test", "int(11) ",
            "DEFAULT NULL", "integer_testRandom()"));
        columns.add(new ColumnEntity("date_test", "date ", "DEFAULT NULL",
            "date_testRandom()"));
        columns.add(new ColumnEntity("timestamp_test", "timestamp ",
            " NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP",
            "timestamp_testRandom()"));
        columns.add(new ColumnEntity("datetime_test", "datetime ",
            "DEFAULT NULL", "datetime_testRandom()"));
        columns.add(new ColumnEntity("varchar_test", "varchar(255) ",
            "DEFAULT NULL", "varchar_testRandom()"));
        columns.add(new ColumnEntity("float_test", "float ", "DEFAULT NULL",
            "float_testRandom()"));
    }

    /**
     * 包含地理字段的表
     */
    public static List<ColumnEntity> getGemoColum() {
        List<ColumnEntity> columns = new ArrayList<ColumnEntity>();

        columns.add(new ColumnEntity("pk", "int(11)  ",
            " NOT NULL AUTO_INCREMENT", "", true));

        columns.add(new ColumnEntity("geom", "geometry ",
            "DEFAULT NULL", ""));
        columns.add(new ColumnEntity("pot", "point ",
            "DEFAULT NULL", "varchar_testRandom()"));
        columns.add(new ColumnEntity("line", "linestring ",
            "DEFAULT NULL", ""));
        columns.add(new ColumnEntity("poly", "polygon ",
            "DEFAULT NULL", ""));
        columns.add(new ColumnEntity("multipot", "multipoint ",
            "DEFAULT NULL", ""));
        columns.add(new ColumnEntity("multiline", "multilinestring ",
            "DEFAULT NULL", ""));
        columns.add(new ColumnEntity("multipoly", "multipolygon ",
            "DEFAULT NULL", ""));
        columns.add(new ColumnEntity("geomlist", "geometrycollection ",
            "DEFAULT NULL", ""));
        return columns;
    }

    /**
     * 测试大小写敏感的特殊表
     */
    public static List<ColumnEntity> getSencitiveFileColum() {
        List<ColumnEntity> columns = new ArrayList<ColumnEntity>();
        columns.add(new ColumnEntity("id", "int(11)  ",
            " NOT NULL ", ""));
        columns.add(new ColumnEntity("name", "varchar(10) ",
            "CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL", ""));
        columns.add(new ColumnEntity("school", "varchar(10)",
            "CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL", ""));
        return columns;
    }

    /**
     * 测试大小写敏感的特殊表
     */
    public static List<ColumnEntity> getSencitiveFieldGbkColum() {
        List<ColumnEntity> columns = new ArrayList<ColumnEntity>();
        columns.add(new ColumnEntity("id", "int(11)  ",
            " NOT NULL ", ""));
        columns.add(new ColumnEntity("name", "varchar(10) ",
            " DEFAULT NULL", ""));
        columns.add(new ColumnEntity("school", "varchar(10)",
            " DEFAULT NULL", ""));
        return columns;
    }

    private static void allTypeColumnExceptPk(List<ColumnEntity> columns) {
        columns.add(new ColumnEntity("integer_test", "int(11) ",
            "DEFAULT NULL", "integer_testRandom()"));

        allTypeColumnExceptPkAndInteger(columns);

    }

    private static void allTypeColumnExceptPkAndInteger(List<ColumnEntity> columns) {
        columns.add(new ColumnEntity("varchar_test", "varchar(255) ",
            "DEFAULT NULL", "varchar_testRandom(true)"));

        columns.add(new ColumnEntity("char_test", "char(255) ", "DEFAULT NULL",
            "char_testRandom()"));
        columns.add(new ColumnEntity("blob_test", "blob", "DEFAULT NULL",
            "blob_testRandom()"));
        columns.add(new ColumnEntity("tinyint_test", "tinyint(4)",
            "DEFAULT NULL", "tinyint_testRandom()"));
        columns.add(new ColumnEntity("tinyint_1bit_test", "tinyint(1) ",
            "DEFAULT NULL", "tinyint_1bit_testRandom()"));
        columns.add(new ColumnEntity("smallint_test", "smallint(6) ",
            "DEFAULT NULL", "smallint_testRandom()"));
        columns.add(new ColumnEntity("mediumint_test", "mediumint(9) ",
            "DEFAULT NULL", "mediumint_testRandom()"));

        columns.add(new ColumnEntity("bit_test", "bit(1) ", "DEFAULT NULL",
            "bit_testRandom()"));

        columns.add(new ColumnEntity("bigint_test", "bigint(20) ",
            "DEFAULT NULL", "bigint_testRandom()"));
        columns.add(new ColumnEntity("float_test", "float ", "DEFAULT NULL",
            "float_testRandom()"));
        columns.add(new ColumnEntity("double_test", "double ", "DEFAULT NULL",
            "double_testRandom()"));
        columns.add(new ColumnEntity("decimal_test", "decimal(10,0) ",
            "DEFAULT NULL", "decimal_testRandom()"));

        columns.add(new ColumnEntity("date_test", "date ", "DEFAULT NULL",
            "date_testRandom()"));
        columns.add(new ColumnEntity("time_test", "time ", "DEFAULT NULL",
            "time_testRandom()"));
        columns.add(new ColumnEntity("datetime_test", "datetime ",
            "DEFAULT NULL", "datetime_testRandom()"));
        columns.add(new ColumnEntity("timestamp_test", "timestamp ",
            " NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP",
            "timestamp_testRandom()"));
        columns.add(new ColumnEntity("year_test", "year(4)", "DEFAULT NULL",
            "year_testRandom()"));
        columns.add(new ColumnEntity("mediumtext_test", "MEDIUMTEXT", "DEFAULT NULL", "text_testRandom()"));

    }

    private static void allTypeCHNColumnExceptPk(List<ColumnEntity> columns) {
        columns.add(new ColumnEntity("中文_varchar_测试", "varchar(255) ",
            "DEFAULT NULL", "varchar_testRandom(true)"));

        columns.add(new ColumnEntity("中文_integer_测试", "int(11) ",
            "DEFAULT NULL", "integer_testRandom()"));

        columns.add(new ColumnEntity("中文_char_测试", "char(255) ", "DEFAULT NULL",
            "char_testRandom()"));
        columns.add(new ColumnEntity("中文_blob_测试", "blob", "DEFAULT NULL",
            "blob_testRandom()"));
        columns.add(new ColumnEntity("中文_tinyint_测试", "tinyint(4)",
            "DEFAULT NULL", "tinyint_testRandom()"));
        columns.add(new ColumnEntity("中文_tinyint_1bit_测试", "tinyint(1) ",
            "DEFAULT NULL", "tinyint_1bit_testRandom()"));
        columns.add(new ColumnEntity("中文_smallint_测试", "smallint(6) ",
            "DEFAULT NULL", "smallint_testRandom()"));
        columns.add(new ColumnEntity("中文_mediumint_测试", "mediumint(9) ",
            "DEFAULT NULL", "mediumint_testRandom()"));

        columns.add(new ColumnEntity("中文_bit_测试", "bit(1) ", "DEFAULT NULL",
            "bit_testRandom()"));

        columns.add(new ColumnEntity("中文_bigint_测试", "bigint(20) ",
            "DEFAULT NULL", "bigint_testRandom()"));
        columns.add(new ColumnEntity("中文_float_测试", "float ", "DEFAULT NULL",
            "float_testRandom()"));
        columns.add(new ColumnEntity("中文_double_测试", "double ", "DEFAULT NULL",
            "double_testRandom()"));
        columns.add(new ColumnEntity("中文_decimal_测试", "decimal(10,0) ",
            "DEFAULT NULL", "decimal_testRandom()"));
        columns.add(new ColumnEntity("中文_date_测试", "date ", "DEFAULT NULL",
            "date_testRandom()"));
        columns.add(new ColumnEntity("中文_time_测试", "time ", "DEFAULT NULL",
            "time_testRandom()"));
        columns.add(new ColumnEntity("中文_datetime_测试", "datetime ",
            "DEFAULT NULL", "datetime_testRandom()"));
        columns.add(new ColumnEntity("中文_timestamp_测试", "timestamp ",
            " NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP",
            "timestamp_testRandom()"));
        columns.add(new ColumnEntity("中文_year_测试", "year(4)", "DEFAULT NULL",
            "year_testRandom()"));

    }

    /**
     * TinyIntTest类中的表类型
     */
    public static List<ColumnEntity> getTinyintColum() {
        List<ColumnEntity> columns = new ArrayList<ColumnEntity>();
        columns.add(new ColumnEntity("tinyintr", "tinyint(4)  ", "NOT NULL",
            ""));
        columns.add(new ColumnEntity("tinyintr_1", "tinyint(1) ",
            "DEFAULT NULL", ""));
        columns.add(new ColumnEntity("tinyintr_3", "tinyint(3) ",
            "DEFAULT NULL", ""));
        return columns;
    }

    public static List<ColumnEntity> 客户表Colum() {
        List<ColumnEntity> columns = new ArrayList<ColumnEntity>();
        columns.add(new ColumnEntity("编号", "int(11)  ", "NOT NULL  ",
            "getPk()"));
        columns.add(new ColumnEntity("姓名", "varchar(255) ",
            "DEFAULT NULL", "varchar_testRandom()"));
        columns.add(new ColumnEntity("性别", "varchar(255) ",
            "DEFAULT NULL", "varchar_testRandom()"));
        columns.add(new ColumnEntity("单号", "int(11) ",
            "DEFAULT NULL", "integer_testRandom()"));
        columns.add(new ColumnEntity("订单时间", "date ", "DEFAULT NULL",
            "date_testRandom()"));
        return columns;
    }

    public static List<ColumnEntity> 客户表AutoIncrementColum() {
        List<ColumnEntity> columns = new ArrayList<ColumnEntity>();

        columns.add(new ColumnEntity("编号", "int(11)  ", "NOT NULL  AUTO_INCREMENT",
            "getPk()", true));

        columns.add(new ColumnEntity("姓名", "varchar(255) ",
            "DEFAULT NULL", "varchar_testRandom()"));
        columns.add(new ColumnEntity("性别", "varchar(255) ",
            "DEFAULT NULL", "varchar_testRandom()"));
        columns.add(new ColumnEntity("单号", "int(11) ",
            "DEFAULT NULL", "integer_testRandom()"));
        columns.add(new ColumnEntity("订单时间", "date ", "DEFAULT NULL",
            "date_testRandom()"));
        return columns;
    }

    public static List<ColumnEntity> 商品表Colum() {
        List<ColumnEntity> columns = new ArrayList<ColumnEntity>();
        columns.add(new ColumnEntity("商品编号", "int(11)  ", "NOT NULL ",
            ""));
        columns.add(new ColumnEntity("商品所属订单", "int(11) ",
            "DEFAULT NULL", ""));
        columns.add(new ColumnEntity("商品价格", "decimal(5,2) ",
            "DEFAULT NULL", ""));
        return columns;
    }

    /**
     * TruncatedValueTest 测试， 与列类型有关系，暂时不改表结构
     */
    public static List<ColumnEntity> getBigintMemIssueSdmtColum() {
        List<ColumnEntity> columns = new ArrayList<ColumnEntity>();
        columns.add(new ColumnEntity("id", " bigint(20)  ", "NOT NULL ",
            ""));
        columns.add(new ColumnEntity("tinyint_id", "tinyint(3) ",
            "DEFAULT NULL", ""));
        columns.add(new ColumnEntity("utinyint_id", "tinyint(3) ",
            "unsigned DEFAULT NULL", ""));
        columns.add(new ColumnEntity("smallint_id", "smallint(5) ",
            "DEFAULT NULL", ""));
        columns.add(new ColumnEntity("usmallint_id", "smallint(5) ",
            " unsigned  DEFAULT NULL", ""));
        columns.add(new ColumnEntity("mediumint_id", "mediumint(8) ",
            "DEFAULT NULL", ""));
        columns.add(new ColumnEntity("umediumint_id", " mediumint(8) ",
            "unsigned DEFAULT NULL", ""));
        columns.add(new ColumnEntity("int_id", "int(11)  ",
            "DEFAULT NULL", ""));
        columns.add(new ColumnEntity("uint_id", "int(11)  ",
            "unsigned DEFAULT NULL", ""));
        columns.add(new ColumnEntity("bigint_id", "bigint(20)  ",
            "DEFAULT NULL", ""));
        columns.add(new ColumnEntity("ubigint_id", "bigint(20) ",
            "unsigned DEFAULT NULL", ""));
        return columns;
    }

    public static List<ColumnEntity> 订单表Colum() {
        List<ColumnEntity> columns = new ArrayList<ColumnEntity>();
        columns.add(new ColumnEntity("单号", "int(11)  ", " NOT NULL ",
            "getPk()"));
        columns.add(new ColumnEntity("起点", "varchar(255) ",
            "DEFAULT NULL", "varchar_testRandom()"));
        columns.add(new ColumnEntity("终点", "varchar(255) ",
            "DEFAULT NULL", "varchar_testRandom()"));
        columns.add(new ColumnEntity("时间", "date ", "DEFAULT NULL",
            "date_testRandom()"));
        columns.add(new ColumnEntity("费用", "decimal(5,2) ", "DEFAULT NULL",
            "integer_testRandom()"));
        return columns;
    }

    public static List<ColumnEntity> getTableColum() {
        List<ColumnEntity> columns = new ArrayList<ColumnEntity>();
        columns.add(new ColumnEntity("create", "int(11)  ", " DEFAULT NULL "));
        columns.add(new ColumnEntity("table", "int(11)  ", " DEFAULT NULL "));
        columns.add(new ColumnEntity("database", "int(11)  ", " DEFAULT NULL "));
        columns.add(new ColumnEntity("by", "int(11)  ", " DEFAULT NULL "));
        columns.add(new ColumnEntity("desc", "int(11)  ", "  NOT NULL DEFAULT '0' "));
        columns.add(new ColumnEntity("int", "int(11)  ", " DEFAULT NULL "));
        columns.add(new ColumnEntity("group", "int(11)  ", " DEFAULT NULL "));
        columns.add(new ColumnEntity("order", "int(11)  ", " DEFAULT NULL "));
        columns.add(new ColumnEntity("primary", "int(11)  ", " DEFAULT NULL "));
        columns.add(new ColumnEntity("key", "int(11)  ", " DEFAULT NULL "));
        return columns;
    }

    public static List<ColumnEntity> get_tddl_TableColum() {
        List<ColumnEntity> columns = new ArrayList<ColumnEntity>();
        columns.add(new ColumnEntity("id", "int(11)  ", "   NOT NULL "));
        columns.add(new ColumnEntity("name", "varchar(255) ", "  NOT NULL"));
        return columns;
    }

    /**
     * 测试最常用表的最小类型集合
     */
    public static List<ColumnEntity> getLoadDataColumn() {
        List<ColumnEntity> columns = new ArrayList<ColumnEntity>();
        columns.add(new ColumnEntity("pk", "bigint(11) ", " NOT NULL",
            "getPk()", true));
        columns.add(new ColumnEntity("integer_test", "int(11) ",
            "DEFAULT NULL", "integer_testRandom()"));
        columns.add(new ColumnEntity("date_test", "date ", "DEFAULT NULL",
            "date_testRandom()"));
        columns.add(new ColumnEntity("timestamp_test", "timestamp ",
            " NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP",
            "timestamp_testRandom()"));
        columns.add(new ColumnEntity("datetime_test", "datetime ",
            "DEFAULT NULL", "datetime_testRandom()"));
        columns.add(new ColumnEntity("varchar_test", "varchar(255) ",
            "DEFAULT NULL", "varchar_testRandom()"));
        columns.add(new ColumnEntity("float_test", "float ", "DEFAULT NULL",
            "float_testRandom()"));
        return columns;
    }

    public static List<ColumnEntity> allTDDLMTypeColumn() {
        List<ColumnEntity> columns = new ArrayList<ColumnEntity>();
        columns.add(new ColumnEntity("pk", "bigint(11) ", " NOT NULL",
            "getPk()", true));
        columns.add(new ColumnEntity("tinyint_test", "tinyint",
            "DEFAULT NULL", "boolean()"));
        columns.add(new ColumnEntity("smallint_test", "smallint ",
            "DEFAULT NULL", "smallint_testRandom()"));
        columns.add(new ColumnEntity("mediumint_test", "mediumint ",
            "DEFAULT NULL", "mediumint_testRandom()"));
        columns.add(new ColumnEntity("bigint_test", "BIGINT",
            "DEFAULT NULL"));
        columns.add(new ColumnEntity("int_test", "INT",
            "DEFAULT NULL"));

        columns.add(new ColumnEntity("bit_test", "bit ", "DEFAULT NULL",
            "bit_testRandom()"));

        columns.add(new ColumnEntity("boolean_test", "tinyint(1)",
            "DEFAULT NULL"));

        columns.add(new ColumnEntity("tinyint_test_p1", "tinyint(1)",
            "DEFAULT NULL"));
        columns.add(new ColumnEntity("smallint_test_p2", "SMALLINT(2)",
            "DEFAULT NULL"));
        columns.add(new ColumnEntity("mediumint_test_p3", "MEDIUMINT(3)",
            "DEFAULT NULL"));
        columns.add(new ColumnEntity("int_test_p4", "INT(4)",
            "DEFAULT NULL"));

        columns.add(new ColumnEntity("bit_test_p6", "BIT(6)", "DEFAULT NULL"));

        columns.add(new ColumnEntity("tinyint_unsigned_test", "tinyint unsigned", "DEFAULT NULL"));
        columns.add(new ColumnEntity("smallint_unsigned_test", "smallint unsigned", "DEFAULT NULL"));
        columns.add(new ColumnEntity("mediumint_unsigned_test", "mediumint unsigned", "DEFAULT NULL"));
        columns.add(new ColumnEntity("int_unsigned_test", "int unsigned", "DEFAULT NULL"));
        columns.add(new ColumnEntity("bigint_unsigned_test", "bigint unsigned", "DEFAULT NULL"));

        columns.add(new ColumnEntity("tinyint_unsigned_test_p1", "tinyint(1) unsigned", "DEFAULT NULL"));
        columns.add(new ColumnEntity("smallint_unsigned_test_p2", "smallint(2) unsigned", "DEFAULT NULL"));
        columns.add(new ColumnEntity("mediumint_unsigned_test_p3", "mediumint(3) unsigned", "DEFAULT NULL"));
        columns.add(new ColumnEntity("int_unsigned_test_p4", "int(4) unsigned", "DEFAULT NULL"));
        columns.add(new ColumnEntity("bigint_unsigned_test_p5", "bigint(5) unsigned", "DEFAULT NULL"));

        columns.add(new ColumnEntity("tinyint_zero_test", "tinyint zerofill", "DEFAULT NULL"));
        columns.add(new ColumnEntity("smallint_zero_test", "smallint zerofill", "DEFAULT NULL"));
        columns.add(new ColumnEntity("mediumint_zero_test", "mediumint zerofill", "DEFAULT NULL"));
        columns.add(new ColumnEntity("int_zero_test", "int zerofill", "DEFAULT NULL"));
        columns.add(new ColumnEntity("bigint_zero_test", "bigint zerofill", "DEFAULT NULL"));

        columns.add(new ColumnEntity("tinyint_unsigned_zero_test", "tinyint unsigned zerofill", "DEFAULT NULL"));
        columns.add(new ColumnEntity("smallint_unsigned_zero_test", "smallint unsigned zerofill", "DEFAULT NULL"));
        columns.add(new ColumnEntity("mediumint_unsigned_zero_test", "mediumint unsigned zerofill", "DEFAULT NULL"));
        columns.add(new ColumnEntity("int_unsigned_zero_test", "int unsigned zerofill", "DEFAULT NULL"));
        columns.add(new ColumnEntity("bigint_unsigned_zero_test", "bigint unsigned zerofill", "DEFAULT NULL"));

        columns.add(new ColumnEntity("tinyint_unsigned_zero_test_p1", "tinyint(1) unsigned zerofill", "DEFAULT NULL"));
        columns
            .add(new ColumnEntity("smallint_unsigned_zero_test_p2", "smallint(2) unsigned zerofill", "DEFAULT NULL"));
        columns
            .add(new ColumnEntity("mediumint_unsigned_zero_test_p3", "mediumint(3) unsigned zerofill", "DEFAULT NULL"));
        columns.add(new ColumnEntity("int_unsigned_zero_test_p4", "int(4) unsigned zerofill", "DEFAULT NULL"));
        columns.add(new ColumnEntity("bigint_unsigned_zero_test_p5", "bigint(5) unsigned zerofill", "DEFAULT NULL"));

        columns.add(new ColumnEntity("float_test", "float ", "DEFAULT NULL"));
        columns.add(new ColumnEntity("double_test", "double ", "DEFAULT NULL"));
        columns.add(new ColumnEntity("decimal_test", "DECIMAL ", "DEFAULT NULL"));
        columns.add(new ColumnEntity("real_test", "REAL ", "DEFAULT NULL"));

        columns.add(new ColumnEntity("float_test_p7p3", "FLOAT(7,3) ", "DEFAULT NULL"));
        columns.add(new ColumnEntity("double_test_p7p3", " DOUBLE(7,3) ", "DEFAULT NULL"));
        columns.add(new ColumnEntity("decimal_test_p7p3", "DECIMAL(7,3) ", "DEFAULT NULL"));
        columns.add(new ColumnEntity("real_test_p7p3", "REAL(7,3) ", "DEFAULT NULL"));

        columns.add(new ColumnEntity("float_unsigned_test", "float UNSIGNED", "DEFAULT NULL"));
        columns.add(new ColumnEntity("double_unsigned_test", "double UNSIGNED", "DEFAULT NULL"));
        columns.add(new ColumnEntity("decimal_unsigned_test", "DECIMAL UNSIGNED", "DEFAULT NULL"));
        columns.add(new ColumnEntity("real_unsigned_test", "REAL UNSIGNED", "DEFAULT NULL"));

        columns.add(new ColumnEntity("float_unsigned_test_p7p3", "FLOAT(7,3) UNSIGNED", "DEFAULT NULL"));
        columns.add(new ColumnEntity("double_unsigned_test_p7p3", "DOUBLE(7,3) UNSIGNED", "DEFAULT NULL"));
        columns.add(new ColumnEntity("decimal_unsigned_test_p7p3", "DECIMAL(7,3) UNSIGNED ", "DEFAULT NULL"));
        columns.add(new ColumnEntity("real_unsigned_test_p7p3", "REAL(7,3) UNSIGNED ", "DEFAULT NULL"));

        columns.add(new ColumnEntity("float_zero_test", "FLOAT ZEROFILL", "DEFAULT NULL"));
        columns.add(new ColumnEntity("double_zero_test", "DOUBLE ZEROFILL", "DEFAULT NULL"));
        columns.add(new ColumnEntity("decimal_zero_test", "DECIMAL ZEROFILL ", "DEFAULT NULL"));
        columns.add(new ColumnEntity("real_zero_test", "REAL ZEROFILL", "DEFAULT NULL"));

        columns.add(new ColumnEntity("float_zero_test_p7p3", "FLOAT(7,3) ZEROFILL", "DEFAULT NULL"));
        columns.add(new ColumnEntity("double_zero_test_p7p3", "DOUBLE(7,3) ZEROFILL", "DEFAULT NULL"));
        columns.add(new ColumnEntity("decimal_zero_test_p7p3", "DECIMAL(7,3) ZEROFILL ", "DEFAULT NULL"));
        columns.add(new ColumnEntity("real_zero_test_p7p3", "REAL(7,3) ZEROFILL ", "DEFAULT NULL"));

        columns.add(new ColumnEntity("float_unsigned_zero_test", "FLOAT UNSIGNED ZEROFILL", "DEFAULT NULL"));
        columns.add(new ColumnEntity("double_unsigned_zero_test", "DOUBLE UNSIGNED ZEROFILL", "DEFAULT NULL"));
        columns.add(new ColumnEntity("decimal_unsigned_zero_test", "DECIMAL UNSIGNED ZEROFILL ", "DEFAULT NULL"));
        columns.add(new ColumnEntity("real_unsigned_zero_test", "REAL UNSIGNED ZEROFILL", "DEFAULT NULL"));

        columns.add(new ColumnEntity("float_unsigned_zero_test_p7p3", "FLOAT(7,3) UNSIGNED ZEROFILL", "DEFAULT NULL"));
        columns
            .add(new ColumnEntity("double_unsigned_zero_test_p7p3", "DOUBLE(7,3) UNSIGNED ZEROFILL", "DEFAULT NULL"));
        columns.add(
            new ColumnEntity("decimal_unsigned_zero_test_p7p3", "DECIMAL(7,3) UNSIGNED ZEROFILL ", "DEFAULT NULL"));
        columns.add(new ColumnEntity("real_unsigned_zero_test_p7p3", "REAL(7,3) UNSIGNED ZEROFILL", "DEFAULT NULL"));

        columns.add(new ColumnEntity("char_test_p255", "CHAR(255)", "DEFAULT NULL"));
        columns.add(new ColumnEntity("varchar_test_p255", "VARCHAR(255)", "DEFAULT NULL"));

        columns.add(new ColumnEntity("binary_test_p255", "BINARY(255)", "DEFAULT NULL"));
        columns.add(new ColumnEntity("varbinary_test_p255", "VARBINARY(255)", "DEFAULT NULL"));

        columns.add(new ColumnEntity("blob_test_p255", "BLOB(255)", "DEFAULT NULL"));

        columns.add(new ColumnEntity("tinytext_test", "TINYTEXT", "DEFAULT NULL"));
        columns.add(new ColumnEntity("mediumtext_test", "MEDIUMTEXT", "DEFAULT NULL"));
        columns.add(new ColumnEntity("text_test", "TEXT", "DEFAULT NULL"));
        columns.add(new ColumnEntity("longtext_test", "LONGTEXT", "DEFAULT NULL"));

        columns.add(new ColumnEntity("tinyblob_test", "TINYBLOB", "DEFAULT NULL"));
        columns.add(new ColumnEntity("mediumblob_test", "MEDIUMBLOB", "DEFAULT NULL"));
        columns.add(new ColumnEntity("blob_test", "BLOB", "DEFAULT NULL"));
        columns.add(new ColumnEntity("longblob_test", "LONGBLOB", "DEFAULT NULL"));

        columns
            .add(new ColumnEntity("enum_test", "enum('x-small','small','medium','large','x-large')", "DEFAULT NULL"));
        columns.add(new ColumnEntity("set_test", "set('a','b','c','d')", "DEFAULT NULL"));

        columns.add(new ColumnEntity("date_test", "DATE", "DEFAULT NULL"));
        columns.add(new ColumnEntity("time_test", "TIME", "DEFAULT NULL"));
        columns.add(new ColumnEntity("year_test", "YEAR", "DEFAULT NULL"));
        columns.add(new ColumnEntity("datetime_test", "DATETIME", "DEFAULT NULL"));
        columns.add(new ColumnEntity("timestamp_test", "TIMESTAMP NULL ON UPDATE CURRENT_TIMESTAMP", "DEFAULT NULL"));

        columns.add(new ColumnEntity("year_test_p4", "YEAR(4)", "DEFAULT NULL"));
        columns.add(new ColumnEntity("time_test_p4", "TIME(4)", "DEFAULT NULL"));
        columns.add(new ColumnEntity("datetime_test_p4", "DATETIME(4)", "DEFAULT NULL"));

        return columns;
    }

    /**
     * 测试最常用的全类型表
     */
    public static List<ColumnEntity> getAllTypeXDBColum() {
        List<ColumnEntity> columns = new ArrayList<ColumnEntity>();
        columns.add(new ColumnEntity("pk", "bigint(11) ", " NOT NULL",
            "getPk()", true));
        allTypeXDBColumnExceptPk(columns);
        return columns;
    }

    private static void allTypeXDBColumnExceptPk(List<ColumnEntity> columns) {
        columns.add(new ColumnEntity("integer_test", "int(11) ",
            "DEFAULT NULL", "integer_testRandom()"));

        allTypeXDBColumnExceptPkAndInteger(columns);

    }

    private static void allTypeXDBColumnExceptPkAndInteger(List<ColumnEntity> columns) {
        columns.add(new ColumnEntity("varchar_test", "varchar(255) ",
            "DEFAULT NULL", "varchar_testRandom(true)"));

        columns.add(new ColumnEntity("char_test", "char(255) ", "DEFAULT NULL",
            "char_testRandom()"));

        columns.add(new ColumnEntity("tinyint_1bit_test", "tinyint(1) ",
            "DEFAULT NULL", "tinyint_1bit_testRandom()"));
        columns.add(new ColumnEntity("smallint_test", "smallint(6) ",
            "DEFAULT NULL", "smallint_testRandom()"));

        columns.add(new ColumnEntity("bigint_test", "bigint(20) ",
            "DEFAULT NULL", "bigint_testRandom()"));
        columns.add(new ColumnEntity("float_test", "float ", "DEFAULT NULL",
            "float_testRandom()"));
        columns.add(new ColumnEntity("double_test", "double ", "DEFAULT NULL",
            "double_testRandom()"));
        columns.add(new ColumnEntity("decimal_test", "decimal(10,0) ",
            "DEFAULT NULL", "decimal_testRandom()"));

        columns.add(new ColumnEntity("date_test", "date ", "DEFAULT NULL",
            "date_testRandom()"));
        columns.add(new ColumnEntity("time_test", "time ", "DEFAULT NULL",
            "time_testRandom()"));
        columns.add(new ColumnEntity("datetime_test", "datetime ",
            "DEFAULT NULL", "datetime_testRandom()"));
        columns.add(new ColumnEntity("timestamp_test", "timestamp ",
            "",
            "timestamp_testRandom()"));

        columns.add(new ColumnEntity("mediumtext_test", "MEDIUMTEXT", "", "text_testRandom()"));

    }
}

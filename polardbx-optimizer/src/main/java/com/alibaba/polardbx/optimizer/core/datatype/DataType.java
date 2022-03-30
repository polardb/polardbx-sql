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

package com.alibaba.polardbx.optimizer.core.datatype;

import com.google.common.collect.ImmutableList;
import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.charset.CollationName;
import com.alibaba.polardbx.common.type.MySQLStandardFieldType;
import com.alibaba.polardbx.optimizer.core.row.Row;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Comparator;

import static com.alibaba.polardbx.common.type.MySQLStandardFieldType.*;

/**
 * http://dev.mysql.com/doc/refman/5.6/en/type-conversion.html
 *
 * <pre>
 * 类型信息主要针对两类
 * a. 字段  【数据库结构里可以获取】
 * b. 函数
 *    1. Dummy函数 (下推) 【返回null，由应用基于返回值调用{@linkplain DataTypeUtil}进行动态获取】
 *    2. tddl自实现 【以函数的第一个字段的类型为准，嵌套函数时递归获取第一个字段】
 *
 * 针对特殊的函数，比如select 1+1, 没有字段信息，默认以LongType处理，
 * ps. mysql中string的加法也是按照数字来处理, 比如 select "a" + "b" = 2 ,  select "1" + "a" = 1
 * </pre>
 */
public interface DataType<DATA> extends Comparator<Object> {

    // 自定义一个year sqlType
    int YEAR_SQL_TYPE = 10001;
    int MEDIUMINT_SQL_TYPE = 10002;
    int DATETIME_SQL_TYPE = 10003;
    int UNDECIDED_SQL_TYPE = 10099; // 未决类型
    int JSON_SQL_TYPE = 245;

    interface ResultGetter {

        Object get(ResultSet rs, int index) throws SQLException;

        Object get(Row rs, int index);
    }

    ResultGetter getResultGetter();

    /**
     * 对应数据类型的最大值
     */
    DATA getMaxValue();

    /**
     * 对应数据类型的最小值
     */
    DATA getMinValue();

    /**
     * 将数据转化为当前DataType类型
     */
    DATA convertFrom(Object value);

    /**
     * 数据类型对应的class
     */
    Class getDataClass();

    /**
     * Convert to objects of Java type
     * It's suitable for non-executor scene, like Partitioner.
     */
    Object convertJavaFrom(Object value);

    /**
     * Get the Java (JDBC) type
     */
    Class getJavaClass();

    /**
     * 数据计算器
     */
    Calculator getCalculator();

    /**
     * 获取类型对应的jdbcType
     */
    int getSqlType();

    String getStringSqlType();

    /**
     * 是否为unsigned类型
     */
    boolean isUnsigned();

    int getPrecision();

    int getScale();

    default CharsetName getCharsetName() {
        return CharsetName.defaultCharset();
    }

    default CollationName getCollationName() {
        return CollationName.defaultCollation();
    }

    default boolean isUtf8Encoding() {
        return true;
    }

    default boolean isLatin1Encoding() {
        return false;
    }

    default int length() {
        return 0;
    }

    /**
     * Get the standard mysql field type name.
     * @return represented field type.
     */
    MySQLStandardFieldType fieldType();

    boolean equalDeeply(DataType that);
}

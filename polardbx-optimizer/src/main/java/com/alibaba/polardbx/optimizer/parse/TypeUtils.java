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

package com.alibaba.polardbx.optimizer.parse;

import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Types;
import java.util.*;

/**
 * 函数匹配逻辑
 *
 * @author hongxi.chx
 * @create 2017-01-03 14:12
 */
public class TypeUtils {

    private static Map<Class<?>, Integer>   javaJdbcTypes;  // Name to value
    private static Map<Integer, Class<?>> jdbcJavaTypes;  // jdbc type to java type

    private static Class mysqlDefs;
    private static Method method;
    static {
        try {
            mysqlDefs = Class.forName("com.mysql.jdbc.MysqlDefs");
            method = mysqlDefs.getDeclaredMethod("mysqlToJavaType", String.class);
            method.setAccessible(true);
        } catch (ClassNotFoundException | NoSuchMethodException e) {
            e.printStackTrace();
        }

        javaJdbcTypes = new HashMap<>();
        jdbcJavaTypes = new HashMap<>();
        // 初始化jdbcJavaTypes：
        jdbcJavaTypes.put(new Integer(Types.LONGNVARCHAR), String.class); // -16 字符串
        jdbcJavaTypes.put(new Integer(Types.NCHAR), String.class); // -15 字符串
        jdbcJavaTypes.put(new Integer(Types.NVARCHAR), String.class); // -9 字符串
        jdbcJavaTypes.put(new Integer(Types.ROWID), String.class); // -8 字符串
        jdbcJavaTypes.put(new Integer(Types.BIT), Boolean.class); // -7 布尔
        jdbcJavaTypes.put(new Integer(Types.TINYINT), Byte.class); // -6 数字
        jdbcJavaTypes.put(new Integer(Types.BIGINT), Long.class); // -5 数字
        jdbcJavaTypes.put(new Integer(Types.LONGVARBINARY), Blob.class); // -4 二进制
        jdbcJavaTypes.put(new Integer(Types.VARBINARY), Blob.class); // -3 二进制
        jdbcJavaTypes.put(new Integer(Types.BINARY), Blob.class); // -2 二进制
        jdbcJavaTypes.put(new Integer(Types.LONGVARCHAR), String.class); // -1 字符串
        // jdbcJavaTypes.put(new Integer(Types.NULL), String.class); // 0 /
        jdbcJavaTypes.put(new Integer(Types.CHAR), String.class); // 1 字符串
        jdbcJavaTypes.put(new Integer(Types.NUMERIC), BigDecimal.class); // 2 数字
        jdbcJavaTypes.put(new Integer(Types.DECIMAL), BigDecimal.class); // 3 数字
        jdbcJavaTypes.put(new Integer(Types.INTEGER), Integer.class); // 4 数字
        jdbcJavaTypes.put(new Integer(Types.SMALLINT), Short.class); // 5 数字
        jdbcJavaTypes.put(new Integer(Types.FLOAT), BigDecimal.class); // 6 数字
        jdbcJavaTypes.put(new Integer(Types.REAL), BigDecimal.class); // 7 数字
        jdbcJavaTypes.put(new Integer(Types.DOUBLE), BigDecimal.class); // 8 数字
        jdbcJavaTypes.put(new Integer(Types.VARCHAR), String.class); // 12 字符串
        jdbcJavaTypes.put(new Integer(Types.BOOLEAN), Boolean.class); // 16 布尔
        // jdbcJavaTypes.put(new Integer(Types.DATALINK), String.class); // 70 /
        jdbcJavaTypes.put(new Integer(Types.DATE), Date.class); // 91 日期
        jdbcJavaTypes.put(new Integer(Types.TIME), Date.class); // 92 日期
        jdbcJavaTypes.put(new Integer(Types.TIMESTAMP), Date.class); // 93 日期
        jdbcJavaTypes.put(new Integer(Types.OTHER), Object.class); // 1111 其他类型？
        // jdbcJavaTypes.put(new Integer(Types.JAVA_OBJECT), Object.class); // 2000
        // jdbcJavaTypes.put(new Integer(Types.DISTINCT), String.class); // 2001
        // jdbcJavaTypes.put(new Integer(Types.STRUCT), String.class); // 2002
        // jdbcJavaTypes.put(new Integer(Types.ARRAY), String.class); // 2003
        jdbcJavaTypes.put(new Integer(Types.BLOB), Blob.class); // 2004 二进制
        jdbcJavaTypes.put(new Integer(Types.CLOB), Clob.class); // 2005 大文本
        // jdbcJavaTypes.put(new Integer(Types.REF), String.class); // 2006
        // jdbcJavaTypes.put(new Integer(Types.SQLXML), String.class); // 2009
        jdbcJavaTypes.put(new Integer(Types.NCLOB), Clob.class); // 2011 大文本

        for (Iterator<Integer> iterator = jdbcJavaTypes.keySet().iterator(); iterator.hasNext();) {
            Integer next = iterator.next();
            javaJdbcTypes.put(jdbcJavaTypes.get(next),next);

        }
    }

    public static int getJdbcCode(Class<?> javaTypeClass) {
        return javaJdbcTypes.get(javaTypeClass);
    }

    public static SqlTypeName jdbcTypeToSqlTypeName(int jdbcType) {
        SqlTypeName nameForJdbcType = SqlTypeName.getNameForJdbcType(jdbcType);
        return nameForJdbcType;
    }

    public static SqlTypeFamily jdbcTypeToSqlTypeFamily(int jdbcType) {
        SqlTypeName nameForJdbcType = SqlTypeName.getNameForJdbcType(jdbcType);
        if (nameForJdbcType == null) {
            return null;
        }
        return nameForJdbcType.getFamily();
    }

    public static Integer mysqlTypeToJdbcType(String type) {
        if (method != null) {
            try {
                return (Integer) method.invoke(null, new Object[]{type});
            } catch (IllegalAccessException | InvocationTargetException e) {
            }
        }
        return null;
    }

    public static void main(String[] args) {
        System.out.println(mysqlTypeToJdbcType("varchar"));
    }

}

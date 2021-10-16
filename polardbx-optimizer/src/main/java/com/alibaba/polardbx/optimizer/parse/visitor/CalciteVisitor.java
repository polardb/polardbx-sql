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

package com.alibaba.polardbx.optimizer.parse.visitor;

import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import com.alibaba.polardbx.optimizer.parse.FastSqlParserException;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.HashMap;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 基础类，为了方便调试暂时继承自MySqlOutputVisitor
 *
 * @author hongxi.chx on 2017/11/21.
 * @since 5.0.0
 */
abstract class CalciteVisitor extends MySqlASTVisitorAdapter {

    public final static String DUAL_TABLE = "DUAL";
    public final static String UNKNOWN = "UNKNOWN";
    public final static String COLUMN_ALL = "*";

    private static final ConcurrentMap<String, TimeUnit> integerTimeUnit = new ConcurrentHashMap<>(10, 1);

    public static TimeUnit getTimeUnit(String timeUintName) {
        TimeUnit timeUnit = integerTimeUnit.get(integerTimeUnit);
        if (timeUnit == null) {

            TimeUnit timeUnitTemp = null;
            try {
                String intervalType = timeUintName.toUpperCase();
                final String odbcPrefix = "SQL_TSI_";
                if (intervalType.startsWith(odbcPrefix)) {
                    intervalType = intervalType.substring(odbcPrefix.length());
                }
                timeUnitTemp = TimeUnit.valueOf(intervalType);
            } catch (IllegalArgumentException e) {
                throw new FastSqlParserException(FastSqlParserException.ExceptionType.NOT_SUPPORT,
                    "Unsupported time unit (1)" + timeUintName);
            }
            timeUnit = integerTimeUnit.putIfAbsent(timeUintName, timeUnitTemp);
            if (timeUnit == null) {
                timeUnit = timeUnitTemp;
            }
        }
        return timeUnit;

    }

    protected static SqlTypeName getCastType(String type) {
        SqlTypeName value = null;
        try {
            if (type == null) {
                return null;
            }
            CastType castType = CastType.valueOf(type.toUpperCase());
            if (castType != null) {
                return castType.getTypeName();
            }
        } catch (NullPointerException | IllegalArgumentException e) {
            throw new FastSqlParserException(FastSqlParserException.ExceptionType.NOT_SUPPORT,
                "Unsupported Cast type " + type);
        }
        return value;
    }

    protected SqlTypeName getType(String type) {
        SqlTypeName value = null;
        try {
            if (type == null) {
                return null;
            }
            Type calType = Type.valueOf(type.toUpperCase());
            if (calType != null) {
                return calType.getTypeName();
            }
        } catch (NullPointerException | IllegalArgumentException e) {
            throw new FastSqlParserException(FastSqlParserException.ExceptionType.NOT_SUPPORT,
                "Unsupported Cast type " + type);
        }
        return value;
    }

    protected SqlTypeName getIntervalType(String type) {
        SqlTypeName value = null;
        try {
            if (type == null) {
                return null;
            }
            IntervalType intervalType = IntervalType.valueOf(type.toUpperCase());
            if (intervalType != null) {
                return intervalType.getTypeName();
            }
        } catch (NullPointerException | IllegalArgumentException e) {
            throw new FastSqlParserException(FastSqlParserException.ExceptionType.NOT_SUPPORT,
                "Unsupported interval type " + type);
        }
        return value;
    }

    public static SqlDataTypeSpec convertTypeToSpec(SqlTypeName type, String charSetName, TimeZone timeZone, int i,
                                                    int j) {
        JavaTypeFactory typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        int precision = -1, scale = -1;
        if (SqlTypeName.DECIMAL == type) {
            precision = 10;
            scale = 0;
            if (i > 0) {
                precision = i;
            }
            if (j >= 0) {
                scale = j;
            }
        }
        RelDataType sqlType = typeFactory.createSqlType(type, precision, scale);
        SqlTypeName typeName = sqlType.getSqlTypeName();
        assert typeName != null;
        SqlIdentifier typeIdentifier = new SqlIdentifier(typeName.name(), SqlParserPos.ZERO);

        return new SqlDataTypeSpec(typeIdentifier, sqlType.getPrecision(), sqlType.getScale(), charSetName, timeZone,
            SqlParserPos.ZERO);
    }

    public static enum Type {
        BINARY(SqlTypeName.BINARY), CHAR(SqlTypeName.CHAR), DATE(SqlTypeName.DATE), DATETIME(SqlTypeName.DATETIME),
        TIME(SqlTypeName.TIME), DECIMAL(SqlTypeName.DECIMAL), SIGNED(SqlTypeName.SIGNED),
        UNSIGNED(SqlTypeName.UNSIGNED), BIGINT(SqlTypeName.BIGINT);

        private SqlTypeName stn;

        private Type(SqlTypeName stn) {
            this.stn = stn;
        }

        public SqlTypeName getTypeName() {
            return stn;
        }
    }

    public static enum CastType {
        BINARY(SqlTypeName.BINARY), CHAR(SqlTypeName.CHAR), DATE(SqlTypeName.DATE), DATETIME(SqlTypeName.DATETIME),
        TIME(SqlTypeName.TIME), DECIMAL(SqlTypeName.DECIMAL), SIGNED(SqlTypeName.SIGNED),
        UNSIGNED(SqlTypeName.UNSIGNED), BIGINT(SqlTypeName.BIGINT);

        private SqlTypeName stn;

        private CastType(SqlTypeName stn) {
            this.stn = stn;
        }

        public SqlTypeName getTypeName() {
            return stn;
        }
    }

    public static enum IntervalType {
        YEAR(SqlTypeName.INTERVAL_YEAR),
        MONTH(SqlTypeName.INTERVAL_MONTH),
        DAY(SqlTypeName.INTERVAL_DAY),
        HOUR(SqlTypeName.INTERVAL_HOUR),
        MINUTE(SqlTypeName.INTERVAL_MINUTE),
        SECOND(SqlTypeName.INTERVAL_SECOND),
        YEAR_MONTH(SqlTypeName.INTERVAL_YEAR_MONTH),
        DAY_HOUR(SqlTypeName.INTERVAL_DAY_HOUR),
        DAY_MINUTE(SqlTypeName.INTERVAL_DAY_MINUTE),
        DAY_SECOND(SqlTypeName.INTERVAL_DAY_SECOND),
        HOUR_MINUTE(SqlTypeName.INTERVAL_HOUR_MINUTE),
        HOUR_SECOND(SqlTypeName.INTERVAL_HOUR_SECOND),
        MINUTE_SECOND(SqlTypeName.INTERVAL_MINUTE_SECOND);

        private SqlTypeName stn;

        private IntervalType(SqlTypeName stn) {
            this.stn = stn;
        }

        public SqlTypeName getTypeName() {
            return stn;
        }
    }

    public static enum IndexType {
        FORCE("FORCE"),
        USE("USE"),
        IGNORE("IGNORE");

        private final static String index = "INDEX";
        private final static String forKey = "FOR";

        private String indexType;

        private IndexType(String indexType) {
            this.indexType = new StringBuilder().append(indexType).append(" ").append(index).toString();
        }

        public String getIndex() {
            return indexType;
        }

        public String getForKey() {
            return forKey;
        }
    }

    private static class UnsupportMethod {
        static UnsupportMethod unsupportMethod = new UnsupportMethod();

        private HashMap<String, Object> stringObjectHashMap = new HashMap<>();

        private UnsupportMethod() {
            //stringObjectHashMap.put("JSON_ARRAYAGG",null);
            //stringObjectHashMap.put("JSON_OBJECTAGG",null);
            stringObjectHashMap.put("STD", null);
            stringObjectHashMap.put("STDDEV", null);
            stringObjectHashMap.put("STDDEV_POP", null);
            stringObjectHashMap.put("STDDEV_SAMP", null);
            stringObjectHashMap.put("VAR_POP", null);
            stringObjectHashMap.put("VAR_SAMP", null);
            stringObjectHashMap.put("VARIANCE", null);

//            stringObjectHashMap.put("BIT_AND", null);
//            stringObjectHashMap.put("BIT_XOR",null);
//            stringObjectHashMap.put("BIT_OR",null);

            /**
             * <pre>
             * window funcs, 
             * support list:
             *   ROW_NUMBER
             *   DENSE_RANK
             *   RANK
             *   PERCENT_RANK
             *   CUME_DIST
             *   FIRST_VALUE
             *   LAST_VALUE
             *   NTH_VALUE
             * </pre>
             */
//            stringObjectHashMap.put("LAG", null);
//            stringObjectHashMap.put("LEAD", null);
//            stringObjectHashMap.put("NTILE", null);
        }

        public boolean isNotSupportFunction(String function) {
            if (function == null) {
                return true;
            }
            function = function.toUpperCase();
            return stringObjectHashMap.containsKey(function);
        }

    }

    public boolean isNotSupportFunction(String function) {
        return UnsupportMethod.unsupportMethod.isNotSupportFunction(function);
    }

}

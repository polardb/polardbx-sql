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

package com.alibaba.polardbx.optimizer.memory;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.memory.ObjectSizeUtils;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.Field;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyOperationBuilderCommon;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableScanBuilder;
import it.unimi.dsi.fastutil.Hash;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.EnumSqlType;

import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Estimate memory usage in bytes according to RelRowType
 *
 */
public class MemoryEstimator {

    public static final long singleParamRetainedSize = ObjectSizeUtils.BASE_SIZE_BYTES;
    public static final long singleCommaRetainedSize = ObjectSizeUtils.BASE_SIZE_BYTES;

    protected static final Map<Class, ParameterMethod> javaBaseClassParameterMethodTypeMap =
        new HashMap<Class, ParameterMethod>();
    protected static final Set<ParameterMethod> setObjectParameterMethodSet = new HashSet<>();

    static {
        javaBaseClassParameterMethodTypeMap.put(Boolean.class, ParameterMethod.setBoolean);

        javaBaseClassParameterMethodTypeMap.put(Short.class, ParameterMethod.setShort);
        javaBaseClassParameterMethodTypeMap.put(Integer.class, ParameterMethod.setInt);
        javaBaseClassParameterMethodTypeMap.put(Long.class, ParameterMethod.setLong);

        javaBaseClassParameterMethodTypeMap.put(Double.class, ParameterMethod.setDouble);
        javaBaseClassParameterMethodTypeMap.put(Float.class, ParameterMethod.setFloat);

        javaBaseClassParameterMethodTypeMap.put(BigDecimal.class, ParameterMethod.setBigDecimal);
        javaBaseClassParameterMethodTypeMap.put(BigInteger.class, ParameterMethod.setBigDecimal);

        javaBaseClassParameterMethodTypeMap.put(String.class, ParameterMethod.setString);
        javaBaseClassParameterMethodTypeMap.put(Clob.class, ParameterMethod.setClob);
        javaBaseClassParameterMethodTypeMap.put(Blob.class, ParameterMethod.setBlob);
        javaBaseClassParameterMethodTypeMap.put(Byte.class, ParameterMethod.setByte);
        javaBaseClassParameterMethodTypeMap.put(byte[].class, ParameterMethod.setBytes);

        javaBaseClassParameterMethodTypeMap.put(InputStream.class, ParameterMethod.setAsciiStream);

        javaBaseClassParameterMethodTypeMap.put(Date.class, ParameterMethod.setDate1);
        javaBaseClassParameterMethodTypeMap.put(Time.class, ParameterMethod.setTime1);
        javaBaseClassParameterMethodTypeMap.put(Timestamp.class, ParameterMethod.setTimestamp1);

        setObjectParameterMethodSet.add(ParameterMethod.setObject1);
        setObjectParameterMethodSet.add(ParameterMethod.setObject2);
        setObjectParameterMethodSet.add(ParameterMethod.setObject3);
    }

    private static final double HASH_KEY_SIZE_PER_ROW = Integer.BYTES / Hash.FAST_LOAD_FACTOR;

    public static double estimateRowSizeInArrayList(RelDataType rowType) {
        return estimateRowSize(rowType) * 1.1;
    }

    public static double estimateRowSizeInHashTable(RelDataType rowType) {
        return estimateRowSize(rowType) * 1.3 + HASH_KEY_SIZE_PER_ROW;
    }

    private static long estimateRowSize(List<RelDataTypeField> fields) {
        long result = 0;
        for (RelDataTypeField field : fields) {
            if (field.getType() instanceof BasicSqlType) {
                BasicSqlType sqlType = (BasicSqlType) field.getType();
                result += estimateFieldSize(sqlType);
            } else if (field.getType().isStruct()) {
                result += estimateRowSize(field.getType().getFieldList());
            } else if (field.getType() instanceof EnumSqlType) {
                result += 10;
            } else {
                throw new AssertionError();
            }
        }
        return result;
    }

    public static long estimateRowSize(RelDataType rowType) {
        return estimateRowSize(rowType.getFieldList());
    }

    public static long estimateColumnSize(ColumnMeta columnMeta) {
        RelDataType type = Optional.ofNullable(columnMeta).map(ColumnMeta::getField).map(
            Field::getRelType).orElse(null);
        if (type == null) {
            return 0;
        }
        long result = 0;
        if (type instanceof BasicSqlType) {
            BasicSqlType sqlType = (BasicSqlType) type;
            result += estimateFieldSize(sqlType);
        } else if (type.isStruct()) {
            result += estimateRowSize(type.getFieldList());
        } else if (type instanceof EnumSqlType) {
            result += 10;
        } else {
            throw new AssertionError();
        }
        return result;
    }

    private static long estimateFieldSize(BasicSqlType type) {
        switch (type.getSqlTypeName()) {
        case BOOLEAN:
            return 1;
        case TINYINT:
        case TINYINT_UNSIGNED:
        case SMALLINT:
            return Short.BYTES;
        case SMALLINT_UNSIGNED:
        case MEDIUMINT:
        case MEDIUMINT_UNSIGNED:
        case INTEGER:
        case SIGNED:
        case BIT:
            return Integer.BYTES;
        case INTEGER_UNSIGNED:
        case UNSIGNED:
        case BIGINT:
        case YEAR:
            return Long.BYTES;
        case BIGINT_UNSIGNED:
            return ObjectSizeUtils.BASE_SIZE_BIG_INTEGER + 8;
        case DECIMAL:
        case BIG_BIT:
            return 30; // see BigDecimalBlock
        case FLOAT:
            return Float.BYTES;
        case REAL:
        case DOUBLE:
            return Double.BYTES;
        case DATETIME:
        case DATE:
        case TIMESTAMP:
        case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            return 12; // see TimestampBlock
        case TIME:
        case TIME_WITH_LOCAL_TIME_ZONE:
            return 8; // see TimeBlock
        case INTERVAL_YEAR:
        case INTERVAL_YEAR_MONTH:
        case INTERVAL_MONTH:
        case INTERVAL_DAY:
        case INTERVAL_DAY_HOUR:
        case INTERVAL_DAY_MINUTE:
        case INTERVAL_DAY_SECOND:
        case INTERVAL_DAY_MICROSECOND:
        case INTERVAL_HOUR:
        case INTERVAL_HOUR_MINUTE:
        case INTERVAL_HOUR_MICROSECOND:
        case INTERVAL_HOUR_SECOND:
        case INTERVAL_MINUTE:
        case INTERVAL_MINUTE_SECOND:
        case INTERVAL_SECOND:
            // These should not appear in MySQL result set... Anyway giving an estimation is not bad
            return Long.BYTES;
        case ENUM:
            return ObjectSizeUtils.BASE_SIZE_STRING + 10;
        case CHAR:
        case VARCHAR:
        case BINARY:
        case VARBINARY:
            long length = Math.min(type.getPrecision(), 100); // treat VARCHAR(256) as length 100
            return ObjectSizeUtils.BASE_SIZE_STRING + length * 2; // UTF-16 use two bytes per character
        case NULL:
            return 0;
        }
        // for all strange values
        return 100;
    }

    protected static long calcMemByParameterContext(ParameterContext pc) {
        ParameterMethod pm = pc.getParameterMethod();
        long pcSize = ObjectSizeUtils.SIZE_OBJ_REF;

        assert pc.getArgs().length >= 2;
        Object obj = pc.getArgs()[1];
        if (setObjectParameterMethodSet.contains(pm)) {
            if (obj == null) {
                pm = ParameterMethod.setNull1;
            } else {
                Class cls = obj.getClass();
                ParameterMethod pmOfObj = javaBaseClassParameterMethodTypeMap.get(cls);
                if (pmOfObj == null) {
                    pcSize += ObjectSizeUtils.calculateDataSize(obj);
                    return pcSize;
                } else {
                    pm = pmOfObj;
                }
            }
        }

        // for ParameterMethod
        switch (pm) {
        case setArray:
            // ignore;
            break;
        case setBinaryStream:
            assert pc.getArgs().length == 3;
            pcSize += (long) pc.getArgs()[2];
            break;
        case setAsciiStream:
        case setCharacterStream:
        case setUnicodeStream:
            assert pc.getArgs().length == 3;
            long len = (long) pc.getArgs()[2];
            pcSize += len * ObjectSizeUtils.SIZE_CHAR;
            break;
        case setBlob:
            assert pc.getArgs().length == 2;
            try {
                pcSize += ((Blob) obj).length() * ObjectSizeUtils.SIZE_CHAR;
            } catch (Throwable e) {
                // igoren ex
            }
            break;
        case setClob:
            // ignore
            assert pc.getArgs().length == 2;
            try {
                pcSize += ((Clob) obj).length() * ObjectSizeUtils.SIZE_CHAR;
            } catch (Throwable e) {
                // igoren ex
            }
            break;
        case setString:
            assert pc.getArgs().length == 2;
            pcSize += ((String) obj).length() * ObjectSizeUtils.SIZE_CHAR;
            break;
        case setBoolean:
            pcSize += ObjectSizeUtils.SIZE_BOOLEAN;
            break;
        case setByte:
            pcSize += ObjectSizeUtils.SIZE_BYTE;
            break;
        case setBytes:
            assert pc.getArgs().length == 2;
            pcSize += ((byte[]) obj).length;
            break;
        case setBigDecimal:
            pcSize += ObjectSizeUtils.BASE_SIZE_BIG_DECIMAL;
            break;
        case setDouble:
            pcSize += Double.BYTES;
            break;
        case setFloat:
            pcSize += Float.BYTES;
            break;
        case setShort:
            pcSize += Short.BYTES;
            break;
        case setInt:
            pcSize += Integer.BYTES;
            break;
        case setLong:
            pcSize += Long.BYTES;
            break;
        case setDate1:
        case setDate2:
        case setTime1:
        case setTime2:
        case setTimestamp1:
        case setTimestamp2:
            pcSize += 12;
            break;
        case setNull1:
        case setNull2:
            // 0, so ignore
            break;
        case setRef:
            pcSize += ObjectSizeUtils.SIZE_OBJ_REF;
            break;
        case setURL:
            // ignore
            break;
        case setTableName:
            String tbStr = (String) obj;
            pcSize += tbStr.length() * ObjectSizeUtils.SIZE_CHAR;
            break;
        }
        return pcSize;
    }

    public static long calcPhyTableScanAndModifyViewBuilderMemory(long sqlSize,
                                                                  Map<Integer, ParameterContext> params,
                                                                  Map<String, List<List<String>>> targetTables) {
        long memVal = 0;
        memVal += sqlSize * ObjectSizeUtils.SIZE_CHAR + ObjectSizeUtils.SIZE_OBJ_REF;
        memVal += ObjectSizeUtils.SIZE_OBJ_REF * params.size() * 2;
        for (ParameterContext pc : params.values()) {
            memVal += calcMemByParameterContext(pc);
        }

        return memVal;
    }

    public static long calcSelectValuesMemCost(long batchSize, RelDataType selectRelRowType) {

        long memoryOfOneRowOfSelectValues =
            MemoryEstimator.estimateRowSize(selectRelRowType);
        long memoryOfBatchRows = memoryOfOneRowOfSelectValues * batchSize;
        return memoryOfBatchRows;

    }

    public static long calcPhyTableScanMemCost(String groupName, List<List<String>> phyTableListArr,
                                               ExecutionContext executionContext) {
        long phyOpMem = 0;
        int lengthOfPhyTbOfOneShard = 0;

        if (phyTableListArr.isEmpty()
            && executionContext.getExplain() != null
            && executionContext.getExplain().explainMode.isAdvisor()) {
            return 0;
        }

        // for all tableNames
        List<String> phyTbListOfOneShard = phyTableListArr.get(0);
        for (int i = 0; i < phyTbListOfOneShard.size(); i++) {
            String phyTb = phyTbListOfOneShard.get(i);
            lengthOfPhyTbOfOneShard += phyTb.length();
        }
        phyOpMem += ObjectSizeUtils.SIZE_CHAR * (lengthOfPhyTbOfOneShard + groupName.length())
            + PhyTableOperation.INSTANCE_MEM_SIZE;
        return phyOpMem;
    }

    public static long calcPhyModifyViewScanMemCost(String groupName, List<List<String>> phyTableListArr,
                                                    Map<Integer, ParameterContext> paramsOfPhySql) {
        long phyOpMem = 0;

        int lengthOfPhyTbOfOneShard = 0;

        // for all tableNames
        if (phyTableListArr != null && phyTableListArr.size() > 0) {
            List<String> phyTbListOfOneShard = phyTableListArr.get(0);
            for (int i = 0; i < phyTbListOfOneShard.size(); i++) {
                String phyTb = phyTbListOfOneShard.get(i);
                lengthOfPhyTbOfOneShard += phyTb.length();
            }
        }

        /**
         * for dml, only calc one phy tbl name as mem cost
         */
        //phyOpMem += ObjectSizeUtils.SIZE_CHAR * lengthOfPhyTbOfOneShard * phyTableListArr.size();
        phyOpMem += ObjectSizeUtils.SIZE_CHAR * lengthOfPhyTbOfOneShard;

        // for groupName
        phyOpMem += ObjectSizeUtils.SIZE_CHAR * groupName.length();

        // for all refs of PhyTableOperation
        phyOpMem += PhyTableOperation.INSTANCE_MEM_SIZE;

        phyOpMem += paramsOfPhySql.size()
            * (ObjectSizeUtils.HASH_ENTRY_SIZE + ObjectSizeUtils.SIZE_OBJ_REF * 2
            + ObjectSizeUtils.SIZE_INTEGER + ParameterContext.INSTANCE_MEM_SIZE);

        return phyOpMem;
    }

    public static long calcPhyTableScanCursorMemCost(long cursorClassInstSize, PhyTableOperation plan) {

        long phyCursorMem = 0;
        PhyOperationBuilderCommon phyOpBuilder = plan.getPhyOperationBuilder();
        if (phyOpBuilder instanceof PhyTableScanBuilder) {
            PhyTableScanBuilder phyTableScanBuilder = (PhyTableScanBuilder) phyOpBuilder;
            phyCursorMem = cursorClassInstSize;
            String logicalSqlTemplate = plan.getNativeSql();
            int realUnionSize = plan.getUnionSize();
            int logicalParamsSize = phyTableScanBuilder.getParams().size();
            long jdbcSqlMemory = ObjectSizeUtils.SIZE_CHAR * logicalSqlTemplate.length() * realUnionSize;
            long jdbcParamCountOfPhySql = realUnionSize * logicalParamsSize;
            phyCursorMem += calcJdbcStmtMemCost(jdbcSqlMemory, jdbcParamCountOfPhySql);
        }
        return phyCursorMem;
    }

    public static long calcPhyTableModifyCursorMemCost(long cursorClassInstSize, PhyTableOperation plan) {
        long phyCursorMem = 0;
        long jdbcParamCountOfPhySql = 0;
        if (plan.getParam() != null && plan.getParam().size() > 0) {
            jdbcParamCountOfPhySql = plan.getParam().size();
        }
        phyCursorMem += cursorClassInstSize;
        phyCursorMem += calcJdbcStmtMemCost(ObjectSizeUtils.SIZE_OBJ_REF, jdbcParamCountOfPhySql);
        return phyCursorMem;
    }

    /**
     * calculte the memory cost for executing query on RDS
     */
    public static long calcJdbcStmtMemCost(long jdbcSqlMemory, long jdbcParamCountOfPhySql) {

        // the total params count of phy stmt: count of each stmt * unionSize
        long psParamCount = jdbcParamCountOfPhySql;

        // originalSql field of JDBC42PreparedStatement
        long originalSqlMem = jdbcSqlMemory;

        // isNull field of JDBC42PreparedStatement,
        // boolean[psParamCount],
        long psIsNullMem = ObjectSizeUtils.SIZE_BOOLEAN * psParamCount;

        // isStream field of JDBC42PreparedStatement,
        // boolean[psParamCount],
        long psIsStreamMem = ObjectSizeUtils.SIZE_BOOLEAN * psParamCount;

        // parameterTypes field of JDBC42PreparedStatement,
        // int[psParamCount],
        // No need to count the memory of objects in parameterTypes(int[]) since
        // all of them are Integers and refers to the same Integer object
        long psParamTypesMem = ObjectSizeUtils.SIZE_OBJ_REF * psParamCount;

        // streamLengths field of JDBC42PreparedStatement,
        // int[psParamCount],
        // No need to count the memory of objects in streamLengths(int[]) since
        // all of them are Integers and refers to the same Integer object
        long psStreamLengthsMem = ObjectSizeUtils.SIZE_OBJ_REF * psParamCount;

        // parameterStreams field of JDBC42PreparedStatement,
        // InputStream[psParamCount],
        // all contents are null, contents will not
        // be null only if calling a procedure
        long psParameterStreamsMem = ObjectSizeUtils.SIZE_OBJ_REF * psParamCount;

        // paramterValues field of JDBC42PreparedStatement,
        // byte[psParamCount][],
        // No need to count the memory of objects in parameterValues since they
        // all refer to the objects of params.
        // for different kind of params, each param takes different bytes, for
        // example, since we can't accurately define what type the params are,
        // here we
        // just assume they are Integers, it's OK since we don't acctually
        // expect an accurate memory cost
        long psParamValuesMem = ObjectSizeUtils.SIZE_OBJ_REF * psParamCount
            + MemoryEstimator.singleParamRetainedSize
            * psParamCount;

        // staticSqlStrings field of JDBC42PreparedStatement,
        // byte[psParamCount][],
        // eg:
        // byte[53] /*DRDS /127.0.0.1/eda38cb54400000/ */( SELECT *.FROM
        // byte[28] AS `ggg`.WHERE (`value` IN(
        // byte[31] ))) UNION ALL ( SELECT *.FROM
        // byte[28] AS `ggg`.WHERE (`value` IN(
        // byte[31] ))) UNION ALL ( SELECT *.FROM
        // ...........
        // byte[2] ,
        // byte[2] ,
        // byte[2] ,
        // ...........
        // the count of ',' == psParamCount
        long psStaticSqlStringsMem = ObjectSizeUtils.SIZE_OBJ_REF * psParamCount + singleCommaRetainedSize
            * psParamCount;

        return psParamValuesMem + psParamTypesMem + psStreamLengthsMem + psParameterStreamsMem + psStaticSqlStringsMem
            + psIsNullMem + psIsStreamMem + originalSqlMem;
    }

    protected static int COMMA_ASCII = 44;

    public static long statSqlCommaCount(String sql) {
        long commaCnt = 0;
        for (int i = 0; i < sql.length(); i++) {
            if (sql.charAt(i) == COMMA_ASCII) {
                ++commaCnt;
            }
        }
        return commaCnt;
    }

}


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

package com.alibaba.polardbx.optimizer.partition.datatype;

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.type.MySQLStandardFieldType;
import com.alibaba.polardbx.common.utils.time.core.MySQLTimeVal;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.field.SessionProperties;
import com.alibaba.polardbx.optimizer.core.field.TypeConversionStatus;
import io.airlift.slice.Slice;

import java.sql.ResultSet;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Represent a type-specific field for partition pruning.
 * The goal of PartitionField is to simulate the mysql storage layer.
 */
public interface PartitionField extends Comparable<PartitionField> {

    /**
     * The metadata of the field.
     * Get the standard name of field type (immutable).
     */
    MySQLStandardFieldType mysqlStandardFieldType();

    /**
     * The metadata of the field.
     * Get the DataType.java representation of type (immutable).
     */
    DataType dataType();

    /**
     * The metadata of the field.
     * pack_length() returns size (in bytes) used to store field data in memory
     * (i.e. it returns the maximum size of the field in a row of the table,
     * which is located in RAM).
     * (Mutable)
     */
    int packetLength();

    /**
     * The Type Conversion Status for the last call of store(...)
     */
    TypeConversionStatus lastStatus();

    /**
     * The Predicate boolean value for the last call of store
     */
    PredicateBoolean lastPredicateBoolean();

    /**
     * Write object to the field.
     * To store the object value according to result type and session variables.
     *
     * @param value the object value to store.
     * @param resultType the type of expression or parameterized value.
     * @param sessionProperties current session variables.
     */
    TypeConversionStatus store(Object value, DataType<?> resultType, SessionProperties sessionProperties);

    /**
     * Write object to the field without session variables
     * To store the object value according to result type and session variables.
     *
     * @param value the object value to store.
     * @param resultType the type of expression or parameterized value.
     */
    TypeConversionStatus store(Object value, DataType<?> resultType);

    /**
     * Write object to the field without session variables.
     * In addition, this method will check and modify the monotonicity according to the comparison operator.
     *
     * @param value the object value to store.
     * @param resultType the type of expression or parameterized value.
     * @param endpoints endpoints[0] = comparison kind, TRUE = > or >=, FALSE = < or <=.
     * endpoints[1] = monotonicity of left & right endpoints, TRUE = open, FALSE = close.
     * @return type conversion status.
     */
    TypeConversionStatus store(Object value, DataType<?> resultType, boolean[] endpoints);

    /**
     * Write object to the field.
     * To store the object value according to result type and session variables.
     * <p>
     * In addition, this method will check and modify the monotonicity according to the comparison operator.
     *
     * @param value the object value to store.
     * @param resultType the type of expression or parameterized value.
     * @param sessionProperties session variables.
     * @param endpoints endpoints[0] = comparison kind, TRUE = > or >=, FALSE = < or <=.
     * endpoints[1] = monotonicity of left & right endpoints, TRUE = open, FALSE = close.
     * @return type conversion status.
     */
    TypeConversionStatus store(Object value, DataType<?> resultType, SessionProperties sessionProperties,
                               boolean[] endpoints);

    /**
     * Write object to the field without session variables.
     * In addition, this method will check and modify the monotonicity according to the comparison operator.
     *
     * @param value the object value to store.
     * @param resultType the type of expression or parameterized value.
     * @param endpoints endpoints[0] = comparison kind, TRUE = > or >=, FALSE = < or <=.
     * endpoints[1] = monotonicity of left & right endpoints, TRUE = open, FALSE = close.
     * @param diff difference between original value & stored value,
     * 0 if STORED = ORIGIN,  >0 if STORED > ORIGIN, <0 if STORED < ORIGIN.
     * @return type conversion status.
     */
    TypeConversionStatus store(Object value, DataType<?> resultType, boolean[] endpoints, AtomicInteger diff);

    /**
     * Write object to the field without session variables.
     * In addition, this method will check and modify the monotonicity according to the comparison operator.
     *
     * @param value the object value to store.
     * @param resultType the type of expression or parameterized value.
     * @param endpoints endpoints[0] = comparison kind, TRUE = > or >=, FALSE = < or <=.
     * endpoints[1] = monotonicity of left & right endpoints, TRUE = open, FALSE = close.
     * @param diff difference between original value & stored value,
     * 0 if STORED = ORIGIN,  >0 if STORED > ORIGIN, <0 if STORED < ORIGIN.
     * @param sessionProperties current session variables.
     * @return type conversion status.
     */
    TypeConversionStatus store(Object value, DataType<?> resultType, SessionProperties sessionProperties,
                               boolean[] endpoints, AtomicInteger diff);

    /**
     * Write Object to field from ResultSet with sessionProperties.
     *
     * @param rs result set
     * @param columnIndex start from 1
     * @param sessionProperties current session variables.
     */
    TypeConversionStatus store(ResultSet rs, int columnIndex, SessionProperties sessionProperties);

    /**
     * Write Object to field from ResultSet.
     *
     * @param rs result set
     * @param columnIndex start from 1
     */
    TypeConversionStatus store(ResultSet rs, int columnIndex);

    /**
     * To reuse the partition field, we can reset some status except the field data type.
     */
    void reset();

    void setNull();

    /**
     * Utilities of the field.
     * Get two hash value from field.
     */
    void hash(long[] numbers);

    /**
     * Get the raw representation of the field object.
     */
    byte[] rawBytes();

    PartitionField maxValue();

    PartitionField minValue();

    /**************************** Type Specific Read **********************************/

    /**
     * Get date/datetime value.
     */
    MysqlDateTime datetimeValue(int timeParseFlags, SessionProperties sessionProperties);

    /**
     * Get date/datetime value according to session properties
     */
    MysqlDateTime datetimeValue();

    /**
     * Get time value.
     */
    MysqlDateTime timeValue(int timeParseFlags, SessionProperties sessionProperties);

    /**
     * Get time value according to session properties
     */
    MysqlDateTime timeValue();

    /**
     * Get timestamp value.
     */
    MySQLTimeVal timestampValue(int timeParseFlags, SessionProperties sessionProperties);

    /**
     * Get timestamp value according to session properties
     */
    MySQLTimeVal timestampValue();

    /**
     * Get the integer value according to session properties
     */
    long longValue(SessionProperties sessionProperties);

    /**
     * Get the integer value by default configurations.
     */
    long longValue();

    /**
     * Get the string value according to session properties
     */
    Slice stringValue(SessionProperties sessionProperties);

    /**
     * Get the string value by default configurations.
     */
    Slice stringValue();

    /**
     * Get the decimal value from field.
     */
    Decimal decimalValue();

    /**
     * Get the decimal value from field  according to session properties.
     */
    Decimal decimalValue(SessionProperties sessionProperties);

    /**
     * Get the decimal value from field.
     */
    double doubleValue();

    /**
     * Get the decimal value from field  according to session properties.
     */
    double doubleValue(SessionProperties sessionProperties);

    /**
     * Check if the value stored in this field is null or not.
     */
    boolean isNull();

    /**
     * Set the nullability attribute info to this field.
     */
    void setNull(boolean isNull);
}

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

package com.alibaba.polardbx.common.utils.convertor;

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.jdbc.ZeroDate;
import com.alibaba.polardbx.common.jdbc.ZeroTime;
import com.alibaba.polardbx.common.jdbc.ZeroTimestamp;
import org.apache.commons.lang.ObjectUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Blob;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;


public class ConvertorHelper {


    public static Map<Class, Object> commonTypes = new HashMap<Class, Object>();
    public static final Convertor stringToCommon = new StringAndCommonConvertor.StringToCommon();
    public static final Convertor commonToCommon = new CommonAndCommonConvertor.CommonToCommon();

    public static final Convertor objectToString = new StringAndObjectConvertor.ObjectToString();

    private static final Convertor arrayToArray = new CollectionAndCollectionConvertor.ArrayToArray();
    private static final Convertor arrayToCollection = new CollectionAndCollectionConvertor.ArrayToCollection();
    private static final Convertor collectionToArray = new CollectionAndCollectionConvertor.CollectionToArray();
    private static final Convertor collectionToCollection =
        new CollectionAndCollectionConvertor.CollectionToCollection();

    public static final Convertor stringToEnum = new StringAndEnumConvertor.StringToEnum();
    public static final Convertor enumToString = new StringAndEnumConvertor.EnumToString();
    public static final Convertor sqlToDate = new SqlDateAndDateConvertor.SqlDateToDateConvertor();
    public static final Convertor dateToSql = new SqlDateAndDateConvertor.DateToSqlDateConvertor();
    public static final Convertor longToDate = new LongAndDateConvertor.LongToDateConvertor();
    public static final Convertor dateToLong = new LongAndDateConvertor.DateToLongConvertor();
    public static final Convertor blobToBytes = new BlobAndBytesConvertor.BlobToBytes();
    public static final Convertor stringToBytes = new StringAndObjectConvertor.StringToBytes();
    public static final Convertor bytesToString = new StringAndObjectConvertor.BytesToString();
    public static final Convertor bigDecimalToString = new StringAndObjectConvertor.BigDecimalToString();
    public static final Convertor realToString = new StringAndObjectConvertor.RealToString();
    public static final Convertor numberTobytes = new BlobAndBytesConvertor.NumberToBytes();
    public static final Convertor bitBytesToNumber = new BlobAndBytesConvertor.BitBytesToNumber();
    public static final Convertor timeTypeToNum = new TimeTypeConvertor.TimeTypeToNumber();
    public static final Convertor numToTimeType = new TimeTypeConvertor.NumberToTimeType();

    private static volatile ConvertorHelper singleton = null;

    private ConvertorRepository repository = null;

    public ConvertorHelper() {
        repository = new ConvertorRepository();
        initDefaultRegister();
    }

    public ConvertorHelper(ConvertorRepository repository) {

        this.repository = repository;
        initDefaultRegister();
    }

    public static ConvertorHelper getInstance() {
        if (singleton == null) {
            synchronized (ConvertorHelper.class) {
                if (singleton == null) {
                    singleton = new ConvertorHelper();
                }
            }
        }
        return singleton;
    }

    public Convertor getConvertor(Class src, Class dest) {
        if (src == dest) {

            return null;
        }

        Convertor convertor = repository.getConvertor(src, dest);

        if (convertor == null && dest == String.class) {
            if (src.isEnum()) {
                convertor = enumToString;
            } else {
                convertor = objectToString;
            }
        }

        boolean isSrcArray = src.isArray();
        boolean isDestArray = dest.isArray();
        if (convertor == null && src.isArray() && dest.isArray()) {
            convertor = arrayToArray;
        } else {
            boolean isSrcCollection = Collection.class.isAssignableFrom(src);
            boolean isDestCollection = Collection.class.isAssignableFrom(dest);
            if (convertor == null && isSrcArray && isDestCollection) {
                convertor = arrayToCollection;
            }
            if (convertor == null && isDestArray && isSrcCollection) {
                convertor = collectionToArray;
            }
            if (convertor == null && isSrcCollection && isDestCollection) {
                convertor = collectionToCollection;
            }
        }

        if (convertor == null && src == String.class) {
            if (commonTypes.containsKey(dest)) {
                convertor = stringToCommon;
            } else if (dest.isEnum()) {
                convertor = stringToEnum;
            }
        }

        if (convertor == null && commonTypes.containsKey(src) && commonTypes.containsKey(dest)) {
            convertor = commonToCommon;
        }

        return convertor;
    }

    public Convertor getConvertor(String alias) {
        return repository.getConvertor(alias);
    }

    public void registerConvertor(Class src, Class dest, Convertor convertor) {
        repository.registerConvertor(src, dest, convertor);
    }

    public void registerConvertor(String alias, Convertor convertor) {
        repository.registerConvertor(alias, convertor);
    }

    public void initDefaultRegister() {
        initCommonTypes();
        StringDateRegister();
    }

    private void StringDateRegister() {

        Convertor stringToDate = new StringAndDateConvertor.StringToDate();
        Convertor stringToCalendar = new StringAndDateConvertor.StringToCalendar();
        Convertor stringToSqlDate = new StringAndDateConvertor.StringToSqlDate();
        Convertor stringToSqlTime = new StringAndDateConvertor.StringToSqlTime();
        Convertor sqlDateToString = new StringAndDateConvertor.SqlDateToString();
        Convertor sqlTimeToString = new StringAndDateConvertor.SqlTimeToString();
        Convertor sqlTimestampToString = new StringAndDateConvertor.SqlTimestampToString();
        Convertor calendarToString = new StringAndDateConvertor.CalendarToString();
        Convertor localDateTimeToString = new StringAndDateConvertor.LocalDateTimeToString();
        Convertor localDateTimeToSqlDate = new SqlDateAndDateConvertor.LocalDateTimeToSqlDateConvertor();
        Convertor localDateTimeToDate = new SqlDateAndDateConvertor.LocalDateTimeToDateConvertor();
        Convertor sqlTimeToSqlDate = new SqlDateAndDateConvertor.SqlTimeToSqlDateConvertor();
        Convertor calendarToDate = new SqlDateAndDateConvertor.CalendarToDateConvertor();

        repository.registerConvertor(String.class, Date.class, stringToDate);
        repository.registerConvertor(String.class, Calendar.class, stringToCalendar);
        repository.registerConvertor(String.class, GregorianCalendar.class, stringToCalendar);

        repository.registerConvertor(String.class, java.sql.Time.class, stringToSqlTime);
        repository.registerConvertor(String.class, java.sql.Date.class, stringToSqlDate);
        repository.registerConvertor(String.class, java.sql.Timestamp.class, stringToSqlDate);

        repository.registerConvertor(java.util.Date.class, String.class, sqlTimestampToString);
        repository.registerConvertor(java.sql.Date.class, String.class, sqlDateToString);
        repository.registerConvertor(java.sql.Time.class, String.class, sqlTimeToString);
        repository.registerConvertor(java.sql.Timestamp.class, String.class, sqlTimestampToString);

        repository.registerConvertor(String.class, ZeroTime.class, stringToSqlTime);
        repository.registerConvertor(String.class, ZeroDate.class, stringToSqlDate);
        repository.registerConvertor(String.class, ZeroTimestamp.class, stringToSqlDate);

        repository.registerConvertor(ZeroDate.class, String.class, sqlTimestampToString);
        repository.registerConvertor(ZeroTime.class, String.class, sqlTimeToString);
        repository.registerConvertor(ZeroTimestamp.class, String.class, sqlTimestampToString);

        repository.registerConvertor(Calendar.class, String.class, calendarToString);
        repository.registerConvertor(GregorianCalendar.class, String.class, calendarToString);

        repository.registerConvertor(LocalDateTime.class, String.class, localDateTimeToString);
        repository.registerConvertor(LocalDateTime.class, Date.class, localDateTimeToDate);
        repository.registerConvertor(LocalDateTime.class, java.sql.Time.class, localDateTimeToSqlDate);
        repository.registerConvertor(LocalDateTime.class, java.sql.Date.class, localDateTimeToSqlDate);
        repository.registerConvertor(LocalDateTime.class, java.sql.Timestamp.class, localDateTimeToSqlDate);
        repository.registerConvertor(LocalDateTime.class, Long.class, dateToLong);
        repository.registerConvertor(LocalDateTime.class, Decimal.class, timeTypeToNum);
        repository.registerConvertor(LocalDateTime.class, Double.class, timeTypeToNum);
        repository.registerConvertor(LocalDateTime.class, Integer.class, timeTypeToNum);

        repository.registerConvertor(java.util.Date.class, Long.class, dateToLong);
        repository.registerConvertor(java.sql.Date.class, Long.class, dateToLong);
        repository.registerConvertor(java.sql.Time.class, Long.class, dateToLong);
        repository.registerConvertor(java.sql.Timestamp.class, Long.class, dateToLong);

        repository.registerConvertor(ZeroDate.class, Long.class, dateToLong);
        repository.registerConvertor(ZeroTimestamp.class, Long.class, dateToLong);
        repository.registerConvertor(ZeroTime.class, Long.class, dateToLong);

        repository.registerConvertor(Long.class, ZeroTimestamp.class, longToDate);
        repository.registerConvertor(Long.class, ZeroDate.class, longToDate);
        repository.registerConvertor(Long.class, ZeroTime.class, longToDate);

        repository.registerConvertor(Long.class, java.util.Date.class, longToDate);
        repository.registerConvertor(Long.class, java.sql.Date.class, longToDate);
        repository.registerConvertor(Long.class, java.sql.Time.class, longToDate);
        repository.registerConvertor(Long.class, java.sql.Timestamp.class, longToDate);

        repository.registerConvertor(java.sql.Date.class, Date.class, sqlToDate);
        repository.registerConvertor(java.sql.Time.class, Date.class, sqlToDate);
        repository.registerConvertor(java.sql.Timestamp.class, Date.class, sqlToDate);
        repository.registerConvertor(Date.class, java.sql.Date.class, dateToSql);
        repository.registerConvertor(Date.class, java.sql.Time.class, dateToSql);
        repository.registerConvertor(Date.class, java.sql.Timestamp.class, dateToSql);
        repository.registerConvertor(java.sql.Timestamp.class, java.sql.Date.class, dateToSql);
        repository.registerConvertor(java.sql.Timestamp.class, java.sql.Time.class, dateToSql);
        repository.registerConvertor(java.sql.Date.class, java.sql.Timestamp.class, dateToSql);
        repository.registerConvertor(java.sql.Date.class, java.sql.Time.class, dateToSql);
        repository.registerConvertor(java.sql.Time.class, java.sql.Timestamp.class, sqlTimeToSqlDate);
        repository.registerConvertor(java.sql.Time.class, java.sql.Date.class, sqlTimeToSqlDate);
        repository.registerConvertor(Calendar.class, Date.class, calendarToDate);
        repository.registerConvertor(Calendar.class, java.sql.Date.class, calendarToDate);
        repository.registerConvertor(Calendar.class, java.sql.Time.class, calendarToDate);
        repository.registerConvertor(Calendar.class, java.sql.Timestamp.class, calendarToDate);
        repository.registerConvertor(GregorianCalendar.class, Date.class, calendarToDate);
        repository.registerConvertor(GregorianCalendar.class, java.sql.Date.class, calendarToDate);
        repository.registerConvertor(GregorianCalendar.class, java.sql.Time.class, calendarToDate);
        repository.registerConvertor(GregorianCalendar.class, java.sql.Timestamp.class, calendarToDate);
        repository.registerConvertor(Blob.class, byte[].class, blobToBytes);
        repository.registerConvertor(String.class, byte[].class, stringToBytes);
        repository.registerConvertor(byte[].class, String.class, bytesToString);
        repository.registerConvertor(BigDecimal.class, String.class, bigDecimalToString);
        repository.registerConvertor(Float.class, String.class, realToString);
        repository.registerConvertor(Double.class, String.class, realToString);
        repository.registerConvertor(byte.class, byte[].class, numberTobytes);
        repository.registerConvertor(Byte.class, byte[].class, numberTobytes);
        repository.registerConvertor(Short.class, byte[].class, numberTobytes);
        repository.registerConvertor(Integer.class, byte[].class, numberTobytes);
        repository.registerConvertor(Long.class, byte[].class, numberTobytes);
        repository.registerConvertor(Character.class, byte[].class, numberTobytes);
        repository.registerConvertor(Double.class, byte[].class, numberTobytes);
        repository.registerConvertor(Timestamp.class, BigInteger.class, timeTypeToNum);
        repository.registerConvertor(Timestamp.class, Short.class, timeTypeToNum);
        repository.registerConvertor(Time.class, BigInteger.class, timeTypeToNum);
        repository.registerConvertor(java.sql.Date.class, BigInteger.class, timeTypeToNum);
        repository.registerConvertor(java.sql.Timestamp.class, Decimal.class, timeTypeToNum);
        repository.registerConvertor(Time.class, Decimal.class, timeTypeToNum);
        repository.registerConvertor(java.sql.Date.class, Decimal.class, timeTypeToNum);
        repository.registerConvertor(java.sql.Timestamp.class, Double.class, timeTypeToNum);
        repository.registerConvertor(Time.class, Double.class, timeTypeToNum);
        repository.registerConvertor(java.sql.Date.class, Double.class, timeTypeToNum);
        repository.registerConvertor(java.sql.Timestamp.class, Integer.class, timeTypeToNum);
        repository.registerConvertor(Time.class, Integer.class, timeTypeToNum);
        repository.registerConvertor(java.sql.Date.class, Integer.class, timeTypeToNum);

        repository.registerConvertor(BigInteger.class, Timestamp.class, numToTimeType);
        repository.registerConvertor(BigInteger.class, Time.class, numToTimeType);
        repository.registerConvertor(BigInteger.class, java.sql.Date.class, numToTimeType);
        repository.registerConvertor(Decimal.class, java.sql.Timestamp.class, numToTimeType);
        repository.registerConvertor(Decimal.class, Time.class, numToTimeType);
        repository.registerConvertor(Decimal.class, java.sql.Date.class, numToTimeType);
        repository.registerConvertor(Double.class, java.sql.Timestamp.class, numToTimeType);
        repository.registerConvertor(Double.class, Time.class, numToTimeType);
        repository.registerConvertor(Double.class, java.sql.Date.class, numToTimeType);
        repository.registerConvertor(Integer.class, java.sql.Timestamp.class, numToTimeType);
        repository.registerConvertor(Integer.class, Time.class, numToTimeType);
        repository.registerConvertor(Integer.class, java.sql.Date.class, numToTimeType);
        repository.registerConvertor(Short.class, java.sql.Timestamp.class, numToTimeType);
        repository.registerConvertor(Short.class, Time.class, numToTimeType);
        repository.registerConvertor(Short.class, java.sql.Date.class, numToTimeType);

        repository.registerConvertor(BigDecimal.class, java.sql.Timestamp.class, numToTimeType);
        repository.registerConvertor(BigDecimal.class, Time.class, numToTimeType);
        repository.registerConvertor(BigDecimal.class, java.sql.Date.class, numToTimeType);
    }

    private void initCommonTypes() {
        commonTypes.put(int.class, ObjectUtils.NULL);
        commonTypes.put(Integer.class, ObjectUtils.NULL);
        commonTypes.put(short.class, ObjectUtils.NULL);
        commonTypes.put(Short.class, ObjectUtils.NULL);
        commonTypes.put(long.class, ObjectUtils.NULL);
        commonTypes.put(Long.class, ObjectUtils.NULL);
        commonTypes.put(boolean.class, ObjectUtils.NULL);
        commonTypes.put(Boolean.class, ObjectUtils.NULL);
        commonTypes.put(byte.class, ObjectUtils.NULL);
        commonTypes.put(Byte.class, ObjectUtils.NULL);
        commonTypes.put(char.class, ObjectUtils.NULL);
        commonTypes.put(Character.class, ObjectUtils.NULL);
        commonTypes.put(float.class, ObjectUtils.NULL);
        commonTypes.put(Float.class, ObjectUtils.NULL);
        commonTypes.put(double.class, ObjectUtils.NULL);
        commonTypes.put(Double.class, ObjectUtils.NULL);
        commonTypes.put(Decimal.class, ObjectUtils.NULL);
        commonTypes.put(BigDecimal.class, ObjectUtils.NULL);
        commonTypes.put(BigInteger.class, ObjectUtils.NULL);
    }

    public static Object convert(Object src, Class destType) {
        Convertor convertor = ConvertorHelper.getInstance().getConvertor(src.getClass(), destType);
        if (convertor == null) {
            return src;
        } else {
            return convertor.convert(src, destType);
        }
    }

    public void setRepository(ConvertorRepository repository) {
        this.repository = repository;
    }
}

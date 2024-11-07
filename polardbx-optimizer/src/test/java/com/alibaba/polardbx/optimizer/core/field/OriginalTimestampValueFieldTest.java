package com.alibaba.polardbx.optimizer.core.field;

import com.alibaba.polardbx.common.type.MySQLStandardFieldType;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.core.OriginalTimestamp;
import com.alibaba.polardbx.common.utils.time.parser.StringTimeParser;
import com.alibaba.polardbx.common.utils.time.parser.TimeParserFlags;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DateTimeType;
import com.alibaba.polardbx.optimizer.core.datatype.DateType;
import com.alibaba.polardbx.optimizer.core.datatype.TimestampType;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Types;

import static com.alibaba.polardbx.common.type.MySQLStandardFieldType.MYSQL_TYPE_DATETIME2;
import static com.alibaba.polardbx.common.type.MySQLStandardFieldType.MYSQL_TYPE_NEWDATE;
import static com.alibaba.polardbx.common.type.MySQLStandardFieldType.MYSQL_TYPE_TIMESTAMP2;

public class OriginalTimestampValueFieldTest {

    @Test
    public void testDatetime() {
        final String timeStr = "2024-06-13 16:36:55.545131";
        doTest(MYSQL_TYPE_DATETIME2, 6, timeStr, "2024-06-13 16:36:55.545131");
        doTest(MYSQL_TYPE_DATETIME2, 5, timeStr, "2024-06-13 16:36:55.54513");
        doTest(MYSQL_TYPE_DATETIME2, 4, timeStr, "2024-06-13 16:36:55.5451");
        doTest(MYSQL_TYPE_DATETIME2, 3, timeStr, "2024-06-13 16:36:55.545");
        doTest(MYSQL_TYPE_DATETIME2, 2, timeStr, "2024-06-13 16:36:55.55");
        doTest(MYSQL_TYPE_DATETIME2, 1, timeStr, "2024-06-13 16:36:55.5");
        doTest(MYSQL_TYPE_DATETIME2, 0, timeStr, "2024-06-13 16:36:56");
    }

    @Test
    public void testDate() {
        final String dateStr = "2024-06-13 23:59:59.875239";
        doTest(MYSQL_TYPE_NEWDATE, 6, dateStr, "2024-06-14 00:00:00");
    }

    @Test
    public void testTimestamp() {
        final String timeStr = "2024-06-13 16:36:55.545131";
        doTest(MYSQL_TYPE_TIMESTAMP2, 6, timeStr, "2024-06-13 16:36:55.545131");
        doTest(MYSQL_TYPE_TIMESTAMP2, 5, timeStr, "2024-06-13 16:36:55.54513");
        doTest(MYSQL_TYPE_TIMESTAMP2, 4, timeStr, "2024-06-13 16:36:55.5451");
        doTest(MYSQL_TYPE_TIMESTAMP2, 3, timeStr, "2024-06-13 16:36:55.545");
        doTest(MYSQL_TYPE_TIMESTAMP2, 2, timeStr, "2024-06-13 16:36:55.55");
        doTest(MYSQL_TYPE_TIMESTAMP2, 1, timeStr, "2024-06-13 16:36:55.5");
        doTest(MYSQL_TYPE_TIMESTAMP2, 0, timeStr, "2024-06-13 16:36:56");
    }

    private void doTest(MySQLStandardFieldType standardFieldType, int scale, String timeStr, String resultStr) {
        DataType dataType;
        StorageField storageField;
        int sqlType;
        switch (standardFieldType) {
        case MYSQL_TYPE_DATE:
        case MYSQL_TYPE_NEWDATE:
            dataType = new DateType();
            storageField = new DateField(dataType);
            sqlType = Types.DATE;
            break;
        case MYSQL_TYPE_DATETIME:
        case MYSQL_TYPE_DATETIME2:
            dataType = new DateTimeType(scale);
            storageField = new DatetimeField(dataType);
            sqlType = Types.TIMESTAMP;
            break;
        case MYSQL_TYPE_TIMESTAMP:
        case MYSQL_TYPE_TIMESTAMP2:
            dataType = new TimestampType(scale);
            storageField = new TimestampField(dataType);
            sqlType = Types.TIMESTAMP;
            break;
        default:
            throw new IllegalArgumentException();
        }

        // 2024-06-13 16:36:55.545131
        MysqlDateTime mysqlDateTime = StringTimeParser.parseString(
            timeStr.getBytes(), sqlType,
            TimeParserFlags.FLAG_TIME_FUZZY_DATE | TimeParserFlags.FLAG_TIME_NO_DATE_FRAC_WARN);

        OriginalTimestamp originalTimestamp = new OriginalTimestamp(mysqlDateTime);

        TypeConversionStatus typeConversionStatus = storageField.store(originalTimestamp, dataType);

        System.out.println(typeConversionStatus);
        System.out.println(originalTimestamp);
        System.out.println(storageField.datetimeValue());

        Assert.assertTrue(typeConversionStatus == TypeConversionStatus.TYPE_OK);

        Assert.assertTrue(originalTimestamp.toString().equals(timeStr));

        Assert.assertTrue(storageField.datetimeValue().toString().equals(resultStr));
    }
}

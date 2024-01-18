package com.alibaba.polardbx.optimizer.partition.datatype;

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.datatype.DecimalBounds;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.DecimalType;
import com.alibaba.polardbx.optimizer.core.datatype.DoubleType;
import com.alibaba.polardbx.optimizer.core.datatype.LongType;
import com.alibaba.polardbx.optimizer.core.datatype.StringType;
import com.alibaba.polardbx.optimizer.core.datatype.TinyIntType;
import com.alibaba.polardbx.optimizer.core.datatype.VarcharType;
import com.alibaba.polardbx.optimizer.core.field.SessionProperties;
import com.alibaba.polardbx.optimizer.core.field.TypeConversionStatus;
import org.junit.Test;

import java.math.BigDecimal;

import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.MAX_DECIMAL_PRECISION;
import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.MAX_DECIMAL_SCALE;

public class DecimalPartitionFieldTest {

    @Test
    public void testIntervalEndPoints() {
        DecimalType decimalType = new DecimalType(24, 0);
        PartitionField df1 = new DecimalPartitionField(decimalType);

        String d1Str = "9223372036854775806.4";
        Decimal d1 = Decimal.fromString(d1Str);

        boolean[] endpoints = new boolean[2];

        /**
         *
         * <pre>
         * endpoints[0]=true  <=>  const < col or const <= col, so const is the left end point,
         * endpoints[0]=false <=>  col < const or col <= const, so const is NOT the left end point,
         *
         * endpoints[1]=true <=> const <= col or col <= const, label if included endpoint
         * endpoints[1]=false <=> const < col or col < const, label if included endpoint
         * </pre>
         *
         * @param enddpoints
         * @return
         */
        endpoints[0] = true; // false: col < d1Str, true: col > d1Str
        endpoints[1] = false; //

        ExecutionContext context = new ExecutionContext();
        SessionProperties sessionProperties = SessionProperties.fromExecutionContext(context);
        df1.store(d1Str, DataTypes.StringType, sessionProperties, endpoints);

        System.out.println(df1.stringValue().toStringUtf8());
        System.out.println(df1.lastStatus().toString());
        System.out.println(df1.lastPredicateBoolean().toString());

        // truncate value
        Assert.assertTrue(df1.lastStatus() == TypeConversionStatus.TYPE_NOTE_TRUNCATED);

        // pred of col col > 9223372036854775806.4
        Assert.assertTrue(df1.lastPredicateBoolean() == PredicateBoolean.IS_NOT_ALWAYS_TRUE_OR_FALSE);

        // # col > 9223372036854775806.4  ==after store==>  col > 9223372036854775806
        Assert.assertTrue(endpoints[0] == true);
        Assert.assertTrue(endpoints[1] == false);

        Assert.assertTrue(df1.stringValue().toStringUtf8().equals("9223372036854775806"));
    }

    @Test
    public void testIntervalEndPoints5() {
        DecimalType decimalType = new DecimalType(24, 0);
        PartitionField df1 = new DecimalPartitionField(decimalType);

        String d1Str = "9223372036854775806.4";
        Decimal d1 = Decimal.fromString(d1Str);

        boolean[] endpoints = new boolean[2];

        /**
         *
         * <pre>
         * endpoints[0]=true  <=>  const < col or const <= col, so const is the left end point,
         * endpoints[0]=false <=>  col < const or col <= const, so const is NOT the left end point,
         *
         * endpoints[1]=true <=> const <= col or col <= const, label if included endpoint
         * endpoints[1]=false <=> const < col or col < const, label if included endpoint
         * </pre>
         *
         * @param enddpoints
         * @return
         */
        endpoints[0] = true; // false: col < d1Str, true: col > d1Str
        endpoints[1] = true; //

        ExecutionContext context = new ExecutionContext();
        SessionProperties sessionProperties = SessionProperties.fromExecutionContext(context);
        df1.store(d1Str, DataTypes.StringType, sessionProperties, endpoints);

        System.out.println(df1.stringValue().toStringUtf8());
        System.out.println(df1.lastStatus().toString());
        System.out.println(df1.lastPredicateBoolean().toString());

        // truncate value
        Assert.assertTrue(df1.lastStatus() == TypeConversionStatus.TYPE_NOTE_TRUNCATED);

        // pred of col col > 9223372036854775806.4
        Assert.assertTrue(df1.lastPredicateBoolean() == PredicateBoolean.IS_NOT_ALWAYS_TRUE_OR_FALSE);

        // # col >= 9223372036854775806.4  ==after store==>  col > 9223372036854775806
        Assert.assertTrue(endpoints[0] == true);
        Assert.assertTrue(endpoints[1] == false);

        Assert.assertTrue(df1.stringValue().toStringUtf8().equals("9223372036854775806"));
    }

    @Test
    public void testIntervalEndPoints2() {
        DecimalType decimalType = new DecimalType(24, 0);
        PartitionField df1 = new DecimalPartitionField(decimalType);

        String d1Str = "9223372036854775806.2";
        Decimal d1 = Decimal.fromString(d1Str);

        boolean[] endpoints = new boolean[2];

        /**
         *
         * <pre>
         * endpoints[0]=true  <=>  const < col or const <= col, so const is the left end point,
         * endpoints[0]=false <=>  col < const or col <= const, so const is NOT the left end point,
         *
         * endpoints[1]=true <=> const <= col or col <= const, label if included endpoint
         * endpoints[1]=false <=> const < col or col < const, label if included endpoint
         * </pre>
         *
         * @param enddpoints
         * @return
         */
        endpoints[0] = false; // false: col < d1Str, true: col > d1Str
        endpoints[1] = false; //

        ExecutionContext context = new ExecutionContext();
        SessionProperties sessionProperties = SessionProperties.fromExecutionContext(context);
        df1.store(d1Str, DataTypes.StringType, sessionProperties, endpoints);

        System.out.println(df1.stringValue().toStringUtf8());
        System.out.println(df1.lastStatus().toString());
        System.out.println(df1.lastPredicateBoolean().toString());

        // truncate value
        Assert.assertTrue(df1.lastStatus() == TypeConversionStatus.TYPE_NOTE_TRUNCATED);

        // pred of col col < 9223372036854775806.2
        Assert.assertTrue(df1.lastPredicateBoolean() == PredicateBoolean.IS_NOT_ALWAYS_TRUE_OR_FALSE);

        // # col < 9223372036854775806.2  ==after store==>  col <= 9223372036854775806
        Assert.assertTrue(endpoints[0] == false);
        Assert.assertTrue(endpoints[1] == true);

        Assert.assertTrue(df1.stringValue().toStringUtf8().equals("9223372036854775806"));
    }

    @Test
    public void testIntervalEndPoints4() {
        DecimalType decimalType = new DecimalType(65, 0);
        PartitionField df1 = new DecimalPartitionField(decimalType);

        String d1Str = "92233720368547758060000.2";
        Decimal d1 = Decimal.fromString(d1Str);

        boolean[] endpoints = new boolean[2];

        /**
         *
         * <pre>
         * endpoints[0]=true  <=>  const < col or const <= col, so const is the left end point,
         * endpoints[0]=false <=>  col < const or col <= const, so const is NOT the left end point,
         *
         * endpoints[1]=true <=> const <= col or col <= const, label if included endpoint
         * endpoints[1]=false <=> const < col or col < const, label if included endpoint
         * </pre>
         *
         * @param enddpoints
         * @return
         */
        endpoints[0] = false; // false: col < d1Str, true: col > d1Str
        endpoints[1] = false; //

        ExecutionContext context = new ExecutionContext();
        SessionProperties sessionProperties = SessionProperties.fromExecutionContext(context);
        df1.store(d1Str, DataTypes.StringType, sessionProperties, endpoints);

        System.out.println(df1.stringValue().toStringUtf8());
        System.out.println(df1.lastStatus().toString());
        System.out.println(df1.lastPredicateBoolean().toString());

        // truncate value
        Assert.assertTrue(df1.lastStatus() == TypeConversionStatus.TYPE_NOTE_TRUNCATED);

        // pred of col col < 92233720368547758060000.2
        Assert.assertTrue(df1.lastPredicateBoolean() == PredicateBoolean.IS_NOT_ALWAYS_TRUE_OR_FALSE);

        // # col < 92233720368547758060000.2  ==after store==>  col <= 92233720368547758060000
        Assert.assertTrue(endpoints[0] == false);
        Assert.assertTrue(endpoints[1] == true);

        Assert.assertTrue(df1.stringValue().toStringUtf8().equals("92233720368547758060000"));
    }

    @Test
    public void testIntervalEndPoints3() {
        DecimalType decimalType = new DecimalType(24, 0);
        PartitionField df1 = new DecimalPartitionField(decimalType);

        String d1Str = "1000.2";
        Decimal d1 = Decimal.fromString(d1Str);

        boolean[] endpoints = new boolean[2];

        /**
         *
         * <pre>
         * endpoints[0]=true  <=>  const < col or const <= col, so const is the left end point,
         * endpoints[0]=false <=>  col < const or col <= const, so const is NOT the left end point,
         *
         * endpoints[1]=true <=> const <= col or col <= const, label if included endpoint
         * endpoints[1]=false <=> const < col or col < const, label if included endpoint
         * </pre>
         *
         * @param enddpoints
         * @return
         */
        endpoints[0] = false; // false: col < d1Str, true: col > d1Str
        endpoints[1] = false; //

        ExecutionContext context = new ExecutionContext();
        SessionProperties sessionProperties = SessionProperties.fromExecutionContext(context);
        df1.store(d1Str, DataTypes.StringType, sessionProperties, endpoints);

        System.out.println(df1.stringValue().toStringUtf8());
        System.out.println(df1.lastStatus().toString());
        System.out.println(df1.lastPredicateBoolean().toString());

        // truncate value
        Assert.assertTrue(df1.lastStatus() == TypeConversionStatus.TYPE_NOTE_TRUNCATED);

        // pred of col col < 1000.2
        Assert.assertTrue(df1.lastPredicateBoolean() == PredicateBoolean.IS_NOT_ALWAYS_TRUE_OR_FALSE);

        // # col < 1000.2  ==after store==>  col <= 1000
        Assert.assertTrue(endpoints[0] == false);
        Assert.assertTrue(endpoints[1] == true);

        Assert.assertTrue(df1.stringValue().toStringUtf8().equals("1000"));
    }

    @Test
    public void testSetNull() {
        PartitionField f1 = new DecimalPartitionField(new DecimalType(10, 0));
        f1.setNull();
        Assert.assertTrue(f1.isNull());

        PartitionField f2 = new DecimalPartitionField(new DecimalType(10, 0));
        f2.setNull();
        Assert.assertTrue(f2.isNull());
    }

    @Test
    public void testDecimalScale0FromLong() {
        PartitionField f = new DecimalPartitionField(new DecimalType(10, 0));
        DataType resultType = new LongType();

        Long decLong1 = 999L;
        Long decLong2 = -999L;
        TypeConversionStatus state = f.store(decLong1, resultType);
        Assert.assertTrue(f.longValue() == 999L);
        Assert.assertTrue(f.decimalValue().equals(new Decimal(999L, 0)));
        Assert.assertTrue(state == TypeConversionStatus.TYPE_OK);

        f.reset();
        state = f.store(decLong2, resultType);
        Assert.assertTrue(f.longValue() == -999L);
        Assert.assertTrue(f.decimalValue().equals(new Decimal(-999L, 0)));
        Assert.assertTrue(state == TypeConversionStatus.TYPE_OK);
    }

    @Test
    public void testDecimalScale0FromTiny() {
        PartitionField f = new DecimalPartitionField(new DecimalType(10, 0));
        DataType resultType = new TinyIntType();

        Long decLong1 = 999L;
        Long decLong2 = -999L;
        TypeConversionStatus state = f.store(decLong1, resultType);
        Assert.assertTrue(f.longValue() == 999L);
        Assert.assertTrue(f.decimalValue().equals(new Decimal(999L, 0)));
        Assert.assertTrue(state == TypeConversionStatus.TYPE_OK);

        f.reset();
        state = f.store(decLong2, resultType);
        Assert.assertTrue(f.longValue() == -999L);
        Assert.assertTrue(f.decimalValue().equals(new Decimal(-999L, 0)));
        Assert.assertTrue(state == TypeConversionStatus.TYPE_OK);
    }

    @Test
    public void testDecimalScale0FromDouble() {
        PartitionField f = new DecimalPartitionField(new DecimalType(10, 0));
        DataType resultType = new DoubleType();

        Double decDouble1 = (double) 999;
        Double decDouble2 = (double) -999;
        TypeConversionStatus state = f.store(decDouble1, resultType);
        Assert.assertTrue(f.doubleValue() == 999);
        Assert.assertTrue(f.decimalValue().equals(new Decimal(999, 0)));
        Assert.assertTrue(state == TypeConversionStatus.TYPE_NOTE_TRUNCATED);

        f.reset();
        state = f.store(decDouble2, resultType);
        Assert.assertTrue(f.doubleValue() == -999);
        Assert.assertTrue(f.decimalValue().equals(new Decimal(-999, 0)));
        Assert.assertTrue(state == TypeConversionStatus.TYPE_NOTE_TRUNCATED);
    }

    @Test
    public void testDecimalScale2FromDouble() {
        PartitionField f = new DecimalPartitionField(new DecimalType(10, 2));
        DataType resultType = new DoubleType();

        Double decDouble1 = 999.88;
        Double decDouble2 = -999.88;
        TypeConversionStatus state = f.store(decDouble1, resultType);
        Assert.assertTrue(f.doubleValue() == 999.88);
        Assert.assertTrue(f.decimalValue().equals(new Decimal(99988, 2)));
        Assert.assertTrue(state == TypeConversionStatus.TYPE_OK);

        f.reset();
        state = f.store(decDouble2, resultType);
        Assert.assertTrue(f.doubleValue() == -999.88);
        Assert.assertTrue(f.decimalValue().equals(new Decimal(-99988, 2)));
        Assert.assertTrue(state == TypeConversionStatus.TYPE_OK);
    }

    @Test
    public void testDecimalScale0FromDecimal() {
        PartitionField f = new DecimalPartitionField(new DecimalType(10, 0));
        DataType resultType = new DecimalType();

        Decimal decDecimal1 = new Decimal(999, 0);
        Decimal decDecimal2 = new Decimal(-999, 0);
        TypeConversionStatus state = f.store(decDecimal1, resultType);
        Assert.assertTrue(f.decimalValue().equals(new Decimal(999, 0)));
        Assert.assertTrue(state == TypeConversionStatus.TYPE_OK);

        f.reset();
        state = f.store(decDecimal2, resultType);
        Assert.assertTrue(f.decimalValue().equals(new Decimal(-999, 0)));
        Assert.assertTrue(state == TypeConversionStatus.TYPE_OK);
    }

    @Test
    public void testDecimalScale2FromDecimal() {
        PartitionField f = new DecimalPartitionField(new DecimalType(10, 2));
        DataType resultType = new DoubleType();

        Decimal decDecimal1 = new Decimal(99988, 2);
        Decimal decDecimal2 = new Decimal(-99988, 2);
        TypeConversionStatus state = f.store(decDecimal1, resultType);
        Assert.assertTrue(f.decimalValue().equals(new Decimal(99988, 2)));
        Assert.assertTrue(state == TypeConversionStatus.TYPE_OK);

        f.reset();
        state = f.store(decDecimal2, resultType);
        Assert.assertTrue(f.decimalValue().equals(new Decimal(-99988, 2)));
        Assert.assertTrue(state == TypeConversionStatus.TYPE_OK);
    }

    @Test
    public void testDecimalScale0FromBigDecimal() {
        PartitionField f = new DecimalPartitionField(new DecimalType(10, 0));
        DataType resultType = new DecimalType();

        BigDecimal decDecimal1 = BigDecimal.valueOf(999);
        BigDecimal decDecimal2 = BigDecimal.valueOf(-999);
        TypeConversionStatus state = f.store(decDecimal1, resultType);
        Assert.assertTrue(f.decimalValue().equals(new Decimal(999, 0)));
        Assert.assertTrue(state == TypeConversionStatus.TYPE_OK);

        f.reset();
        state = f.store(decDecimal2, resultType);
        Assert.assertTrue(f.decimalValue().equals(new Decimal(-999, 0)));
        Assert.assertTrue(state == TypeConversionStatus.TYPE_OK);
    }

    @Test
    public void testDecimalScale2FromBigDecimal() {
        PartitionField f = new DecimalPartitionField(new DecimalType(10, 2));
        DataType resultType = new DoubleType();

        BigDecimal decDecimal1 = BigDecimal.valueOf(999.88);
        BigDecimal decDecimal2 = BigDecimal.valueOf(-999.88);
        TypeConversionStatus state = f.store(decDecimal1, resultType);
        Assert.assertTrue(f.decimalValue().equals(new Decimal(99988, 2)));
        Assert.assertTrue(state == TypeConversionStatus.TYPE_OK);

        f.reset();
        state = f.store(decDecimal2, resultType);
        Assert.assertTrue(f.decimalValue().equals(new Decimal(-99988, 2)));
        Assert.assertTrue(state == TypeConversionStatus.TYPE_OK);
    }

    @Test
    public void testDecimalScale0FromString() {
        PartitionField f = new DecimalPartitionField(new DecimalType(10, 0));
        DataType resultType = new StringType();

        String decStr1 = "999";
        TypeConversionStatus state = f.store(decStr1, resultType);
        Assert.assertTrue(f.stringValue().toStringUtf8().equals("999"));
        Assert.assertTrue(state == TypeConversionStatus.TYPE_OK);

        String decStr2 = "-999";
        f.reset();
        state = f.store(decStr2, resultType);
        Assert.assertTrue(f.stringValue().toStringUtf8().equals("-999"));
        Assert.assertTrue(state == TypeConversionStatus.TYPE_OK);
    }

    @Test
    public void testDecimalBigString() {
        PartitionField f = new DecimalPartitionField(new DecimalType(24, 0));
        DataType resultType = new StringType();

        String decStr1 = "1234567890123456789012340";
        TypeConversionStatus state = f.store(decStr1, resultType);
        Assert.assertTrue(f.stringValue().toStringUtf8().equals("999999999999999999999999"));
        Assert.assertTrue(state == TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE);

        f.reset();

        String decStr2 = "-1234567890123456789012340";
        state = f.store(decStr2, resultType);
        Assert.assertTrue(f.stringValue().toStringUtf8().equals("-999999999999999999999999"));
        Assert.assertTrue(state == TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE);
    }

    @Test
    public void testDecimalScale2FromString() {
        PartitionField f = new DecimalPartitionField(new DecimalType(10, 2));
        DataType resultType = new StringType();

        String decStr1 = "999.33";
        String decStr2 = "-999.88";

        TypeConversionStatus state = f.store(decStr1, resultType);
        Assert.assertTrue(f.stringValue().toStringUtf8().equals("999.33"));
        Assert.assertTrue(state == TypeConversionStatus.TYPE_OK);

        f.reset();
        state = f.store(decStr2, resultType);
        Assert.assertTrue(f.stringValue().toStringUtf8().equals("-999.88"));
        Assert.assertTrue(state == TypeConversionStatus.TYPE_OK);
    }

    @Test
    public void testLongLT() {
        PartitionField f = new DecimalPartitionField(new DecimalType(10, 2));
        boolean[] endPoints = {false, false}; // <

        // field < 10
        TypeConversionStatus state = f.store(10, new LongType(), SessionProperties.empty(), endPoints);
        Assert.assertTrue(f.decimalValue().equals(new Decimal(10, 0)));
        Assert.assertTrue(endPoints[1] == false);
        Assert.assertTrue(state == TypeConversionStatus.TYPE_OK);
    }

    @Test
    public void testLongLE() {
        PartitionField f = new DecimalPartitionField(new DecimalType(10, 2));
        boolean[] endPoints = {false, true}; //<=

        // field <= 10
        TypeConversionStatus state = f.store(10, new LongType(), SessionProperties.empty(), endPoints);
        Assert.assertTrue(f.decimalValue().equals(new Decimal(10, 0)));
        Assert.assertTrue(endPoints[1] == true);
        Assert.assertTrue(state == TypeConversionStatus.TYPE_OK);
    }

    @Test
    public void testLongGT() {
        PartitionField f = new DecimalPartitionField(new DecimalType(10, 2));
        boolean[] endPoints = {true, false}; // >

        // field > 10
        TypeConversionStatus state = f.store(10, new LongType(), SessionProperties.empty(), endPoints);
        Assert.assertTrue(f.decimalValue().equals(new Decimal(10, 0)));
        Assert.assertTrue(endPoints[1] == false);
        Assert.assertTrue(state == TypeConversionStatus.TYPE_OK);
    }

    @Test
    public void testLongGE() {
        PartitionField f = new DecimalPartitionField(new DecimalType(10, 2));
        boolean[] endPoints = {true, true}; // >=

        // field >= 10 => field >= 10
        TypeConversionStatus state = f.store(10, new LongType(), SessionProperties.empty(), endPoints);
        Assert.assertTrue(f.decimalValue().equals(new Decimal(10, 0)));
        Assert.assertTrue(endPoints[1] == true);
        Assert.assertTrue(state == TypeConversionStatus.TYPE_OK);
    }

    @Test
    public void testDoubleLTUpper() {
        PartitionField f = new DecimalPartitionField(new DecimalType(10, 1));
        boolean[] endPoints = {false, false}; // <

        // field < 10.55 => field <= 10.6
        TypeConversionStatus state = f.store(10.55, new DoubleType(), SessionProperties.empty(), endPoints);
        Assert.assertTrue(f.decimalValue().equals(new Decimal(106, 1)));
        Assert.assertTrue(endPoints[1] == true);
        Assert.assertTrue(state == TypeConversionStatus.TYPE_NOTE_TRUNCATED);
    }

    @Test
    public void testDoubleLEUpper() {
        PartitionField f = new DecimalPartitionField(new DecimalType(10, 1));
        boolean[] endPoints = {false, true}; //<=

        // field <= 10.55 => field <= 10.6
        TypeConversionStatus state = f.store(10.55, new DoubleType(), SessionProperties.empty(), endPoints);
        Assert.assertTrue(f.decimalValue().equals(new Decimal(106, 1)));
        Assert.assertTrue(endPoints[1] == true);
        Assert.assertTrue(state == TypeConversionStatus.TYPE_NOTE_TRUNCATED);
    }

    @Test
    public void testDoubleGTUpper() {
        PartitionField f = new DecimalPartitionField(new DecimalType(10, 1));
        boolean[] endPoints = {true, false}; // >

        // field > 10.55 => field >= 10.6
        TypeConversionStatus state = f.store(10.55, new DoubleType(), SessionProperties.empty(), endPoints);
        Assert.assertTrue(f.decimalValue().equals(new Decimal(106, 1)));
        Assert.assertTrue(endPoints[1] == true);
        Assert.assertTrue(state == TypeConversionStatus.TYPE_NOTE_TRUNCATED);
    }

    @Test
    public void testDoubleGEUpper() {
        PartitionField f = new DecimalPartitionField(new DecimalType(10, 1));
        boolean[] endPoints = {true, true}; // >=

        // field >= 10.55 => field >= 10.6
        TypeConversionStatus state = f.store(10.55, new DoubleType(), SessionProperties.empty(), endPoints);
        Assert.assertTrue(f.decimalValue().equals(new Decimal(106, 1)));
        Assert.assertTrue(endPoints[1] == true);
        Assert.assertTrue(state == TypeConversionStatus.TYPE_NOTE_TRUNCATED);
    }

    @Test
    public void testDoubleLTLowerScale0() {
        PartitionField f = new DecimalPartitionField(new DecimalType(10, 0));
        boolean[] endPoints = {false, false}; // <

        // field < 999 => field < 999
        TypeConversionStatus state = f.store((double) 999, new DoubleType(), SessionProperties.empty(), endPoints);
        Assert.assertTrue(f.doubleValue() == (double) 999);
        Assert.assertTrue(endPoints[1] == false);
        Assert.assertTrue(state == TypeConversionStatus.TYPE_NOTE_TRUNCATED);
    }

    @Test
    public void testDoubleLELowerScale0() {
        PartitionField f = new DecimalPartitionField(new DecimalType(10, 0));
        boolean[] endPoints = {false, true}; //<=

        // field <= 999 => field <= 999
        TypeConversionStatus state = f.store((double) 999, new DoubleType(), SessionProperties.empty(), endPoints);
        Assert.assertTrue(f.doubleValue() == (double) 999);
        Assert.assertTrue(endPoints[1] == true);
        Assert.assertTrue(state == TypeConversionStatus.TYPE_NOTE_TRUNCATED);
    }

    @Test
    public void testDoubleGTLowerScale0() {
        PartitionField f = new DecimalPartitionField(new DecimalType(10, 0));
        boolean[] endPoints = {true, false}; // >

        // field > 999 => field > 999
        TypeConversionStatus state = f.store((double) 999, new DoubleType(), SessionProperties.empty(), endPoints);
        Assert.assertTrue(f.doubleValue() == (double) 999);
        Assert.assertTrue(endPoints[1] == false);
        Assert.assertTrue(state == TypeConversionStatus.TYPE_NOTE_TRUNCATED);
    }

    @Test
    public void testDoubleGELowerScale0() {
        PartitionField f = new DecimalPartitionField(new DecimalType(10, 0));
        boolean[] endPoints = {true, true}; // >=

        // field >= 999 => field >= 999
        TypeConversionStatus state = f.store((double) 999, new DoubleType(), SessionProperties.empty(), endPoints);
        Assert.assertTrue(f.doubleValue() == (double) 999);
        Assert.assertTrue(endPoints[1] == true);
        Assert.assertTrue(state == TypeConversionStatus.TYPE_NOTE_TRUNCATED);
    }

    @Test
    public void testDoubleLTLower() {
        PartitionField f = new DecimalPartitionField(new DecimalType(10, 1));
        boolean[] endPoints = {false, false}; // <

        // field < 10.33 => field <= 10.3
        TypeConversionStatus state = f.store(10.33, new DoubleType(), SessionProperties.empty(), endPoints);
        Assert.assertTrue(f.decimalValue().equals(new Decimal(103, 1)));
        Assert.assertTrue(endPoints[1] == true);
        Assert.assertTrue(state == TypeConversionStatus.TYPE_NOTE_TRUNCATED);
    }

    @Test
    public void testDoubleLELower() {
        PartitionField f = new DecimalPartitionField(new DecimalType(10, 1));
        boolean[] endPoints = {false, true}; //<=

        // field <= 10.33 => field <= 10.3
        TypeConversionStatus state = f.store(10.33, new DoubleType(), SessionProperties.empty(), endPoints);
        Assert.assertTrue(f.decimalValue().equals(new Decimal(103, 1)));
        Assert.assertTrue(endPoints[1] == true);
        Assert.assertTrue(state == TypeConversionStatus.TYPE_NOTE_TRUNCATED);
    }

    @Test
    public void testDoubleGTLower() {
        PartitionField f = new DecimalPartitionField(new DecimalType(10, 1));
        boolean[] endPoints = {true, false}; // >

        // field > 10.33 => field > 10.3
        TypeConversionStatus state = f.store(10.33, new DoubleType(), SessionProperties.empty(), endPoints);
        Assert.assertTrue(f.decimalValue().equals(new Decimal(103, 1)));
        Assert.assertTrue(endPoints[1] == false);
        Assert.assertTrue(state == TypeConversionStatus.TYPE_NOTE_TRUNCATED);
    }

    @Test
    public void testDoubleGELower() {
        PartitionField f = new DecimalPartitionField(new DecimalType(10, 1));
        boolean[] endPoints = {true, true}; // >=

        // field >= 10.33 => field > 10.3
        TypeConversionStatus state = f.store(10.33, new DoubleType(), SessionProperties.empty(), endPoints);
        Assert.assertTrue(f.decimalValue().equals(new Decimal(103, 1)));
        Assert.assertTrue(endPoints[1] == false);
        Assert.assertTrue(state == TypeConversionStatus.TYPE_NOTE_TRUNCATED);
    }

    @Test
    public void testVarcharLTUpper() {
        PartitionField f = new DecimalPartitionField(new DecimalType(10, 1));
        boolean[] endPoints = {false, false}; // <

        // field < 10.55 => field <= 10.6
        TypeConversionStatus state = f.store("10.55", new VarcharType(), SessionProperties.empty(), endPoints);
        Assert.assertTrue(f.decimalValue().equals(new Decimal(106, 1)));
        Assert.assertTrue(endPoints[1] == true);
        Assert.assertTrue(state == TypeConversionStatus.TYPE_NOTE_TRUNCATED);
    }

    @Test
    public void testVarcharLEUpper() {
        PartitionField f = new DecimalPartitionField(new DecimalType(10, 1));
        boolean[] endPoints = {false, true}; //<=

        // field <= 10.55 => field <= 10.6
        TypeConversionStatus state = f.store("10.55", new VarcharType(), SessionProperties.empty(), endPoints);
        Assert.assertTrue(f.decimalValue().equals(new Decimal(106, 1)));
        Assert.assertTrue(endPoints[1] == true);
        Assert.assertTrue(state == TypeConversionStatus.TYPE_NOTE_TRUNCATED);
    }

    @Test
    public void testVarcharGTUpper() {
        PartitionField f = new DecimalPartitionField(new DecimalType(10, 1));
        boolean[] endPoints = {true, false}; // >

        // field > 10.55 => field >= 10.6
        TypeConversionStatus state = f.store("10.55", new VarcharType(), SessionProperties.empty(), endPoints);
        Assert.assertTrue(f.decimalValue().equals(new Decimal(106, 1)));
        Assert.assertTrue(endPoints[1] == true);
        Assert.assertTrue(state == TypeConversionStatus.TYPE_NOTE_TRUNCATED);
    }

    @Test
    public void testVarcharGEUpper() {
        PartitionField f = new DecimalPartitionField(new DecimalType(10, 1));
        boolean[] endPoints = {true, true}; // >=

        // field >= 10.55 => field > 10.6
        TypeConversionStatus state = f.store("10.55", new VarcharType(), SessionProperties.empty(), endPoints);
        Assert.assertTrue(f.decimalValue().equals(new Decimal(106, 1)));
        Assert.assertTrue(endPoints[1] == true);
        Assert.assertTrue(state == TypeConversionStatus.TYPE_NOTE_TRUNCATED);
    }

    @Test
    public void testVarcharLTLower() {
        PartitionField f = new DecimalPartitionField(new DecimalType(10, 1));
        boolean[] endPoints = {false, false}; // <

        // field < 10.33 => field <= 10.3
        TypeConversionStatus state = f.store("10.33", new VarcharType(), SessionProperties.empty(), endPoints);
        Assert.assertTrue(f.decimalValue().equals(new Decimal(103, 1)));
        Assert.assertTrue(endPoints[1] == true);
        Assert.assertTrue(state == TypeConversionStatus.TYPE_NOTE_TRUNCATED);
    }

    @Test
    public void testVarcharLELower() {
        PartitionField f = new DecimalPartitionField(new DecimalType(10, 1));
        boolean[] endPoints = {false, true}; //<=

        // field <= 10.33 => field <= 10.3
        TypeConversionStatus state = f.store("10.33", new VarcharType(), SessionProperties.empty(), endPoints);
        Assert.assertTrue(f.decimalValue().equals(new Decimal(103, 1)));
        Assert.assertTrue(endPoints[1] == true);
        Assert.assertTrue(state == TypeConversionStatus.TYPE_NOTE_TRUNCATED);
    }

    @Test
    public void testVarcharGTLower() {
        PartitionField f = new DecimalPartitionField(new DecimalType(10, 1));
        boolean[] endPoints = {true, false}; // >

        // field > 10.33 => field > 10.3
        TypeConversionStatus state = f.store("10.33", new VarcharType(), SessionProperties.empty(), endPoints);
        Assert.assertTrue(f.decimalValue().equals(new Decimal(103, 1)));
        Assert.assertTrue(endPoints[1] == false);
        Assert.assertTrue(state == TypeConversionStatus.TYPE_NOTE_TRUNCATED);
    }

    @Test
    public void testVarcharGELower() {
        PartitionField f = new DecimalPartitionField(new DecimalType(10, 1));
        boolean[] endPoints = {true, true}; // >=

        // field >= 10.33 => field > 10.3
        TypeConversionStatus state = f.store("10.33", new VarcharType(), SessionProperties.empty(), endPoints);
        Assert.assertTrue(f.decimalValue().equals(new Decimal(103, 1)));
        Assert.assertTrue(endPoints[1] == false);
        Assert.assertTrue(state == TypeConversionStatus.TYPE_NOTE_TRUNCATED);
    }

    @Test
    public void testTruncated() {
        PartitionField f = new DecimalPartitionField(new DecimalType(10, 3));
        TypeConversionStatus state = f.store(10.99999, new DoubleType());
        Assert.assertTrue(f.decimalValue().equals(new Decimal(11000, 3)));
        Assert.assertTrue(state == TypeConversionStatus.TYPE_NOTE_TRUNCATED);
    }

    @Test
    public void testOverflowVarchar() {
        PartitionField f = new DecimalPartitionField(new DecimalType(20, 0));
        TypeConversionStatus state = f.store("123456789012345678900", new VarcharType());
        Assert.assertTrue(state == TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE);
    }

    @Test
    public void testOverflowDouble() {
        PartitionField f = new DecimalPartitionField(new DecimalType(5, 0));
        TypeConversionStatus state = f.store(123456789.0, new DoubleType());
        Assert.assertTrue(state == TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE);
    }

    @Test
    public void testOverflowLong() {
        PartitionField f = new DecimalPartitionField(new DecimalType(10, 0));
        TypeConversionStatus state = f.store(123456789012345678L, new LongType());
        Assert.assertTrue(state == TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE);
    }

    @Test
    public void testDecimalBound() {
        PartitionField f = new DecimalPartitionField(new DecimalType(2, 2));
        DataType resultType = new DecimalType();
        Decimal decDecimal1 = new Decimal(99, 2);
        TypeConversionStatus state = f.store(decDecimal1, resultType);
        Assert.assertTrue(f.decimalValue().equals(new Decimal(99, 2)));
        Assert.assertTrue(state == TypeConversionStatus.TYPE_OK);
    }

    @Test
    public void testMaxDecimalValue() {
        for (int precision = 1; precision <= MAX_DECIMAL_PRECISION; precision++) {
            for (int scale = 0; scale <= Math.min(precision, MAX_DECIMAL_SCALE); scale++) {
                PartitionField f = new DecimalPartitionField(new DecimalType(precision, scale));
                TypeConversionStatus state =
                    f.store(new Decimal(DecimalBounds.maxValue(precision, scale)), new DecimalType());
                Assert.assertTrue(state == TypeConversionStatus.TYPE_OK);
                Assert.assertTrue(f.decimalValue().equals(new Decimal(DecimalBounds.maxValue(precision, scale))));
            }
        }
        for (int precision = 1; precision <= MAX_DECIMAL_PRECISION; precision++) {
            for (int scale = 0; scale <= Math.min(precision, MAX_DECIMAL_SCALE); scale++) {
                PartitionField f = new DecimalPartitionField(new DecimalType(precision, scale));
                TypeConversionStatus state =
                    f.store(new Decimal(DecimalBounds.minValue(precision, scale)), new DecimalType());
                Assert.assertTrue(state == TypeConversionStatus.TYPE_OK);
                Assert.assertTrue(f.decimalValue().equals(new Decimal(DecimalBounds.minValue(precision, scale))));
            }
        }
    }

}

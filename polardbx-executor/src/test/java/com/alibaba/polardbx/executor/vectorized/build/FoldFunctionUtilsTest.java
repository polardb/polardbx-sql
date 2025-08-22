package com.alibaba.polardbx.executor.vectorized.build;

import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.TddlTypeFactoryImpl;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

// test blacklist

// test whitelist

// test dynamic
public class FoldFunctionUtilsTest {
    private final static RelDataTypeFactory TYPE_FACTORY =
        new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());
    private final static RexBuilder REX_BUILDER = new RexBuilder(TYPE_FACTORY);

    // This method tests the behavior of SQL operators against a blacklist.
    private void doTestBlackList(SqlOperator operator) {
        List<RexNode> operands;

        // Check if the operator is a binary operator
        if (operator instanceof SqlBinaryOperator) {
            // Create operands with literals "1" and "2"
            operands = ImmutableList.of(REX_BUILDER.makeLiteral("1"), REX_BUILDER.makeLiteral("2"));
        }
        // Check if the operator is EXISTS or NOT_EXISTS
        else if (operator == TddlOperatorTable.EXISTS || operator == TddlOperatorTable.NOT_EXISTS) {
            // No operands for EXISTS or NOT_EXISTS
            operands = ImmutableList.of();
        }
        // For other cases, create a single operand with literal "1"
        else {
            operands = ImmutableList.of(REX_BUILDER.makeLiteral("1"));
        }

        // Create a RexCall with the specified operator and operands
        RexCall call = (RexCall) REX_BUILDER.makeCall(
            TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT), operator, operands);

        // Check if folding is enabled for the call
        boolean isEnabled = FoldFunctionUtils.isFoldEnabled(call, null, null);

        // Assert that folding is not enabled for the blacklist test
        Assert.assertFalse(isEnabled);
    }

    // This method tests the behavior of SQL operators against a whitelist.
    private void doTestWhiteList(SqlOperator operator) {
        List<RexNode> operands;

        // Check if the operator is a binary operator
        if (operator instanceof SqlBinaryOperator) {
            // Create operands with literals "1" and "2"
            operands = ImmutableList.of(REX_BUILDER.makeLiteral("1"), REX_BUILDER.makeLiteral("2"));
        }
        // Check if the operator is EXISTS or NOT_EXISTS
        else if (operator == TddlOperatorTable.EXISTS || operator == TddlOperatorTable.NOT_EXISTS) {
            // No operands for EXISTS or NOT_EXISTS
            operands = ImmutableList.of();
        }
        // For other cases, create a single operand with literal "1"
        else {
            operands = ImmutableList.of(REX_BUILDER.makeLiteral("1"));
        }

        // Create a RexCall with the specified operator and operands
        RexCall call = (RexCall) REX_BUILDER.makeCall(
            TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT), operator, operands);

        // Check if folding is enabled for the call
        boolean isEnabled = FoldFunctionUtils.isFoldEnabled(call, null, null);

        // Assert that folding is enabled for the whitelist test
        Assert.assertTrue(isEnabled);
    }

    // This method tests the behavior of SQL operators against a blacklist.
    private void doTestDynamic(SqlOperator operator) {
        List<RexNode> operands;

        // Check if the operator is a binary operator
        if (operator instanceof SqlBinaryOperator) {
            // Create operands with literals "1" and "2"
            operands = ImmutableList.of(REX_BUILDER.makeLiteral("1"), REX_BUILDER.makeLiteral("2"));
        }
        // Check if the operator is EXISTS or NOT_EXISTS
        else if (operator == TddlOperatorTable.EXISTS || operator == TddlOperatorTable.NOT_EXISTS) {
            // No operands for EXISTS or NOT_EXISTS
            operands = ImmutableList.of();
        }
        // For other cases, create a single operand with literal "1"
        else {
            operands = ImmutableList.of(REX_BUILDER.makeLiteral("1"));
        }

        // Create a RexCall with the specified operator and operands
        RexCall call = (RexCall) REX_BUILDER.makeCall(
            TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT), operator, operands);

        // Check if folding is enabled for the call
        boolean isEnabled = FoldFunctionUtils.isFoldEnabled(call, null, null);

        // Assert that folding is not enabled for the blacklist test
        Assert.assertFalse(isEnabled);
    }

    @Test
    public void testBlackList() {
        doTestBlackList(TddlOperatorTable.PART_ROUTE);
        // Non-mysql Functions
        doTestBlackList(TddlOperatorTable.PART_ROUTE);

        // Information Functions
        doTestBlackList(TddlOperatorTable.CONNECTION_ID);
        doTestBlackList(TddlOperatorTable.CURRENT_ROLE);
        doTestBlackList(TddlOperatorTable.CURRENT_USER);
        doTestBlackList(TddlOperatorTable.DATABASE);
        doTestBlackList(TddlOperatorTable.FOUND_ROWS);
        doTestBlackList(TddlOperatorTable.ROW_COUNT);
        doTestBlackList(TddlOperatorTable.SCHEMA);
        doTestBlackList(TddlOperatorTable.SESSION_USER);
        doTestBlackList(TddlOperatorTable.SYSTEM_USER);
        doTestBlackList(TddlOperatorTable.USER);
        doTestBlackList(TddlOperatorTable.VERSION);
        doTestBlackList(TddlOperatorTable.TSO_TIMESTAMP);
        doTestBlackList(TddlOperatorTable.SPECIAL_POW);
        doTestBlackList(TddlOperatorTable.CAN_ACCESS_TABLE);
        doTestBlackList(TddlOperatorTable.GET_LOCK);
        doTestBlackList(TddlOperatorTable.RELEASE_LOCK);
        doTestBlackList(TddlOperatorTable.RELEASE_ALL_LOCKS);
        doTestBlackList(TddlOperatorTable.IS_FREE_LOCK);
        doTestBlackList(TddlOperatorTable.IS_USED_LOCK);
        doTestBlackList(TddlOperatorTable.HYPERLOGLOG);
        doTestBlackList(TddlOperatorTable.PART_HASH);
        doTestBlackList(TddlOperatorTable.LBAC_CHECK);
        doTestBlackList(TddlOperatorTable.LBAC_READ);
        doTestBlackList(TddlOperatorTable.LBAC_WRITE);
        doTestBlackList(TddlOperatorTable.LBAC_WRITE_STRICT_CHECK);
        doTestBlackList(TddlOperatorTable.LBAC_USER_WRITE_LABEL);

        // Time Function
        doTestBlackList(TddlOperatorTable.CURDATE);
        doTestBlackList(TddlOperatorTable.CURRENT_DATE);
        doTestBlackList(TddlOperatorTable.CURRENT_TIME);
        doTestBlackList(TddlOperatorTable.CURRENT_TIMESTAMP);
        doTestBlackList(TddlOperatorTable.CURTIME);
        doTestBlackList(TddlOperatorTable.LOCALTIME);
        doTestBlackList(TddlOperatorTable.LOCALTIMESTAMP);
        doTestBlackList(TddlOperatorTable.NOW);
        doTestBlackList(TddlOperatorTable.SYSDATE);
        doTestBlackList(TddlOperatorTable.UNIX_TIMESTAMP);
        doTestBlackList(TddlOperatorTable.UTC_DATE);
        doTestBlackList(TddlOperatorTable.UTC_TIME);
        doTestBlackList(TddlOperatorTable.TSO_TIMESTAMP);

        // Numeric Functions
        doTestBlackList(TddlOperatorTable.RAND);

        // Miscellaneous Functions
        doTestBlackList(TddlOperatorTable.UUID);
        doTestBlackList(TddlOperatorTable.UUID_SHORT);

        // Information Functions
        doTestBlackList(TddlOperatorTable.CONNECTION_ID);
        doTestBlackList(TddlOperatorTable.CURRENT_ROLE);
        doTestBlackList(TddlOperatorTable.CURRENT_USER);
        doTestBlackList(TddlOperatorTable.DATABASE);
        doTestBlackList(TddlOperatorTable.FOUND_ROWS);
        doTestBlackList(TddlOperatorTable.LAST_INSERT_ID);
        doTestBlackList(TddlOperatorTable.ROW_COUNT);
        doTestBlackList(TddlOperatorTable.SCHEMA);
        doTestBlackList(TddlOperatorTable.SESSION_USER);
        doTestBlackList(TddlOperatorTable.SYSTEM_USER);
        doTestBlackList(TddlOperatorTable.USER);
        doTestBlackList(TddlOperatorTable.VERSION);

        // Locking function
        doTestBlackList(TddlOperatorTable.GET_LOCK);
        doTestBlackList(TddlOperatorTable.IS_FREE_LOCK);
        doTestBlackList(TddlOperatorTable.IS_USED_LOCK);
        doTestBlackList(TddlOperatorTable.RELEASE_ALL_LOCKS);
        doTestBlackList(TddlOperatorTable.RELEASE_LOCK);

        // Benchmark function
        doTestBlackList(TddlOperatorTable.BENCHMARK);

        // cast function
        doTestBlackList(TddlOperatorTable.CAST);
        doTestBlackList(TddlOperatorTable.CONVERT);
        doTestBlackList(TddlOperatorTable.IMPLICIT_CAST);

        // all functions
        doTestBlackList(TddlOperatorTable.BITWISE_AND);
        doTestBlackList(TddlOperatorTable.BITRSHIFT);

        doTestBlackList(TddlOperatorTable.BITLSHIFT);

        doTestBlackList(TddlOperatorTable.JSON_EXTRACT);
        doTestBlackList(TddlOperatorTable.JSON_UNQUOTE);

        doTestBlackList(TddlOperatorTable.ASSIGNMENT);
        doTestBlackList(TddlOperatorTable.BITWISE_XOR);

        doTestBlackList(TddlOperatorTable.ACOS);

        doTestBlackList(TddlOperatorTable.AES_DECRYPT);
        doTestBlackList(TddlOperatorTable.AES_ENCRYPT);
        doTestBlackList(TddlOperatorTable.BITWISE_AND);

        doTestBlackList(TddlOperatorTable.ASCII);
        doTestBlackList(TddlOperatorTable.ASIN);
        doTestBlackList(TddlOperatorTable.ATAN);
        doTestBlackList(TddlOperatorTable.ATAN2);
        doTestBlackList(TddlOperatorTable.AVG);
        doTestBlackList(TddlOperatorTable.BENCHMARK);

        doTestBlackList(TddlOperatorTable.BIN);
        doTestBlackList(TddlOperatorTable.BIN_TO_UUID);
        doTestBlackList(TddlOperatorTable.BINARY);
        doTestBlackList(TddlOperatorTable.BIT_AND);
        doTestBlackList(TddlOperatorTable.BIT_COUNT);
        doTestBlackList(TddlOperatorTable.BIT_LENGTH);
        doTestBlackList(TddlOperatorTable.BIT_OR);
        doTestBlackList(TddlOperatorTable.BIT_XOR);

        doTestBlackList(TddlOperatorTable.CAST);

        doTestBlackList(TddlOperatorTable.CHAR);
        doTestBlackList(TddlOperatorTable.CHAR_LENGTH);
        doTestBlackList(TddlOperatorTable.CHARACTER_LENGTH);
        doTestBlackList(TddlOperatorTable.CHARSET);

        doTestBlackList(TddlOperatorTable.COLLATION);
        doTestBlackList(TddlOperatorTable.COMPRESS);

        doTestBlackList(TddlOperatorTable.CONCAT_WS);
        doTestBlackList(TddlOperatorTable.CONNECTION_ID);
        doTestBlackList(TddlOperatorTable.CONV);
        doTestBlackList(TddlOperatorTable.CONVERT);
        doTestBlackList(TddlOperatorTable.CONVERT_TZ);
        doTestBlackList(TddlOperatorTable.COS);
        doTestBlackList(TddlOperatorTable.COT);
        doTestBlackList(TddlOperatorTable.COUNT);
        doTestBlackList(TddlOperatorTable.CRC32);
        doTestBlackList(TddlOperatorTable.CUME_DIST);
        doTestBlackList(TddlOperatorTable.CURDATE);

        doTestBlackList(TddlOperatorTable.CURRENT_DATE);
        doTestBlackList(TddlOperatorTable.CURRENT_ROLE);
        doTestBlackList(TddlOperatorTable.CURRENT_TIME);
        doTestBlackList(TddlOperatorTable.CURRENT_TIMESTAMP);
        doTestBlackList(TddlOperatorTable.CURRENT_USER);
        doTestBlackList(TddlOperatorTable.CURTIME);

        doTestBlackList(TddlOperatorTable.DATABASE);

        doTestBlackList(TddlOperatorTable.DEFAULT);
        doTestBlackList(TddlOperatorTable.DEGREES);
        doTestBlackList(TddlOperatorTable.DENSE_RANK);

        doTestBlackList(TddlOperatorTable.ELT);
        doTestBlackList(TddlOperatorTable.EXISTS);
        doTestBlackList(TddlOperatorTable.EXP);
        doTestBlackList(TddlOperatorTable.EXPORTSET);

        doTestBlackList(TddlOperatorTable.FIELD);
        doTestBlackList(TddlOperatorTable.FIND_IN_SET);
        doTestBlackList(TddlOperatorTable.FIRST_VALUE);
        doTestBlackList(TddlOperatorTable.FORMAT);

        doTestBlackList(TddlOperatorTable.FOUND_ROWS);
        doTestBlackList(TddlOperatorTable.FROM_UNIXTIME);
        doTestBlackList(TddlOperatorTable.GET_LOCK);
        doTestBlackList(TddlOperatorTable.GREATEST);
        doTestBlackList(TddlOperatorTable.GROUP_CONCAT);
        doTestBlackList(TddlOperatorTable.HEX);
        doTestBlackList(TddlOperatorTable.HOUR);

        doTestBlackList(TddlOperatorTable.INSERT);
        doTestBlackList(TddlOperatorTable.INSTR);
        doTestBlackList(TddlOperatorTable.INTERVAL);

        doTestBlackList(TddlOperatorTable.IS_FREE_LOCK);

        doTestBlackList(TddlOperatorTable.IS_USED_LOCK);

        doTestBlackList(TddlOperatorTable.JSON_ARRAY);
        doTestBlackList(TddlOperatorTable.JSON_ARRAY_APPEND);
        doTestBlackList(TddlOperatorTable.JSON_ARRAY_INSERT);
        doTestBlackList(TddlOperatorTable.JSON_ARRAYAGG);
        doTestBlackList(TddlOperatorTable.JSON_CONTAINS);
        doTestBlackList(TddlOperatorTable.JSON_CONTAINS_PATH);
        doTestBlackList(TddlOperatorTable.JSON_DEPTH);
        doTestBlackList(TddlOperatorTable.JSON_EXTRACT);
        doTestBlackList(TddlOperatorTable.JSON_INSERT);
        doTestBlackList(TddlOperatorTable.JSON_KEYS);
        doTestBlackList(TddlOperatorTable.JSON_LENGTH);
        doTestBlackList(TddlOperatorTable.JSON_MERGE);
        doTestBlackList(TddlOperatorTable.JSON_MERGE_PATCH);
        doTestBlackList(TddlOperatorTable.JSON_MERGE_PRESERVE);
        doTestBlackList(TddlOperatorTable.JSON_OBJECT);
        doTestBlackList(TddlOperatorTable.JSON_OBJECTAGG);
        doTestBlackList(TddlOperatorTable.JSON_OVERLAPS);
        doTestBlackList(TddlOperatorTable.JSON_PRETTY);
        doTestBlackList(TddlOperatorTable.JSON_QUOTE);
        doTestBlackList(TddlOperatorTable.JSON_REMOVE);
        doTestBlackList(TddlOperatorTable.JSON_REPLACE);
        doTestBlackList(TddlOperatorTable.JSON_SEARCH);
        doTestBlackList(TddlOperatorTable.JSON_SET);
        doTestBlackList(TddlOperatorTable.JSON_STORAGE_SIZE);
        doTestBlackList(TddlOperatorTable.JSON_TYPE);
        doTestBlackList(TddlOperatorTable.JSON_UNQUOTE);
        doTestBlackList(TddlOperatorTable.JSON_VALID);

        doTestBlackList(TddlOperatorTable.LAG);
        doTestBlackList(TddlOperatorTable.LAST_INSERT_ID);
        doTestBlackList(TddlOperatorTable.LAST_VALUE);

        doTestBlackList(TddlOperatorTable.LEAD);
        doTestBlackList(TddlOperatorTable.LEAST);
        doTestBlackList(TddlOperatorTable.LEFT);
        doTestBlackList(TddlOperatorTable.LENGTH);

        doTestBlackList(TddlOperatorTable.LineString);
        doTestBlackList(TddlOperatorTable.LN);

        doTestBlackList(TddlOperatorTable.LOCALTIME);
        doTestBlackList(TddlOperatorTable.LOCALTIMESTAMP);
        doTestBlackList(TddlOperatorTable.LOCATE);
        doTestBlackList(TddlOperatorTable.LOG);
        doTestBlackList(TddlOperatorTable.LOG10);
        doTestBlackList(TddlOperatorTable.LOG2);
        doTestBlackList(TddlOperatorTable.LOWER);
        doTestBlackList(TddlOperatorTable.LPAD);
        doTestBlackList(TddlOperatorTable.LTRIM);
        doTestBlackList(TddlOperatorTable.MAKESET);
        doTestBlackList(TddlOperatorTable.MAKEDATE);
        doTestBlackList(TddlOperatorTable.MAKETIME);
        doTestBlackList(TddlOperatorTable.MATCH_AGAINST);
        doTestBlackList(TddlOperatorTable.MAX);
        doTestBlackList(TddlOperatorTable.MD5);
        doTestBlackList(TddlOperatorTable.MEMBER_OF);
        doTestBlackList(TddlOperatorTable.MICROSECOND);
        doTestBlackList(TddlOperatorTable.MID);
        doTestBlackList(TddlOperatorTable.MIN);
        doTestBlackList(TddlOperatorTable.MINUTE);
        doTestBlackList(TddlOperatorTable.MONTH);
        doTestBlackList(TddlOperatorTable.MONTHNAME);
        doTestBlackList(TddlOperatorTable.MultiLineString);
        doTestBlackList(TddlOperatorTable.MultiPoint);
        doTestBlackList(TddlOperatorTable.MultiPolygon);

        doTestBlackList(TddlOperatorTable.NOT_REGEXP);
        doTestBlackList(TddlOperatorTable.NOW);
        doTestBlackList(TddlOperatorTable.NTH_VALUE);
        doTestBlackList(TddlOperatorTable.NTILE);

        doTestBlackList(TddlOperatorTable.OCT);
        doTestBlackList(TddlOperatorTable.OCTET_LENGTH);

        doTestBlackList(TddlOperatorTable.ORD);
        doTestBlackList(TddlOperatorTable.PERCENT_RANK);

        doTestBlackList(TddlOperatorTable.PI);
        doTestBlackList(TddlOperatorTable.Point);
        doTestBlackList(TddlOperatorTable.Polygon);
        doTestBlackList(TddlOperatorTable.POSITION);

        doTestBlackList(TddlOperatorTable.QUARTER);
        doTestBlackList(TddlOperatorTable.QUOTE);
        doTestBlackList(TddlOperatorTable.RADIANS);
        doTestBlackList(TddlOperatorTable.RAND);
        doTestBlackList(TddlOperatorTable.RANDOM_BYTES);
        doTestBlackList(TddlOperatorTable.RANK);

        doTestBlackList(TddlOperatorTable.RELEASE_ALL_LOCKS);
        doTestBlackList(TddlOperatorTable.RELEASE_LOCK);
        doTestBlackList(TddlOperatorTable.REPEAT);
        doTestBlackList(TddlOperatorTable.REPLACE);
        doTestBlackList(TddlOperatorTable.REVERSE);
        doTestBlackList(TddlOperatorTable.RIGHT);
        doTestBlackList(TddlOperatorTable.RLIKE);
        doTestBlackList(TddlOperatorTable.ROW_COUNT);
        doTestBlackList(TddlOperatorTable.ROW_NUMBER);
        doTestBlackList(TddlOperatorTable.RPAD);
        doTestBlackList(TddlOperatorTable.RTRIM);
        doTestBlackList(TddlOperatorTable.SCHEMA);
        doTestBlackList(TddlOperatorTable.SEC_TO_TIME);

        doTestBlackList(TddlOperatorTable.SESSION_USER);
        doTestBlackList(TddlOperatorTable.SHA1);
        doTestBlackList(TddlOperatorTable.SHA2);
        doTestBlackList(TddlOperatorTable.SIGN);
        doTestBlackList(TddlOperatorTable.SIN);
        doTestBlackList(TddlOperatorTable.SLEEP);
        doTestBlackList(TddlOperatorTable.SOUNDEX);

        // don't have function: sound_like

        doTestBlackList(TddlOperatorTable.SPACE);
        doTestBlackList(TddlOperatorTable.SQRT);
        doTestBlackList(TddlOperatorTable.ST_Area);
        doTestBlackList(TddlOperatorTable.ST_AsBinary);
        doTestBlackList(TddlOperatorTable.ST_AsGeoJSON);
        doTestBlackList(TddlOperatorTable.ST_AsText);
        doTestBlackList(TddlOperatorTable.ST_Buffer);
        doTestBlackList(TddlOperatorTable.ST_Centroid);

        // don't have function: ST_Collect

        doTestBlackList(TddlOperatorTable.ST_Contains);
        doTestBlackList(TddlOperatorTable.ST_ConvexHull);
        doTestBlackList(TddlOperatorTable.ST_Crosses);
        doTestBlackList(TddlOperatorTable.ST_Difference);
        doTestBlackList(TddlOperatorTable.ST_Disjoint);
        doTestBlackList(TddlOperatorTable.ST_Distance);
        doTestBlackList(TddlOperatorTable.ST_EndPoint);
        doTestBlackList(TddlOperatorTable.ST_Envelope);
        doTestBlackList(TddlOperatorTable.ST_Equals);
        doTestBlackList(TddlOperatorTable.ST_ExteriorRing);

        // don't have function: ST_FrechetDistance

        doTestBlackList(TddlOperatorTable.ST_GeoHash);
        doTestBlackList(TddlOperatorTable.ST_GeomCollFromText);
        doTestBlackList(TddlOperatorTable.ST_GeomFromText);

        // don't have function: ST_GeomFromWKB
        // don't have function: ST_HausdorffDistance

        doTestBlackList(TddlOperatorTable.ST_InteriorRingN);
        doTestBlackList(TddlOperatorTable.ST_Intersection);
        doTestBlackList(TddlOperatorTable.ST_Intersects);
        doTestBlackList(TddlOperatorTable.ST_IsClosed);
        doTestBlackList(TddlOperatorTable.ST_IsEmpty);
        doTestBlackList(TddlOperatorTable.ST_IsValid);
        doTestBlackList(TddlOperatorTable.ST_NumGeometries);
        doTestBlackList(TddlOperatorTable.ST_NumInteriorRing);
        doTestBlackList(TddlOperatorTable.ST_NumPoints);
        doTestBlackList(TddlOperatorTable.ST_Overlaps);

        // don't have function: ST_PointAtDistance

        doTestBlackList(TddlOperatorTable.ST_PointFromGeoHash);
        doTestBlackList(TddlOperatorTable.ST_PointFromText);
        doTestBlackList(TddlOperatorTable.ST_PointFromWKB);
        doTestBlackList(TddlOperatorTable.ST_PointN);
        doTestBlackList(TddlOperatorTable.ST_PolyFromText);
        doTestBlackList(TddlOperatorTable.ST_PolyFromWKB);
        doTestBlackList(TddlOperatorTable.ST_Simplify);
        doTestBlackList(TddlOperatorTable.ST_SRID);
        doTestBlackList(TddlOperatorTable.ST_StartPoint);

        // don't have function: ST_Transform

        doTestBlackList(TddlOperatorTable.ST_Union);
        doTestBlackList(TddlOperatorTable.ST_Validate);
        doTestBlackList(TddlOperatorTable.ST_Within);
        doTestBlackList(TddlOperatorTable.ST_X);
        doTestBlackList(TddlOperatorTable.ST_Y);

        // don't have function: STATEMENT_DIGEST, STATEMENT_DIGEST_TEXT

        doTestBlackList(TddlOperatorTable.STD);
        doTestBlackList(TddlOperatorTable.STDDEV);
        doTestBlackList(TddlOperatorTable.STDDEV_POP);
        doTestBlackList(TddlOperatorTable.STDDEV_SAMP);

        doTestBlackList(TddlOperatorTable.SUM);
        doTestBlackList(TddlOperatorTable.SYSDATE);
        doTestBlackList(TddlOperatorTable.SYSTEM_USER);
        doTestBlackList(TddlOperatorTable.TAN);
        doTestBlackList(TddlOperatorTable.TIME);

        doTestBlackList(TddlOperatorTable.TRIM);
        doTestBlackList(TddlOperatorTable.TRUNCATE);
        doTestBlackList(TddlOperatorTable.UNCOMPRESS);

        // don't have function: UNCOMPRESSED_LENGTH, UPDATE_XML, VALIDATE_PASSWORD_STRENGTH,
        // WAIT_FOR_EXECUTED_GTID_SET,WEIGHT_STRING,LOGICAL_XOR,BITWISE_INVERSION，WEIGHT_STRING,
        // LOGICAL_XOR,BITWISE_INVERSION

        doTestBlackList(TddlOperatorTable.UNHEX);
        doTestBlackList(TddlOperatorTable.UNIX_TIMESTAMP);
        doTestBlackList(TddlOperatorTable.UPPER);
        doTestBlackList(TddlOperatorTable.USER);

        doTestBlackList(TddlOperatorTable.UUID);
        doTestBlackList(TddlOperatorTable.UUID_SHORT);
        doTestBlackList(TddlOperatorTable.UUID_TO_BIN);
        doTestBlackList(TddlOperatorTable.VALUES);
        doTestBlackList(TddlOperatorTable.VAR_POP);
        doTestBlackList(TddlOperatorTable.VAR_SAMP);
        doTestBlackList(TddlOperatorTable.VARIANCE);
        doTestBlackList(TddlOperatorTable.VERSION);

        doTestBlackList(TddlOperatorTable.BITWISE_OR);
    }

    @Test
    public void testWhitelist() {
        doTestWhiteList(TddlOperatorTable.GREATER_THAN);
        doTestWhiteList(TddlOperatorTable.GREATER_THAN_OR_EQUAL);
        doTestWhiteList(TddlOperatorTable.LESS_THAN);
        doTestWhiteList(TddlOperatorTable.NOT_EQUALS);

        doTestWhiteList(TddlOperatorTable.LESS_THAN_OR_EQUAL);
        doTestWhiteList(TddlOperatorTable.NULL_SAFE_EQUAL);
        doTestWhiteList(TddlOperatorTable.MOD);
        doTestWhiteList(TddlOperatorTable.MULTIPLY);
        doTestWhiteList(TddlOperatorTable.PLUS);
        doTestWhiteList(TddlOperatorTable.MINUS);

        doTestWhiteList(TddlOperatorTable.DIVIDE);
        doTestWhiteList(TddlOperatorTable.EQUALS);

        doTestWhiteList(TddlOperatorTable.ABS);

        doTestWhiteList(TddlOperatorTable.ADDDATE);
        doTestWhiteList(TddlOperatorTable.ADDTIME);

        doTestWhiteList(TddlOperatorTable.BETWEEN);

        doTestWhiteList(TddlOperatorTable.CASE);
        doTestWhiteList(TddlOperatorTable.CEIL);
        doTestWhiteList(TddlOperatorTable.CEILING);
        doTestWhiteList(TddlOperatorTable.COALESCE);
        doTestWhiteList(TddlOperatorTable.CONCAT);

        doTestWhiteList(TddlOperatorTable.DATE);
        doTestWhiteList(TddlOperatorTable.DATE_ADD);
        doTestWhiteList(TddlOperatorTable.DATE_FORMAT);
        doTestWhiteList(TddlOperatorTable.DATE_SUB);
        doTestWhiteList(TddlOperatorTable.DATEDIFF);
        doTestWhiteList(TddlOperatorTable.DAY);
        doTestWhiteList(TddlOperatorTable.DAYNAME);
        doTestWhiteList(TddlOperatorTable.DAYOFMONTH);
        doTestWhiteList(TddlOperatorTable.DAYOFWEEK);
        doTestWhiteList(TddlOperatorTable.DAYOFYEAR);
        doTestWhiteList(TddlOperatorTable.DIVIDE_INTEGER);
        doTestWhiteList(TddlOperatorTable.EXTRACT);
        doTestWhiteList(TddlOperatorTable.FLOOR);
        doTestWhiteList(TddlOperatorTable.FROM_DAYS);

        doTestWhiteList(TddlOperatorTable.IF);
        doTestWhiteList(TddlOperatorTable.IFNULL);
        doTestWhiteList(TddlOperatorTable.IN);

        doTestWhiteList(TddlOperatorTable.IS_TRUE);
        doTestWhiteList(TddlOperatorTable.IS_NOT_TRUE);
        doTestWhiteList(TddlOperatorTable.IS_NOT_NULL);
        doTestWhiteList(TddlOperatorTable.IS_NULL);
        doTestWhiteList(TddlOperatorTable.ISNULL);

        doTestWhiteList(TddlOperatorTable.LIKE);

        doTestWhiteList(TddlOperatorTable.NOT);
        doTestWhiteList(TddlOperatorTable.NOT_BETWEEN);
        doTestWhiteList(TddlOperatorTable.NOT_EXISTS);
        doTestWhiteList(TddlOperatorTable.NOT_IN);
        doTestWhiteList(TddlOperatorTable.NOT_LIKE);

        doTestWhiteList(TddlOperatorTable.NULLIF);
        doTestWhiteList(TddlOperatorTable.OR);

        doTestWhiteList(TddlOperatorTable.PERIODADD);
        doTestWhiteList(TddlOperatorTable.PERIODDIFF);

        doTestWhiteList(TddlOperatorTable.POW);
        doTestWhiteList(TddlOperatorTable.POWER);
        doTestWhiteList(TddlOperatorTable.REGEXP);

        doTestWhiteList(TddlOperatorTable.SECOND);
        doTestWhiteList(TddlOperatorTable.STRTODATE);
        doTestWhiteList(TddlOperatorTable.STRCMP);
        doTestWhiteList(TddlOperatorTable.SUBDATE);
        doTestWhiteList(TddlOperatorTable.SUBSTR);
        doTestWhiteList(TddlOperatorTable.SUBSTRING);
        doTestWhiteList(TddlOperatorTable.SUBSTRING_INDEX);

        doTestWhiteList(TddlOperatorTable.WEEK);
        doTestWhiteList(TddlOperatorTable.WEEKDAY);
        doTestWhiteList(TddlOperatorTable.WEEKOFYEAR);

        doTestWhiteList(TddlOperatorTable.YEAR);
        doTestWhiteList(TddlOperatorTable.YEARWEEK);
    }

    @Test
    public void testDynamic() {
        // Time Function
        doTestDynamic(TddlOperatorTable.CURDATE);
        doTestDynamic(TddlOperatorTable.CURRENT_DATE);
        doTestDynamic(TddlOperatorTable.CURRENT_TIME);
        doTestDynamic(TddlOperatorTable.CURRENT_TIMESTAMP);
        doTestDynamic(TddlOperatorTable.CURTIME);
        doTestDynamic(TddlOperatorTable.LOCALTIME);
        doTestDynamic(TddlOperatorTable.LOCALTIMESTAMP);
        doTestDynamic(TddlOperatorTable.NOW);
        doTestDynamic(TddlOperatorTable.SYSDATE);
        doTestDynamic(TddlOperatorTable.UNIX_TIMESTAMP);
        doTestDynamic(TddlOperatorTable.UTC_DATE);
        doTestDynamic(TddlOperatorTable.UTC_TIME);
        doTestDynamic(TddlOperatorTable.TSO_TIMESTAMP);

        // Numeric Functions
        doTestDynamic(TddlOperatorTable.RAND);

        // Miscellaneous Functions
        doTestDynamic(TddlOperatorTable.UUID);
        doTestDynamic(TddlOperatorTable.UUID_SHORT);

        // Information Functions
        doTestDynamic(TddlOperatorTable.CONNECTION_ID);
        doTestDynamic(TddlOperatorTable.CURRENT_ROLE);
        doTestDynamic(TddlOperatorTable.CURRENT_USER);
        doTestDynamic(TddlOperatorTable.DATABASE);
        doTestDynamic(TddlOperatorTable.FOUND_ROWS);
        doTestDynamic(TddlOperatorTable.LAST_INSERT_ID);
        doTestDynamic(TddlOperatorTable.ROW_COUNT);
        doTestDynamic(TddlOperatorTable.SCHEMA);
        doTestDynamic(TddlOperatorTable.SESSION_USER);
        doTestDynamic(TddlOperatorTable.SYSTEM_USER);
        doTestDynamic(TddlOperatorTable.USER);
        doTestDynamic(TddlOperatorTable.VERSION);

        doTestDynamic(TddlOperatorTable.GET_LOCK);
        doTestDynamic(TddlOperatorTable.IS_FREE_LOCK);
        doTestDynamic(TddlOperatorTable.IS_USED_LOCK);
        doTestDynamic(TddlOperatorTable.RELEASE_ALL_LOCKS);
        doTestDynamic(TddlOperatorTable.RELEASE_LOCK);

        doTestDynamic(TddlOperatorTable.BENCHMARK);
    }

}
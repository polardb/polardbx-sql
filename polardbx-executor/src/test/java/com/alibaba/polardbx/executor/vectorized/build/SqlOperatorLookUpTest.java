package com.alibaba.polardbx.executor.vectorized.build;

import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import org.apache.calcite.sql.SqlOperator;
import org.junit.Assert;
import org.junit.Test;

public class SqlOperatorLookUpTest {
    @Test
    public void test() {
        doTestLookup(TddlOperatorTable.GREATER_THAN.getName());
        doTestLookup(TddlOperatorTable.GREATER_THAN_OR_EQUAL.getName());
        doTestLookup(TddlOperatorTable.LESS_THAN.getName());
        doTestLookup(TddlOperatorTable.NOT_EQUALS.getName());

        doTestLookup(TddlOperatorTable.LESS_THAN_OR_EQUAL.getName());
        doTestLookup(TddlOperatorTable.NULL_SAFE_EQUAL.getName());
        doTestLookup(TddlOperatorTable.MOD.getName());
        doTestLookup(TddlOperatorTable.MULTIPLY.getName());
        doTestLookup(TddlOperatorTable.PLUS.getName());
        doTestLookup(TddlOperatorTable.MINUS.getName());

        doTestLookup(TddlOperatorTable.DIVIDE.getName());
        doTestLookup(TddlOperatorTable.EQUALS.getName());

        doTestLookup(TddlOperatorTable.ABS.getName());

        doTestLookup(TddlOperatorTable.ADDDATE.getName());
        doTestLookup(TddlOperatorTable.ADDTIME.getName());

        doTestLookup(TddlOperatorTable.BETWEEN.getName());

        doTestLookup(TddlOperatorTable.CASE.getName());
        doTestLookup(TddlOperatorTable.CEIL.getName());
        doTestLookup(TddlOperatorTable.CEILING.getName());
        doTestLookup(TddlOperatorTable.COALESCE.getName());
        doTestLookup(TddlOperatorTable.CONCAT.getName());

        doTestLookup(TddlOperatorTable.DATE.getName());
        doTestLookup(TddlOperatorTable.DATE_ADD.getName());
        doTestLookup(TddlOperatorTable.DATE_FORMAT.getName());
        doTestLookup(TddlOperatorTable.DATE_SUB.getName());
        doTestLookup(TddlOperatorTable.DATEDIFF.getName());
        doTestLookup(TddlOperatorTable.DAY.getName());
        doTestLookup(TddlOperatorTable.DAYNAME.getName());
        doTestLookup(TddlOperatorTable.DAYOFMONTH.getName());
        doTestLookup(TddlOperatorTable.DAYOFWEEK.getName());
        doTestLookup(TddlOperatorTable.DAYOFYEAR.getName());
        doTestLookup(TddlOperatorTable.DIVIDE_INTEGER.getName());
        doTestLookup(TddlOperatorTable.EXTRACT.getName());
        doTestLookup(TddlOperatorTable.FLOOR.getName());
        doTestLookup(TddlOperatorTable.FROM_DAYS.getName());

        doTestLookup(TddlOperatorTable.IF.getName());
        doTestLookup(TddlOperatorTable.IFNULL.getName());
        doTestLookup(TddlOperatorTable.IN.getName());

        doTestLookup(TddlOperatorTable.IS_TRUE.getName());
        doTestLookup(TddlOperatorTable.IS_NOT_TRUE.getName());
        doTestLookup(TddlOperatorTable.IS_NOT_NULL.getName());
        doTestLookup(TddlOperatorTable.IS_NULL.getName());
        doTestLookup(TddlOperatorTable.ISNULL.getName());

        doTestLookup(TddlOperatorTable.LIKE.getName());

        doTestLookup(TddlOperatorTable.NOT.getName());
        doTestLookup(TddlOperatorTable.NOT_BETWEEN.getName());
        doTestLookup(TddlOperatorTable.NOT_EXISTS.getName());
        doTestLookup(TddlOperatorTable.NOT_IN.getName());
        doTestLookup(TddlOperatorTable.NOT_LIKE.getName());

        doTestLookup(TddlOperatorTable.NULLIF.getName());
        doTestLookup(TddlOperatorTable.OR.getName());

        doTestLookup(TddlOperatorTable.PERIODADD.getName());
        doTestLookup(TddlOperatorTable.PERIODDIFF.getName());

        doTestLookup(TddlOperatorTable.POW.getName());
        doTestLookup(TddlOperatorTable.POWER.getName());
        doTestLookup(TddlOperatorTable.REGEXP.getName());

        doTestLookup(TddlOperatorTable.SECOND.getName());
        doTestLookup(TddlOperatorTable.STRTODATE.getName());
        doTestLookup(TddlOperatorTable.STRCMP.getName());
        doTestLookup(TddlOperatorTable.SUBDATE.getName());
        doTestLookup(TddlOperatorTable.SUBSTR.getName());
        doTestLookup(TddlOperatorTable.SUBSTRING.getName());
        doTestLookup(TddlOperatorTable.SUBSTRING_INDEX.getName());

        doTestLookup(TddlOperatorTable.WEEK.getName());
        doTestLookup(TddlOperatorTable.WEEKDAY.getName());
        doTestLookup(TddlOperatorTable.WEEKOFYEAR.getName());

        doTestLookup(TddlOperatorTable.YEAR.getName());
        doTestLookup(TddlOperatorTable.YEARWEEK.getName());

        doTestLookup(TddlOperatorTable.PART_ROUTE.getName());
        // Non-mysql Functions
        doTestLookup(TddlOperatorTable.PART_ROUTE.getName());

        // Information Functions
        doTestLookup(TddlOperatorTable.CONNECTION_ID.getName());
        doTestLookup(TddlOperatorTable.CURRENT_ROLE.getName());
        doTestLookup(TddlOperatorTable.CURRENT_USER.getName());
        doTestLookup(TddlOperatorTable.DATABASE.getName());
        doTestLookup(TddlOperatorTable.FOUND_ROWS.getName());
        doTestLookup(TddlOperatorTable.ROW_COUNT.getName());
        doTestLookup(TddlOperatorTable.SCHEMA.getName());
        doTestLookup(TddlOperatorTable.SESSION_USER.getName());
        doTestLookup(TddlOperatorTable.SYSTEM_USER.getName());
        doTestLookup(TddlOperatorTable.USER.getName());
        doTestLookup(TddlOperatorTable.VERSION.getName());
        doTestLookup(TddlOperatorTable.TSO_TIMESTAMP.getName());
        doTestLookup(TddlOperatorTable.SPECIAL_POW.getName());
        doTestLookup(TddlOperatorTable.CAN_ACCESS_TABLE.getName());
        doTestLookup(TddlOperatorTable.GET_LOCK.getName());
        doTestLookup(TddlOperatorTable.RELEASE_LOCK.getName());
        doTestLookup(TddlOperatorTable.RELEASE_ALL_LOCKS.getName());
        doTestLookup(TddlOperatorTable.IS_FREE_LOCK.getName());
        doTestLookup(TddlOperatorTable.IS_USED_LOCK.getName());
        doTestLookup(TddlOperatorTable.HYPERLOGLOG.getName());
        doTestLookup(TddlOperatorTable.PART_HASH.getName());
        doTestLookup(TddlOperatorTable.LBAC_CHECK.getName());
        doTestLookup(TddlOperatorTable.LBAC_READ.getName());
        doTestLookup(TddlOperatorTable.LBAC_WRITE.getName());
        doTestLookup(TddlOperatorTable.LBAC_WRITE_STRICT_CHECK.getName());
        doTestLookup(TddlOperatorTable.LBAC_USER_WRITE_LABEL.getName());

        // Time Function
        doTestLookup(TddlOperatorTable.CURDATE.getName());
        doTestLookup(TddlOperatorTable.CURRENT_DATE.getName());
        doTestLookup(TddlOperatorTable.CURRENT_TIME.getName());
        doTestLookup(TddlOperatorTable.CURRENT_TIMESTAMP.getName());
        doTestLookup(TddlOperatorTable.CURTIME.getName());
        doTestLookup(TddlOperatorTable.LOCALTIME.getName());
        doTestLookup(TddlOperatorTable.LOCALTIMESTAMP.getName());
        doTestLookup(TddlOperatorTable.NOW.getName());
        doTestLookup(TddlOperatorTable.SYSDATE.getName());
        doTestLookup(TddlOperatorTable.UNIX_TIMESTAMP.getName());
        doTestLookup(TddlOperatorTable.UTC_DATE.getName());
        doTestLookup(TddlOperatorTable.UTC_TIME.getName());
        doTestLookup(TddlOperatorTable.TSO_TIMESTAMP.getName());

        // Numeric Functions
        doTestLookup(TddlOperatorTable.RAND.getName());

        // Miscellaneous Functions
        doTestLookup(TddlOperatorTable.UUID.getName());
        doTestLookup(TddlOperatorTable.UUID_SHORT.getName());

        // Information Functions
        doTestLookup(TddlOperatorTable.CONNECTION_ID.getName());
        doTestLookup(TddlOperatorTable.CURRENT_ROLE.getName());
        doTestLookup(TddlOperatorTable.CURRENT_USER.getName());
        doTestLookup(TddlOperatorTable.DATABASE.getName());
        doTestLookup(TddlOperatorTable.FOUND_ROWS.getName());
        doTestLookup(TddlOperatorTable.LAST_INSERT_ID.getName());
        doTestLookup(TddlOperatorTable.ROW_COUNT.getName());
        doTestLookup(TddlOperatorTable.SCHEMA.getName());
        doTestLookup(TddlOperatorTable.SESSION_USER.getName());
        doTestLookup(TddlOperatorTable.SYSTEM_USER.getName());
        doTestLookup(TddlOperatorTable.USER.getName());
        doTestLookup(TddlOperatorTable.VERSION.getName());

        // Locking function
        doTestLookup(TddlOperatorTable.GET_LOCK.getName());
        doTestLookup(TddlOperatorTable.IS_FREE_LOCK.getName());
        doTestLookup(TddlOperatorTable.IS_USED_LOCK.getName());
        doTestLookup(TddlOperatorTable.RELEASE_ALL_LOCKS.getName());
        doTestLookup(TddlOperatorTable.RELEASE_LOCK.getName());

        // Benchmark function
        doTestLookup(TddlOperatorTable.BENCHMARK.getName());

        // cast function
        doTestLookup(TddlOperatorTable.CAST.getName());
        doTestLookup(TddlOperatorTable.CONVERT.getName());
        doTestLookup(TddlOperatorTable.IMPLICIT_CAST.getName());

        // all functions
        doTestLookup(TddlOperatorTable.BITWISE_AND.getName());
        doTestLookup(TddlOperatorTable.BITRSHIFT.getName());

        doTestLookup(TddlOperatorTable.BITLSHIFT.getName());

        doTestLookup(TddlOperatorTable.JSON_EXTRACT.getName());
        doTestLookup(TddlOperatorTable.JSON_UNQUOTE.getName());

        doTestLookup(TddlOperatorTable.ASSIGNMENT.getName());
        doTestLookup(TddlOperatorTable.BITWISE_XOR.getName());

        doTestLookup(TddlOperatorTable.ACOS.getName());

        doTestLookup(TddlOperatorTable.AES_DECRYPT.getName());
        doTestLookup(TddlOperatorTable.AES_ENCRYPT.getName());
        doTestLookup(TddlOperatorTable.BITWISE_AND.getName());

        doTestLookup(TddlOperatorTable.ASCII.getName());
        doTestLookup(TddlOperatorTable.ASIN.getName());
        doTestLookup(TddlOperatorTable.ATAN.getName());
        doTestLookup(TddlOperatorTable.ATAN2.getName());
        doTestLookup(TddlOperatorTable.AVG.getName());
        doTestLookup(TddlOperatorTable.BENCHMARK.getName());

        doTestLookup(TddlOperatorTable.BIN.getName());
        doTestLookup(TddlOperatorTable.BIN_TO_UUID.getName());
        doTestLookup(TddlOperatorTable.BINARY.getName());
        doTestLookup(TddlOperatorTable.BIT_AND.getName());
        doTestLookup(TddlOperatorTable.BIT_COUNT.getName());
        doTestLookup(TddlOperatorTable.BIT_LENGTH.getName());
        doTestLookup(TddlOperatorTable.BIT_OR.getName());
        doTestLookup(TddlOperatorTable.BIT_XOR.getName());

        doTestLookup(TddlOperatorTable.CAST.getName());

        doTestLookup(TddlOperatorTable.CHAR.getName());
        doTestLookup(TddlOperatorTable.CHAR_LENGTH.getName());
        doTestLookup(TddlOperatorTable.CHARACTER_LENGTH.getName());
        doTestLookup(TddlOperatorTable.CHARSET.getName());

        doTestLookup(TddlOperatorTable.COLLATION.getName());
        doTestLookup(TddlOperatorTable.COMPRESS.getName());

        doTestLookup(TddlOperatorTable.CONCAT_WS.getName());
        doTestLookup(TddlOperatorTable.CONNECTION_ID.getName());
        doTestLookup(TddlOperatorTable.CONV.getName());
        doTestLookup(TddlOperatorTable.CONVERT.getName());
        doTestLookup(TddlOperatorTable.CONVERT_TZ.getName());
        doTestLookup(TddlOperatorTable.COS.getName());
        doTestLookup(TddlOperatorTable.COT.getName());
        doTestLookup(TddlOperatorTable.COUNT.getName());
        doTestLookup(TddlOperatorTable.CRC32.getName());
        doTestLookup(TddlOperatorTable.CUME_DIST.getName());
        doTestLookup(TddlOperatorTable.CURDATE.getName());

        doTestLookup(TddlOperatorTable.CURRENT_DATE.getName());
        doTestLookup(TddlOperatorTable.CURRENT_ROLE.getName());
        doTestLookup(TddlOperatorTable.CURRENT_TIME.getName());
        doTestLookup(TddlOperatorTable.CURRENT_TIMESTAMP.getName());
        doTestLookup(TddlOperatorTable.CURRENT_USER.getName());
        doTestLookup(TddlOperatorTable.CURTIME.getName());

        doTestLookup(TddlOperatorTable.DATABASE.getName());

        doTestLookup(TddlOperatorTable.DEFAULT.getName());
        doTestLookup(TddlOperatorTable.DEGREES.getName());
        doTestLookup(TddlOperatorTable.DENSE_RANK.getName());

        doTestLookup(TddlOperatorTable.ELT.getName());
        doTestLookup(TddlOperatorTable.EXISTS.getName());
        doTestLookup(TddlOperatorTable.EXP.getName());
        doTestLookup(TddlOperatorTable.EXPORTSET.getName());

        doTestLookup(TddlOperatorTable.FIELD.getName());
        doTestLookup(TddlOperatorTable.FIND_IN_SET.getName());
        doTestLookup(TddlOperatorTable.FIRST_VALUE.getName());
        doTestLookup(TddlOperatorTable.FORMAT.getName());

        doTestLookup(TddlOperatorTable.FOUND_ROWS.getName());
        doTestLookup(TddlOperatorTable.FROM_UNIXTIME.getName());
        doTestLookup(TddlOperatorTable.GET_LOCK.getName());
        doTestLookup(TddlOperatorTable.GREATEST.getName());
        doTestLookup(TddlOperatorTable.GROUP_CONCAT.getName());
        doTestLookup(TddlOperatorTable.HEX.getName());
        doTestLookup(TddlOperatorTable.HOUR.getName());

        doTestLookup(TddlOperatorTable.INSERT.getName());
        doTestLookup(TddlOperatorTable.INSTR.getName());
        doTestLookup(TddlOperatorTable.INTERVAL.getName());

        doTestLookup(TddlOperatorTable.IS_FREE_LOCK.getName());

        doTestLookup(TddlOperatorTable.IS_USED_LOCK.getName());

        doTestLookup(TddlOperatorTable.JSON_ARRAY.getName());
        doTestLookup(TddlOperatorTable.JSON_ARRAY_APPEND.getName());
        doTestLookup(TddlOperatorTable.JSON_ARRAY_INSERT.getName());
        doTestLookup(TddlOperatorTable.JSON_ARRAYAGG.getName());
        doTestLookup(TddlOperatorTable.JSON_CONTAINS.getName());
        doTestLookup(TddlOperatorTable.JSON_CONTAINS_PATH.getName());
        doTestLookup(TddlOperatorTable.JSON_DEPTH.getName());
        doTestLookup(TddlOperatorTable.JSON_EXTRACT.getName());
        doTestLookup(TddlOperatorTable.JSON_INSERT.getName());
        doTestLookup(TddlOperatorTable.JSON_KEYS.getName());
        doTestLookup(TddlOperatorTable.JSON_LENGTH.getName());
        doTestLookup(TddlOperatorTable.JSON_MERGE.getName());
        doTestLookup(TddlOperatorTable.JSON_MERGE_PATCH.getName());
        doTestLookup(TddlOperatorTable.JSON_MERGE_PRESERVE.getName());
        doTestLookup(TddlOperatorTable.JSON_OBJECT.getName());
        doTestLookup(TddlOperatorTable.JSON_OBJECTAGG.getName());
        doTestLookup(TddlOperatorTable.JSON_OVERLAPS.getName());
        doTestLookup(TddlOperatorTable.JSON_PRETTY.getName());
        doTestLookup(TddlOperatorTable.JSON_QUOTE.getName());
        doTestLookup(TddlOperatorTable.JSON_REMOVE.getName());
        doTestLookup(TddlOperatorTable.JSON_REPLACE.getName());
        doTestLookup(TddlOperatorTable.JSON_SEARCH.getName());
        doTestLookup(TddlOperatorTable.JSON_SET.getName());
        doTestLookup(TddlOperatorTable.JSON_STORAGE_SIZE.getName());
        doTestLookup(TddlOperatorTable.JSON_TYPE.getName());
        doTestLookup(TddlOperatorTable.JSON_UNQUOTE.getName());
        doTestLookup(TddlOperatorTable.JSON_VALID.getName());

        doTestLookup(TddlOperatorTable.LAG.getName());
        doTestLookup(TddlOperatorTable.LAST_INSERT_ID.getName());
        doTestLookup(TddlOperatorTable.LAST_VALUE.getName());

        doTestLookup(TddlOperatorTable.LEAD.getName());
        doTestLookup(TddlOperatorTable.LEAST.getName());
        doTestLookup(TddlOperatorTable.LEFT.getName());
        doTestLookup(TddlOperatorTable.LENGTH.getName());

        doTestLookup(TddlOperatorTable.LineString.getName());
        doTestLookup(TddlOperatorTable.LN.getName());

        doTestLookup(TddlOperatorTable.LOCALTIME.getName());
        doTestLookup(TddlOperatorTable.LOCALTIMESTAMP.getName());
        doTestLookup(TddlOperatorTable.LOCATE.getName());
        doTestLookup(TddlOperatorTable.LOG.getName());
        doTestLookup(TddlOperatorTable.LOG10.getName());
        doTestLookup(TddlOperatorTable.LOG2.getName());
        doTestLookup(TddlOperatorTable.LOWER.getName());
        doTestLookup(TddlOperatorTable.LPAD.getName());
        doTestLookup(TddlOperatorTable.LTRIM.getName());
        doTestLookup(TddlOperatorTable.MAKESET.getName());
        doTestLookup(TddlOperatorTable.MAKEDATE.getName());
        doTestLookup(TddlOperatorTable.MAKETIME.getName());
        doTestLookup(TddlOperatorTable.MATCH_AGAINST.getName());
        doTestLookup(TddlOperatorTable.MAX.getName());
        doTestLookup(TddlOperatorTable.MD5.getName());
        doTestLookup(TddlOperatorTable.MEMBER_OF.getName());
        doTestLookup(TddlOperatorTable.MICROSECOND.getName());
        doTestLookup(TddlOperatorTable.MID.getName());
        doTestLookup(TddlOperatorTable.MIN.getName());
        doTestLookup(TddlOperatorTable.MINUTE.getName());
        doTestLookup(TddlOperatorTable.MONTH.getName());
        doTestLookup(TddlOperatorTable.MONTHNAME.getName());
        doTestLookup(TddlOperatorTable.MultiLineString.getName());
        doTestLookup(TddlOperatorTable.MultiPoint.getName());
        doTestLookup(TddlOperatorTable.MultiPolygon.getName());

        doTestLookup(TddlOperatorTable.NOT_REGEXP.getName());
        doTestLookup(TddlOperatorTable.NOW.getName());
        doTestLookup(TddlOperatorTable.NTH_VALUE.getName());
        doTestLookup(TddlOperatorTable.NTILE.getName());

        doTestLookup(TddlOperatorTable.OCT.getName());
        doTestLookup(TddlOperatorTable.OCTET_LENGTH.getName());

        doTestLookup(TddlOperatorTable.ORD.getName());
        doTestLookup(TddlOperatorTable.PERCENT_RANK.getName());

        doTestLookup(TddlOperatorTable.PI.getName());
        doTestLookup(TddlOperatorTable.Point.getName());
        doTestLookup(TddlOperatorTable.Polygon.getName());
        doTestLookup(TddlOperatorTable.POSITION.getName());

        doTestLookup(TddlOperatorTable.QUARTER.getName());
        doTestLookup(TddlOperatorTable.QUOTE.getName());
        doTestLookup(TddlOperatorTable.RADIANS.getName());
        doTestLookup(TddlOperatorTable.RAND.getName());
        doTestLookup(TddlOperatorTable.RANDOM_BYTES.getName());
        doTestLookup(TddlOperatorTable.RANK.getName());

        doTestLookup(TddlOperatorTable.RELEASE_ALL_LOCKS.getName());
        doTestLookup(TddlOperatorTable.RELEASE_LOCK.getName());
        doTestLookup(TddlOperatorTable.REPEAT.getName());
        doTestLookup(TddlOperatorTable.REPLACE.getName());
        doTestLookup(TddlOperatorTable.REVERSE.getName());
        doTestLookup(TddlOperatorTable.RIGHT.getName());
        doTestLookup(TddlOperatorTable.RLIKE.getName());
        doTestLookup(TddlOperatorTable.ROW_COUNT.getName());
        doTestLookup(TddlOperatorTable.ROW_NUMBER.getName());
        doTestLookup(TddlOperatorTable.RPAD.getName());
        doTestLookup(TddlOperatorTable.RTRIM.getName());
        doTestLookup(TddlOperatorTable.SCHEMA.getName());
        doTestLookup(TddlOperatorTable.SEC_TO_TIME.getName());

        doTestLookup(TddlOperatorTable.SESSION_USER.getName());
        doTestLookup(TddlOperatorTable.SHA1.getName());
        doTestLookup(TddlOperatorTable.SHA2.getName());
        doTestLookup(TddlOperatorTable.SIGN.getName());
        doTestLookup(TddlOperatorTable.SIN.getName());
        doTestLookup(TddlOperatorTable.SLEEP.getName());
        doTestLookup(TddlOperatorTable.SOUNDEX.getName());

        // don't have function: sound_like

        doTestLookup(TddlOperatorTable.SPACE.getName());
        doTestLookup(TddlOperatorTable.SQRT.getName());
        doTestLookup(TddlOperatorTable.ST_Area.getName());
        doTestLookup(TddlOperatorTable.ST_AsBinary.getName());
        doTestLookup(TddlOperatorTable.ST_AsGeoJSON.getName());
        doTestLookup(TddlOperatorTable.ST_AsText.getName());
        doTestLookup(TddlOperatorTable.ST_Buffer.getName());
        doTestLookup(TddlOperatorTable.ST_Centroid.getName());

        // don't have function: ST_Collect

        doTestLookup(TddlOperatorTable.ST_Contains.getName());
        doTestLookup(TddlOperatorTable.ST_ConvexHull.getName());
        doTestLookup(TddlOperatorTable.ST_Crosses.getName());
        doTestLookup(TddlOperatorTable.ST_Difference.getName());
        doTestLookup(TddlOperatorTable.ST_Disjoint.getName());
        doTestLookup(TddlOperatorTable.ST_Distance.getName());
        doTestLookup(TddlOperatorTable.ST_EndPoint.getName());
        doTestLookup(TddlOperatorTable.ST_Envelope.getName());
        doTestLookup(TddlOperatorTable.ST_Equals.getName());
        doTestLookup(TddlOperatorTable.ST_ExteriorRing.getName());

        // don't have function: ST_FrechetDistance

        doTestLookup(TddlOperatorTable.ST_GeoHash.getName());
        doTestLookup(TddlOperatorTable.ST_GeomCollFromText.getName());
        doTestLookup(TddlOperatorTable.ST_GeomFromText.getName());

        // don't have function: ST_GeomFromWKB
        // don't have function: ST_HausdorffDistance

        doTestLookup(TddlOperatorTable.ST_InteriorRingN.getName());
        doTestLookup(TddlOperatorTable.ST_Intersection.getName());
        doTestLookup(TddlOperatorTable.ST_Intersects.getName());
        doTestLookup(TddlOperatorTable.ST_IsClosed.getName());
        doTestLookup(TddlOperatorTable.ST_IsEmpty.getName());
        doTestLookup(TddlOperatorTable.ST_IsValid.getName());
        doTestLookup(TddlOperatorTable.ST_NumGeometries.getName());
        doTestLookup(TddlOperatorTable.ST_NumInteriorRing.getName());
        doTestLookup(TddlOperatorTable.ST_NumPoints.getName());
        doTestLookup(TddlOperatorTable.ST_Overlaps.getName());

        // don't have function: ST_PointAtDistance

        doTestLookup(TddlOperatorTable.ST_PointFromGeoHash.getName());
        doTestLookup(TddlOperatorTable.ST_PointFromText.getName());
        doTestLookup(TddlOperatorTable.ST_PointFromWKB.getName());
        doTestLookup(TddlOperatorTable.ST_PointN.getName());
        doTestLookup(TddlOperatorTable.ST_PolyFromText.getName());
        doTestLookup(TddlOperatorTable.ST_PolyFromWKB.getName());
        doTestLookup(TddlOperatorTable.ST_Simplify.getName());
        doTestLookup(TddlOperatorTable.ST_SRID.getName());
        doTestLookup(TddlOperatorTable.ST_StartPoint.getName());

        // don't have function: ST_Transform

        doTestLookup(TddlOperatorTable.ST_Union.getName());
        doTestLookup(TddlOperatorTable.ST_Validate.getName());
        doTestLookup(TddlOperatorTable.ST_Within.getName());
        doTestLookup(TddlOperatorTable.ST_X.getName());
        doTestLookup(TddlOperatorTable.ST_Y.getName());

        // don't have function: STATEMENT_DIGEST, STATEMENT_DIGEST_TEXT

        doTestLookup(TddlOperatorTable.STD.getName());
        doTestLookup(TddlOperatorTable.STDDEV.getName());
        doTestLookup(TddlOperatorTable.STDDEV_POP.getName());
        doTestLookup(TddlOperatorTable.STDDEV_SAMP.getName());

        doTestLookup(TddlOperatorTable.SUM.getName());
        doTestLookup(TddlOperatorTable.SYSDATE.getName());
        doTestLookup(TddlOperatorTable.SYSTEM_USER.getName());
        doTestLookup(TddlOperatorTable.TAN.getName());
        doTestLookup(TddlOperatorTable.TIME.getName());

        doTestLookup(TddlOperatorTable.TRIM.getName());
        doTestLookup(TddlOperatorTable.TRUNCATE.getName());
        doTestLookup(TddlOperatorTable.UNCOMPRESS.getName());

        // don't have function: UNCOMPRESSED_LENGTH, UPDATE_XML, VALIDATE_PASSWORD_STRENGTH,
        // WAIT_FOR_EXECUTED_GTID_SET,WEIGHT_STRING,LOGICAL_XOR,BITWISE_INVERSION，WEIGHT_STRING,
        // LOGICAL_XOR,BITWISE_INVERSION

        doTestLookup(TddlOperatorTable.UNHEX.getName());
        doTestLookup(TddlOperatorTable.UNIX_TIMESTAMP.getName());
        doTestLookup(TddlOperatorTable.UPPER.getName());
        doTestLookup(TddlOperatorTable.USER.getName());

        doTestLookup(TddlOperatorTable.UUID.getName());
        doTestLookup(TddlOperatorTable.UUID_SHORT.getName());
        doTestLookup(TddlOperatorTable.UUID_TO_BIN.getName());
        doTestLookup(TddlOperatorTable.VALUES.getName());
        doTestLookup(TddlOperatorTable.VAR_POP.getName());
        doTestLookup(TddlOperatorTable.VAR_SAMP.getName());
        doTestLookup(TddlOperatorTable.VARIANCE.getName());
        doTestLookup(TddlOperatorTable.VERSION.getName());

        doTestLookup(TddlOperatorTable.BITWISE_OR.getName());
    }

    private void doTestLookup(String operatorStr) {
        SqlOperator sqlOperator = FoldFunctionUtils.lookup(operatorStr);
        Assert.assertTrue(sqlOperator.getName().equalsIgnoreCase(operatorStr));
    }
}

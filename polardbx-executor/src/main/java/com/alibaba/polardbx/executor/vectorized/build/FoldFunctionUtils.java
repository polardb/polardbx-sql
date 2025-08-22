package com.alibaba.polardbx.executor.vectorized.build;

import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexSystemVar;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexUserVar;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlPostfixOperator;
import org.apache.calcite.sql.SqlPrefixOperator;
import org.apache.calcite.sql.fun.SqlCaseOperator;
import org.apache.calcite.sql.fun.SqlLikeOperator;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This is a list of all MySQL supported expressions organized as Java comments.
 * <p>
 * Operators:
 * &          Bitwise AND
 * >          Greater than operator
 * >>         Right shift
 * >=         Greater than or equal operator
 * <          Less than operator
 * <> , !=    Not equal operator
 * <<         Left shift
 * <=         Less than or equal operator
 * <=>        NULL-safe equal to operator
 * %, MOD     Modulo operator
 * *          Multiplication operator
 * +          Addition operator
 * -          Minus operator
 * -          Change the sign of the argument
 * ->         Return value from JSON column after evaluating path; equivalent to JSON_EXTRACT().
 * ->>        Return value from JSON column after evaluating path and unquoting the result.
 * /          Division operator
 * :=         Assign a value
 * =          Assign a value (as part of a SET statement, or as part of the SET clause in an UPDATE statement)
 * =          Equal operator
 * ^          Bitwise XOR
 * <p>
 * Functions:
 * ABS()                          Return the absolute value
 * ACOS()                         Return the arc cosine
 * ADDDATE()                      Add time values (intervals) to a date value
 * ADDTIME()                      Add time
 * AES_DECRYPT()                 Decrypt using AES
 * AES_ENCRYPT()                 Encrypt using AES
 * AND, &&                        Logical AND
 * ANY_VALUE()                   Suppress ONLY_FULL_GROUP_BY value rejection
 * ASCII()                        Return numeric value of left-most character
 * ASIN()                         Return the arc sine
 * ATAN()                         Return the arc tangent
 * ATAN2(), ATAN()               Return the arc tangent of the two arguments
 * AVG()                          Return the average value of the argument
 * BENCHMARK()                    Repeatedly execute an expression
 * BETWEEN ... AND ...           Whether a value is within a range of values
 * BIN()                          Return a string containing binary representation of a number
 * BIN_TO_UUID()                 Convert binary UUID to string
 * BINARY                         Cast a string to a binary string (Deprecated)
 * BIT_AND()                    Return bitwise AND
 * BIT_COUNT()                  Return the number of bits that are set
 * BIT_LENGTH()                 Return length of argument in bits
 * BIT_OR()                     Return bitwise OR
 * BIT_XOR()                    Return bitwise XOR
 * CASE                          Case operator
 * CAST()                        Cast a value as a certain type
 * CEIL()                        Return the smallest integer value not less than the argument
 * CEILING()                     Return the smallest integer value not less than the argument
 * CHAR()                        Return the character for each integer passed
 * CHAR_LENGTH()                 Return number of characters in argument
 * CHARACTER_LENGTH()           Synonym for CHAR_LENGTH()
 * CHARSET()                     Return the character set of the argument
 * COALESCE()                    Return the first non-NULL argument
 * COLLATION()                   Return the collation of the string argument
 * COMPRESS()                    Return result as a binary string
 * CONCAT()                       Return concatenated string
 * CONCAT_WS()                   Return concatenate with separator
 * CONNECTION_ID()               Return the connection ID (thread ID) for the connection
 * CONV()                         Convert numbers between different number bases
 * CONVERT()                      Cast a value as a certain type
 * CONVERT_TZ()                  Convert from one time zone to another
 * COS()                          Return the cosine
 * COT()                          Return the cotangent
 * COUNT()                        Return a count of the number of rows returned
 * COUNT(DISTINCT)              Return the count of a number of different values
 * CRC32()                       Compute a cyclic redundancy check value
 * CUME_DIST()                   Cumulative distribution value
 * CURDATE()                     Return the current date
 * CURRENT_DATE(), CURRENT_DATE  Synonyms for CURDATE()
 * CURRENT_ROLE()                Return the current active roles
 * CURRENT_TIME(), CURRENT_TIME  Synonyms for CURTIME()
 * CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP  Synonyms for NOW()
 * CURRENT_USER(), CURRENT_USER   The authenticated user name and host name
 * CURTIME()                     Return the current time
 * DATABASE()                    Return the default (current) database name
 * DATE()                        Extract the date part of a date or datetime expression
 * DATE_ADD()                    Add time values (intervals) to a date value
 * DATE_FORMAT()                 Format date as specified
 * DATE_SUB()                    Subtract a time value (interval) from a date
 * DATEDIFF()                    Subtract two dates
 * DAY()                         Synonym for DAYOFMONTH()
 * DAYNAME()                     Return the name of the weekday
 * DAYOFMONTH()                  Return the day of the month (0-31)
 * DAYOFWEEK()                   Return the weekday index of the argument
 * DAYOFYEAR()                   Return the day of the year (1-366)
 * DEFAULT()                     Return the default value for a table column
 * DEGREES()                     Convert radians to degrees
 * DENSE_RANK()                  Rank of current row within its partition, without gaps
 * DIV()                         Integer division
 * ELT()                         Return string at index number
 * EXISTS()                      Whether the result of a query contains any rows
 * EXP()                         Raise to the power of
 * EXPORT_SET()                  Return a string such that for every bit set in the value bits, you get an on string
 * EXTRACT()                     Extract part of a date
 * FIELD()                       Index (position) of first argument in subsequent arguments
 * FIND_IN_SET()                 Index (position) of first argument within second argument
 * FIRST_VALUE()                 Value of argument from first row of window frame
 * FLOOR()                       Return the largest integer value not greater than the argument
 * FORMAT()                      Return a number formatted to specified number of decimal places
 * FORMAT_BYTES()                Convert byte count to value with units
 * FORMAT_PICO_TIME()            Convert time in picoseconds to value with units
 * FOUND_ROWS()                  For a SELECT with a LIMIT clause, the number of rows that would be returned
 * FROM_DAYS()                   Convert a day number to a date
 * FROM_UNIXTIME()               Format Unix timestamp as a date
 * GET_LOCK()                    Get a named lock
 * GREATEST()                    Return the largest argument
 * GROUP_CONCAT()                Return a concatenated string
 * HEX()                         Hexadecimal representation of decimal or string value
 * HOUR()                        Extract the hour
 * IF()                          If/else construct
 * IFNULL()                      Null if/else construct
 * IN()                          Whether a value is within a set of values
 * INET_ATON()                   Return the numeric value of an IP address
 * INET_NTOA()                   Return the IP address from a numeric value
 * INSERT()                     Insert substring at specified position up to specified number of characters
 * INSTR()                      Return the index of the first occurrence of substring
 * INTERVAL()                    Return the index of the argument that is less than the first argument
 * IS()                          Test a value against a boolean
 * IS_FREE_LOCK()                Whether the named lock is free
 * IS NOT                        Test a value against a boolean
 * IS NOT NULL                   NOT NULL value test
 * IS NULL                        NULL value test
 * IS_USED_LOCK()                Whether the named lock is in use; return connection identifier if true
 * IS_UUID()                     Whether argument is a valid UUID
 * ISNULL()                      Test whether the argument is NULL
 * JSON_ARRAY()                  Create JSON array
 * JSON_ARRAY_APPEND()           Append data to JSON document
 * JSON_ARRAY_INSERT()           Insert into JSON array
 * JSON_ARRAYAGG()               Return result set as a single JSON array
 * JSON_CONTAINS()               Whether JSON document contains specific object at path
 * JSON_CONTAINS_PATH()         Whether JSON document contains any data at path
 * JSON_DEPTH()                  Maximum depth of JSON document
 * JSON_EXTRACT()                Return data from JSON document
 * JSON_INSERT()                 Insert data into JSON document
 * JSON_KEYS()                   Array of keys from JSON document
 * JSON_LENGTH()                 Number of elements in JSON document
 * JSON_MERGE()                  Merge JSON documents, preserving duplicate keys. Deprecated synonym
 * JSON_MERGE_PATCH()            Merge JSON documents, replacing values of duplicate keys
 * JSON_MERGE_PRESERVE()         Merge JSON documents, preserving duplicate keys
 * JSON_OBJECT()                 Create JSON object
 * JSON_OBJECTAGG()              Return result set as a single JSON object
 * JSON_OVERLAPS()               Compares two JSON documents, returns TRUE (1) if these have any key-value pairs or array elements in common
 * JSON_PRETTY()                 Print a JSON document in human-readable format
 * JSON_QUOTE()                  Quote JSON document
 * JSON_REMOVE()                 Remove data from JSON document
 * JSON_REPLACE()                Replace values in JSON document
 * JSON_SCHEMA_VALID()           Validate JSON document against JSON schema
 * JSON_SCHEMA_VALIDATION_REPORT() Validate JSON document against JSON schema; returns report in JSON format
 * JSON_SEARCH()                 Path to value within JSON document
 * JSON_SET()                    Insert data into JSON document
 * JSON_STORAGE_FREE()           Freed space within binary representation of JSON column value following partial update
 * JSON_STORAGE_SIZE()           Space used for storage of binary representation of a JSON document
 * JSON_TABLE()                  Return data from a JSON expression as a relational table
 * JSON_TYPE()                   Type of JSON value
 * JSON_UNQUOTE()                Unquote JSON value
 * JSON_VALID()                  Whether JSON value is valid
 * JSON_VALUE()                  Extract value from JSON document at location pointed to by path provided
 * LAG()                         Value of argument from row lagging current row within partition
 * LAST_DAY                      Return the last day of the month for the argument
 * LAST_INSERT_ID()              Value of the AUTOINCREMENT column for the last INSERT
 * LAST_VALUE()                  Value of argument from last row of window frame
 * LCASE()                       Synonym for LOWER()
 * LEAD()                        Value of argument from row leading current row within partition
 * LEAST()                       Return the smallest argument
 * LEFT()                        Return the leftmost number of characters as specified
 * LENGTH()                      Return the length of a string in bytes
 * LIKE()                        Simple pattern matching
 * LINESTRING()                  Construct LineString from Point values
 * LN()                          Return the natural logarithm of the argument
 * LOAD_FILE()                   Load the named file
 * LOCALTIME(), LOCALTIME       Synonym for NOW()
 * LOCALTIMESTAMP, LOCALTIMESTAMP() Synonym for NOW()
 * LOCATE()                      Return the position of the first occurrence of substring
 * LOG()                         Return the natural logarithm of the first argument
 * LOG10()                       Return the base-10 logarithm of the argument
 * LOG2()                        Return the base-2 logarithm of the argument
 * LOWER()                       Return the argument in lowercase
 * LPAD()                        Return the string argument, left-padded with the specified string
 * LTRIM()                       Remove leading spaces
 * MAKE_SET()                    Return a set of comma-separated strings that have the corresponding bit in bits set
 * MAKEDATE()                    Create a date from the year and day of year
 * MAKETIME()                    Create time from hour, minute, second
 * MATCH()                       Perform full-text search
 * MAX()                         Return the maximum value
 * MD5()                         Calculate MD5 checksum
 * MEMBER OF()                   Returns true (1) if first operand matches any element of JSON array passed as second operand
 * MICROSECOND()                 Return the microseconds from argument
 * MID()                         Return a substring starting from the specified position
 * MIN()                         Return the minimum value
 * MINUTE()                      Return the minute from the argument
 * MOD()                         Return the remainder
 * MONTH()                       Return the month from the date passed
 * MONTHNAME()                   Return the name of the month
 * MULTILINESTRING()             Construct MultiLineString from LineString values
 * MULTIPOINT()                  Construct MultiPoint from Point values
 * MULTIPOLYGON()                Construct MultiPolygon from Polygon values
 * NAME_CONST()                  Cause the column to have the given name
 * NOT, !                        Negates value
 * NOT BETWEEN ... AND ...       Whether a value is not within a range of values
 * NOT EXISTS()                  Whether the result of a query contains no rows
 * NOT IN()                      Whether a value is not within a set of values
 * NOT LIKE                       Negation of simple pattern matching
 * NOT REGEXP                     Negation of REGEXP
 * NOW()                         Return the current date and time
 * NTH_VALUE()                   Value of argument from N-th row of window frame
 * NTILE()                       Bucket number of current row within its partition.
 * NULLIF()                      Return NULL if expr1 = expr2
 * OCT()                         Return a string containing octal representation of a number
 * OCTET_LENGTH()                Synonym for LENGTH()
 * OR, ||                        Logical OR
 * ORD()                         Return character code for leftmost character of the argument
 * PERCENT_RANK()                Percentage rank value
 * PERIOD_ADD()                  Add a period to a year-month
 * PERIOD_DIFF()                 Return the number of months between periods
 * PI()                          Return the value of pi
 * POINT()                       Construct Point from coordinates
 * POLYGON()                     Construct Polygon from LineString arguments
 * POSITION()                    Synonym for LOCATE()
 * POW()                         Return the argument raised to the specified power
 * POWER()                       Return the argument raised to the specified power
 * QUARTER()                     Return the quarter from a date argument
 * QUOTE()                       Escape the argument for use in an SQL statement
 * RADIANS()                     Return argument converted to radians
 * RAND()                        Return a random floating-point value
 * RANDOM_BYTES()                Return a random byte vector
 * RANK()                        Rank of current row within its partition, with gaps
 * REGEXP                        Whether string matches regular expression
 * REGEXP_INSTR()                Starting index of substring matching regular expression
 * REGEXP_LIKE()                 Whether string matches regular expression
 * REGEXP_REPLACE()              Replace substrings matching regular expression
 * REGEXP_SUBSTR()               Return substring matching regular expression
 * RELEASE_ALL_LOCKS()           Release all current named locks
 * RELEASE_LOCK()                Release the named lock
 * REPEAT()                      Repeat a string the specified number of times
 * REPLACE()                     Replace occurrences of a specified string
 * REVERSE()                     Reverse the characters in a string
 * RIGHT()                       Return the specified rightmost number of characters
 * RLIKE                         Whether string matches regular expression
 * ROW_COUNT()                   The number of rows updated
 * ROW_NUMBER()                  Number of current row within its partition
 * RPAD()                        Append string the specified number of times
 * RTRIM()                       Remove trailing spaces
 * SCHEMA()                      Synonym for DATABASE()
 * SEC_TO_TIME()                 Converts seconds to 'hh:mm:ss' format
 * SECOND()                      Return the second (0-59)
 * SESSION_USER()                Synonym for USER()
 * SHA1(), SHA()                 Calculate an SHA-1 160-bit checksum
 * SHA2()                        Calculate an SHA-2 checksum
 * SIGN()                        Return the sign of the argument
 * SIN()                         Return the sine of the argument
 * SLEEP()                       Sleep for a number of seconds
 * SOUNDEX()                     Return a soundex string
 * SOUNDS LIKE                   Compare sounds
 * SPACE()                       Return a string of the specified number of spaces
 * SQRT()                        Return the square root of the argument
 * ST_Area()                     Return Polygon or MultiPolygon area
 * ST_AsBinary(), ST_AsWKB()    Convert from internal geometry format to WKB
 * ST_AsGeoJSON()                Generate GeoJSON object from geometry
 * ST_AsText(), ST_AsWKT()      Convert from internal geometry format to WKT
 * ST_Buffer()                   Return geometry of points within given distance from geometry
 * ST_Centroid()                 Return centroid as a point
 * ST_Collect()                  Aggregate spatial values into collection
 * ST_Contains()                 Whether one geometry contains another
 * ST_ConvexHull()               Return convex hull of geometry
 * ST_Crosses()                  Whether one geometry crosses another
 * ST_Difference()               Return point set difference of two geometries
 * ST_Disjoint()                 Whether one geometry is disjoint from another
 * ST_Distance()                 The distance of one geometry from another
 * ST_EndPoint()                 End Point of LineString
 * ST_Envelope()                 Return MBR of geometry
 * ST_Equals()                  Whether one geometry is equal to another
 * ST_ExteriorRing()            Return exterior ring of Polygon
 * ST_FrechetDistance()         The discrete Fréchet distance of one geometry from another
 * ST_GeoHash()                 Produce a geohash value
 * ST_GeomCollFromText()        Return geometry collection from WKT
 * ST_GeomFromText()            Construct Point from WKT
 * ST_GeomFromWKB()             Construct Point from WKB
 * ST_HausdorffDistance()       The discrete Hausdorff distance of one geometry from another
 * ST_InteriorRingN()           Return N-th interior ring of Polygon
 * ST_Intersection()            Return point set intersection of two geometries
 * ST_Intersects()              Whether one geometry intersects another
 * ST_IsClosed()                Whether a geometry is closed and simple
 * ST_IsEmpty()                 Whether a geometry is empty
 * ST_IsValid()                 Whether a geometry is valid
 * ST_NumGeometries()           Return number of geometries in geometry collection
 * ST_NumInteriorRing()         Return number of interior rings in Polygon
 * ST_NumPoints()               Return number of points in LineString
 * ST_Overlaps()                Whether one geometry overlaps another
 * ST_PointAtDistance()         The point a given distance along a LineString
 * ST_PointFromGeoHash()       Convert geohash value to POINT value
 * ST_PointFromText()          Construct Point from WKT
 * ST_PointFromWKB()           Construct Point from WKB
 * ST_PointN()                 Return N-th point from LineString
 * ST_PolyFromText()           Construct Polygon from WKT
 * ST_PolyFromWKB()           Construct Polygon from WKB
 * ST_Simplify()               Return simplified geometry
 * ST_SRID()                   Return spatial reference system ID for geometry
 * ST_StartPoint()              Start Point of LineString
 * ST_Transform()               Transform coordinates of geometry
 * ST_Union()                   Return point set union of two geometries
 * ST_Validate()                Return validated geometry
 * ST_Within()                 Whether one geometry is within another
 * ST_X()                       Return X coordinate of Point
 * ST_Y()                       Return Y coordinate of Point
 * STATEMENT_DIGEST()          Compute statement digest hash value
 * STATEMENT_DIGEST_TEXT()     Compute normalized statement digest
 * STD()                       Return the population standard deviation
 * STDDEV()                   Return the population standard deviation
 * STDDEV_POP()               Return the population standard deviation
 * STDDEV_SAMP()              Return the sample standard deviation
 * STR_TO_DATE()              Convert a string to a date
 * STRCMP()                   Compare two strings
 * SUBDATE()                  Synonym for DATE_SUB() when invoked with three arguments
 * SUBSTR()                   Return the substring as specified
 * SUBSTRING()                Return the substring as specified
 * SUBSTRING_INDEX()          Return a substring from a string before the specified number of occurrences of the delimiter
 * SUM()                      Return the sum
 * SYSDATE()                 Return the time at which the function executes
 * SYSTEM_USER()             Synonym for USER()
 * TAN()                      Return the tangent of the argument
 * TIME()                     Extract the time portion of the expression passed
 * TIME_FORMAT()              Format as time
 * TIME_TO_SEC()              Return the argument converted to seconds
 * TIMEDIFF()                 Subtract time
 * TIMESTAMP()                With a single argument, this function returns the date or datetime expression
 * TIMESTAMPADD()             Add an interval to a datetime expression
 * TIMESTAMPDIFF()            Return the difference of two datetime expressions, using the units specified
 * TO_DAYS()                  Return the date argument converted to days
 * TO_SECONDS()               Return the date or datetime argument converted to seconds since Year 0
 * TRIM()                     Remove leading and trailing spaces
 * TRUNCATE()                 Truncate to specified number of decimal places
 * UCASE()                    Synonym for UPPER()
 * UNCOMPRESS()               Uncompress a string compressed
 * UNCOMPRESSED_LENGTH()      Return the length of a string before compression
 * UNHEX()                    Return a string containing hex representation of a number
 * UNIX_TIMESTAMP()           Return a Unix timestamp
 * UPDATE_XML()               Return replaced XML fragment
 * UPPER()                    Convert to uppercase
 * USER()                     The user name and host name provided by the client
 * UTC_DATE()                 Return the current UTC date
 * UTC_TIME()                  Return the current UTC time
 * UTC_TIMESTAMP()            Return the current UTC date and time
 * UUID()                     Return a Universal Unique Identifier (UUID)
 * UUID_SHORT()               Return an integer-valued universal identifier
 * UUID_TO_BIN()              Convert string UUID to binary
 * VALIDATE_PASSWORD_STRENGTH() Determine strength of password
 * VALUES()                   Define the values to be used during an INSERT
 * VAR_POP()                  Return the population standard variance
 * VAR_SAMP()                 Return the sample variance
 * VARIANCE()                 Return the population standard variance
 * VERSION()                  Return a string that indicates the MySQL server version
 * WAIT_FOR_EXECUTED_GTID_SET() Wait until the given GTIDs have executed on the replica.
 * WEEK()                     Return the week number
 * WEEKDAY()                  Return the weekday index
 * WEEKOFYEAR()               Return the calendar week of the date (1-53)
 * WEIGHT_STRING()            Return the weight string for a string
 * XOR                        Logical XOR
 * YEAR()                     Return the year
 * YEARWEEK()                 Return the year and week
 * |                          Bitwise OR
 * ~                          Bitwise inversion
 */
public class FoldFunctionUtils {
    private static Set<SqlOperator> FUNCTION_BLACK_LIST = new HashSet<>();
    private static Set<SqlOperator> FUNCTION_WHITE_LIST = new HashSet<>();

    static {
        initWhiteList();
        initBlackList();
    }

    public static boolean isFoldEnabled(RexNode node,
                                        List<SqlOperator> extraBlackList,
                                        List<SqlOperator> extraWhiteList) {
        RexVisitor<Boolean> checkVisitor = new CheckVisitor(extraBlackList, extraWhiteList);
        return node.accept(checkVisitor);
    }

    public static SqlOperator lookup(String operatorName) {
        for (SqlOperator sqlOperator : FUNCTION_BLACK_LIST) {
            if (sqlOperator.getName().equalsIgnoreCase(operatorName)) {
                return sqlOperator;
            }
        }

        for (SqlOperator sqlOperator : FUNCTION_WHITE_LIST) {
            if (sqlOperator.getName().equalsIgnoreCase(operatorName)) {
                return sqlOperator;
            }
        }
        return null;
    }

    static class CheckVisitor implements RexVisitor<Boolean> {
        List<SqlOperator> extraBlackList;
        List<SqlOperator> extraWhiteList;

        public CheckVisitor(List<SqlOperator> extraBlackList, List<SqlOperator> extraWhiteList) {
            this.extraBlackList = extraBlackList;
            this.extraWhiteList = extraWhiteList;
        }

        public Boolean visitLiteral(RexLiteral literal) {
            return true;
        }

        public Boolean visitInputRef(RexInputRef inputRef) {
            return false;
        }

        public Boolean visitLocalRef(RexLocalRef localRef) {
            return false;
        }

        public Boolean visitOver(RexOver over) {
            return false;
        }

        public Boolean visitSubQuery(RexSubQuery subQuery) {
            return false;
        }

        @Override
        public void visit(SqlBinaryOperator operator, RexCall call) {
            // default
        }

        @Override
        public void visit(SqlOperator operator, RexCall call) {
            // default
        }

        @Override
        public void visit(AggregateCall aggregateCall) {
            // default
        }

        @Override
        public void visit(SqlFunction operator, RexCall call) {
            // default
        }

        @Override
        public void visit(SqlPrefixOperator operator, RexCall call) {
            // default
        }

        @Override
        public void visit(SqlCaseOperator operator, RexCall call) {
            // default
        }

        @Override
        public void visit(SqlPostfixOperator operator, RexCall call) {
            // default
        }

        @Override
        public void visit(SqlLikeOperator operator, RexCall call) {
            // default
        }

        @Override
        public Boolean visitTableInputRef(RexTableInputRef ref) {
            return false;
        }

        @Override
        public Boolean visitPatternFieldRef(RexPatternFieldRef fieldRef) {
            return false;
        }

        @Override
        public Boolean visitSystemVar(RexSystemVar systemVar) {
            return false;
        }

        @Override
        public Boolean visitUserVar(RexUserVar userVar) {
            return false;
        }

        public Boolean visitCorrelVariable(RexCorrelVariable correlVariable) {
            // Correlating variables change when there is an internal restart.
            // Not good enough for our purposes.
            return false;
        }

        public Boolean visitDynamicParam(RexDynamicParam dynamicParam) {
            // Dynamic parameters are constant WITHIN AN EXECUTION, so that's
            // good enough.
            return true;
        }

        public Boolean visitCall(RexCall call) {
            // Constant if operator is deterministic and all operands are
            // constant.

            SqlOperator operator = call.getOperator();

            // Don't allow the functions whose results are not deterministic
            boolean isDeterministic = call.getOperator().isDeterministic();

            // Check the operators
            boolean isAllowedOperator = isAllowedOperator(operator);

            // Check children.
            boolean verifyAnd = RexVisitorImpl.visitArrayAnd(this, call.getOperands());

            return isDeterministic && isAllowedOperator && verifyAnd;
        }

        private boolean isAllowedOperator(SqlOperator operator) {
            // According to initial black list and white list.
            boolean isAllowedOperator = !FUNCTION_BLACK_LIST.contains(operator)
                && FUNCTION_WHITE_LIST.contains(operator);

            // Forced black list.
            if (extraBlackList != null && !extraBlackList.isEmpty()) {
                isAllowedOperator &= !extraBlackList.contains(operator);
            }

            // Forced white list.
            if (extraWhiteList != null && !extraWhiteList.isEmpty()) {
                isAllowedOperator |= extraWhiteList.contains(operator);
            }
            return isAllowedOperator;
        }

        public Boolean visitRangeRef(RexRangeRef rangeRef) {
            return false;
        }

        public Boolean visitFieldAccess(RexFieldAccess fieldAccess) {
            // "<expr>.FIELD" is constant if "<expr>" is constant.
            return fieldAccess.getReferenceExpr().accept(this);
        }
    }

    private static void initWhiteList() {
        FUNCTION_WHITE_LIST.add(TddlOperatorTable.GREATER_THAN);
        FUNCTION_WHITE_LIST.add(TddlOperatorTable.GREATER_THAN_OR_EQUAL);
        FUNCTION_WHITE_LIST.add(TddlOperatorTable.LESS_THAN);
        FUNCTION_WHITE_LIST.add(TddlOperatorTable.NOT_EQUALS);
        FUNCTION_WHITE_LIST.add(TddlOperatorTable.INTERVAL_PRIMARY);

        FUNCTION_WHITE_LIST.add(TddlOperatorTable.LESS_THAN_OR_EQUAL);
        FUNCTION_WHITE_LIST.add(TddlOperatorTable.NULL_SAFE_EQUAL);
        FUNCTION_WHITE_LIST.add(TddlOperatorTable.MOD);
        FUNCTION_WHITE_LIST.add(TddlOperatorTable.MULTIPLY);
        FUNCTION_WHITE_LIST.add(TddlOperatorTable.PLUS);
        FUNCTION_WHITE_LIST.add(TddlOperatorTable.MINUS);

        FUNCTION_WHITE_LIST.add(TddlOperatorTable.DIVIDE);
        FUNCTION_WHITE_LIST.add(TddlOperatorTable.EQUALS);

        FUNCTION_WHITE_LIST.add(TddlOperatorTable.ABS);

        FUNCTION_WHITE_LIST.add(TddlOperatorTable.ADDDATE);
        FUNCTION_WHITE_LIST.add(TddlOperatorTable.ADDTIME);

        FUNCTION_WHITE_LIST.add(TddlOperatorTable.BETWEEN);

        FUNCTION_WHITE_LIST.add(TddlOperatorTable.CASE);
        FUNCTION_WHITE_LIST.add(TddlOperatorTable.CEIL);
        FUNCTION_WHITE_LIST.add(TddlOperatorTable.CEILING);
        FUNCTION_WHITE_LIST.add(TddlOperatorTable.COALESCE);
        FUNCTION_WHITE_LIST.add(TddlOperatorTable.CONCAT);

        FUNCTION_WHITE_LIST.add(TddlOperatorTable.DATE);
        FUNCTION_WHITE_LIST.add(TddlOperatorTable.DATE_ADD);
        FUNCTION_WHITE_LIST.add(TddlOperatorTable.DATE_FORMAT);
        FUNCTION_WHITE_LIST.add(TddlOperatorTable.DATE_SUB);
        FUNCTION_WHITE_LIST.add(TddlOperatorTable.DATEDIFF);
        FUNCTION_WHITE_LIST.add(TddlOperatorTable.DAY);
        FUNCTION_WHITE_LIST.add(TddlOperatorTable.DAYNAME);
        FUNCTION_WHITE_LIST.add(TddlOperatorTable.DAYOFMONTH);
        FUNCTION_WHITE_LIST.add(TddlOperatorTable.DAYOFWEEK);
        FUNCTION_WHITE_LIST.add(TddlOperatorTable.DAYOFYEAR);
        FUNCTION_WHITE_LIST.add(TddlOperatorTable.DIVIDE_INTEGER);
        FUNCTION_WHITE_LIST.add(TddlOperatorTable.EXTRACT);
        FUNCTION_WHITE_LIST.add(TddlOperatorTable.FLOOR);
        FUNCTION_WHITE_LIST.add(TddlOperatorTable.FROM_DAYS);

        FUNCTION_WHITE_LIST.add(TddlOperatorTable.IF);
        FUNCTION_WHITE_LIST.add(TddlOperatorTable.IFNULL);
        FUNCTION_WHITE_LIST.add(TddlOperatorTable.IN);

        FUNCTION_WHITE_LIST.add(TddlOperatorTable.IS_TRUE);
        FUNCTION_WHITE_LIST.add(TddlOperatorTable.IS_NOT_TRUE);
        FUNCTION_WHITE_LIST.add(TddlOperatorTable.IS_NOT_NULL);
        FUNCTION_WHITE_LIST.add(TddlOperatorTable.IS_NULL);
        FUNCTION_WHITE_LIST.add(TddlOperatorTable.ISNULL);

        FUNCTION_WHITE_LIST.add(TddlOperatorTable.LIKE);

        FUNCTION_WHITE_LIST.add(TddlOperatorTable.NOT);
        FUNCTION_WHITE_LIST.add(TddlOperatorTable.NOT_BETWEEN);
        FUNCTION_WHITE_LIST.add(TddlOperatorTable.NOT_EXISTS);
        FUNCTION_WHITE_LIST.add(TddlOperatorTable.NOT_IN);
        FUNCTION_WHITE_LIST.add(TddlOperatorTable.NOT_LIKE);

        FUNCTION_WHITE_LIST.add(TddlOperatorTable.NULLIF);
        FUNCTION_WHITE_LIST.add(TddlOperatorTable.OR);

        FUNCTION_WHITE_LIST.add(TddlOperatorTable.PERIODADD);
        FUNCTION_WHITE_LIST.add(TddlOperatorTable.PERIODDIFF);

        FUNCTION_WHITE_LIST.add(TddlOperatorTable.PERCENT_REMAINDER);

        FUNCTION_WHITE_LIST.add(TddlOperatorTable.POW);
        FUNCTION_WHITE_LIST.add(TddlOperatorTable.POWER);
        FUNCTION_WHITE_LIST.add(TddlOperatorTable.REGEXP);

        FUNCTION_WHITE_LIST.add(TddlOperatorTable.SECOND);
        FUNCTION_WHITE_LIST.add(TddlOperatorTable.STRTODATE);
        FUNCTION_WHITE_LIST.add(TddlOperatorTable.STRCMP);
        FUNCTION_WHITE_LIST.add(TddlOperatorTable.SUBDATE);
        FUNCTION_WHITE_LIST.add(TddlOperatorTable.SUBSTR);
        FUNCTION_WHITE_LIST.add(TddlOperatorTable.SUBSTRING);
        FUNCTION_WHITE_LIST.add(TddlOperatorTable.SUBSTRING_INDEX);

        FUNCTION_WHITE_LIST.add(TddlOperatorTable.WEEK);
        FUNCTION_WHITE_LIST.add(TddlOperatorTable.WEEKDAY);
        FUNCTION_WHITE_LIST.add(TddlOperatorTable.WEEKOFYEAR);

        FUNCTION_WHITE_LIST.add(TddlOperatorTable.YEAR);
        FUNCTION_WHITE_LIST.add(TddlOperatorTable.YEARWEEK);
    }

    private static void initBlackList() {
        // Non-mysql Functions
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.PART_ROUTE);

        // Information Functions
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.CONNECTION_ID);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.CURRENT_ROLE);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.CURRENT_USER);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.DATABASE);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.FOUND_ROWS);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.ROW_COUNT);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.SCHEMA);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.SESSION_USER);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.SYSTEM_USER);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.USER);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.VERSION);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.TSO_TIMESTAMP);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.SPECIAL_POW);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.CAN_ACCESS_TABLE);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.GET_LOCK);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.RELEASE_LOCK);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.RELEASE_ALL_LOCKS);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.IS_FREE_LOCK);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.IS_USED_LOCK);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.HYPERLOGLOG);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.PART_HASH);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.LBAC_CHECK);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.LBAC_READ);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.LBAC_WRITE);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.LBAC_WRITE_STRICT_CHECK);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.LBAC_USER_WRITE_LABEL);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.PRE_FILTER);

        // Time Function
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.CURDATE);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.CURRENT_DATE);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.CURRENT_TIME);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.CURRENT_TIMESTAMP);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.CURTIME);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.LOCALTIME);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.LOCALTIMESTAMP);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.NOW);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.SYSDATE);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.UNIX_TIMESTAMP);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.UTC_DATE);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.UTC_TIME);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.TSO_TIMESTAMP);

        // Numeric Functions
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.RAND);

        // Miscellaneous Functions
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.UUID);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.UUID_SHORT);

        // Information Functions
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.CONNECTION_ID);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.CURRENT_ROLE);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.CURRENT_USER);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.DATABASE);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.FOUND_ROWS);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.LAST_INSERT_ID);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.ROW_COUNT);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.SCHEMA);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.SESSION_USER);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.SYSTEM_USER);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.USER);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.VERSION);

        // Locking function
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.GET_LOCK);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.IS_FREE_LOCK);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.IS_USED_LOCK);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.RELEASE_ALL_LOCKS);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.RELEASE_LOCK);

        // Benchmark function
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.BENCHMARK);

        // cast function
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.CAST);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.CONVERT);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.IMPLICIT_CAST);

        // all functions
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.BITWISE_AND);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.BITRSHIFT);

        FUNCTION_BLACK_LIST.add(TddlOperatorTable.BITLSHIFT);

        FUNCTION_BLACK_LIST.add(TddlOperatorTable.JSON_EXTRACT);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.JSON_UNQUOTE);

        FUNCTION_BLACK_LIST.add(TddlOperatorTable.ASSIGNMENT);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.BITWISE_XOR);

        FUNCTION_BLACK_LIST.add(TddlOperatorTable.ACOS);

        FUNCTION_BLACK_LIST.add(TddlOperatorTable.AES_DECRYPT);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.AES_ENCRYPT);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.BITWISE_AND);

        FUNCTION_BLACK_LIST.add(TddlOperatorTable.ASCII);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.ASIN);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.ATAN);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.ATAN2);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.AVG);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.BENCHMARK);

        FUNCTION_BLACK_LIST.add(TddlOperatorTable.BIN);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.BIN_TO_UUID);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.BINARY);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.BIT_AND);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.BIT_COUNT);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.BIT_LENGTH);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.BIT_OR);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.BIT_XOR);

        FUNCTION_BLACK_LIST.add(TddlOperatorTable.CAST);

        FUNCTION_BLACK_LIST.add(TddlOperatorTable.CHAR);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.CHAR_LENGTH);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.CHARACTER_LENGTH);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.CHARSET);

        FUNCTION_BLACK_LIST.add(TddlOperatorTable.COLLATION);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.COMPRESS);

        FUNCTION_BLACK_LIST.add(TddlOperatorTable.CONCAT_WS);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.CONNECTION_ID);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.CONV);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.CONVERT);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.CONVERT_TZ);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.COS);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.COT);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.COUNT);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.CRC32);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.CUME_DIST);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.CURDATE);

        FUNCTION_BLACK_LIST.add(TddlOperatorTable.CURRENT_DATE);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.CURRENT_ROLE);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.CURRENT_TIME);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.CURRENT_TIMESTAMP);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.CURRENT_USER);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.CURTIME);

        FUNCTION_BLACK_LIST.add(TddlOperatorTable.DATABASE);

        FUNCTION_BLACK_LIST.add(TddlOperatorTable.DEFAULT);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.DEGREES);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.DENSE_RANK);

        FUNCTION_BLACK_LIST.add(TddlOperatorTable.ELT);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.EXISTS);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.EXP);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.EXPORTSET);

        FUNCTION_BLACK_LIST.add(TddlOperatorTable.FIELD);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.FIND_IN_SET);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.FIRST_VALUE);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.FORMAT);

        FUNCTION_BLACK_LIST.add(TddlOperatorTable.FOUND_ROWS);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.FROM_UNIXTIME);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.GET_LOCK);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.GREATEST);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.GROUP_CONCAT);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.HEX);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.HOUR);

        FUNCTION_BLACK_LIST.add(TddlOperatorTable.INSERT);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.INSTR);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.INTERVAL);

        FUNCTION_BLACK_LIST.add(TddlOperatorTable.IS_FREE_LOCK);

        FUNCTION_BLACK_LIST.add(TddlOperatorTable.IS_USED_LOCK);

        FUNCTION_BLACK_LIST.add(TddlOperatorTable.JSON_ARRAY);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.JSON_ARRAY_APPEND);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.JSON_ARRAY_INSERT);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.JSON_ARRAYAGG);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.JSON_CONTAINS);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.JSON_CONTAINS_PATH);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.JSON_DEPTH);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.JSON_EXTRACT);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.JSON_INSERT);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.JSON_KEYS);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.JSON_LENGTH);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.JSON_MERGE);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.JSON_MERGE_PATCH);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.JSON_MERGE_PRESERVE);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.JSON_OBJECT);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.JSON_OBJECTAGG);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.JSON_OVERLAPS);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.JSON_PRETTY);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.JSON_QUOTE);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.JSON_REMOVE);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.JSON_REPLACE);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.JSON_SEARCH);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.JSON_SET);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.JSON_STORAGE_SIZE);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.JSON_TYPE);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.JSON_UNQUOTE);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.JSON_VALID);

        FUNCTION_BLACK_LIST.add(TddlOperatorTable.LAG);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.LAST_INSERT_ID);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.LAST_VALUE);

        FUNCTION_BLACK_LIST.add(TddlOperatorTable.LEAD);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.LEAST);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.LEFT);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.LENGTH);

        FUNCTION_BLACK_LIST.add(TddlOperatorTable.LineString);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.LN);

        FUNCTION_BLACK_LIST.add(TddlOperatorTable.LOCALTIME);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.LOCALTIMESTAMP);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.LOCATE);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.LOG);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.LOG10);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.LOG2);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.LOWER);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.LPAD);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.LTRIM);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.MAKESET);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.MAKEDATE);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.MAKETIME);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.MATCH_AGAINST);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.MAX);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.MD5);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.MEMBER_OF);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.MICROSECOND);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.MID);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.MIN);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.MINUTE);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.MONTH);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.MONTHNAME);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.MultiLineString);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.MultiPoint);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.MultiPolygon);

        FUNCTION_BLACK_LIST.add(TddlOperatorTable.NOT_REGEXP);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.NOW);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.NTH_VALUE);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.NTILE);

        FUNCTION_BLACK_LIST.add(TddlOperatorTable.OCT);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.OCTET_LENGTH);

        FUNCTION_BLACK_LIST.add(TddlOperatorTable.ORD);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.PERCENT_RANK);

        FUNCTION_BLACK_LIST.add(TddlOperatorTable.PI);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.Point);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.Polygon);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.POSITION);

        FUNCTION_BLACK_LIST.add(TddlOperatorTable.QUARTER);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.QUOTE);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.RADIANS);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.RAND);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.RANDOM_BYTES);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.RANK);

        FUNCTION_BLACK_LIST.add(TddlOperatorTable.RELEASE_ALL_LOCKS);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.RELEASE_LOCK);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.REPEAT);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.REPLACE);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.REVERSE);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.RIGHT);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.RLIKE);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.ROW_COUNT);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.ROW_NUMBER);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.RPAD);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.RTRIM);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.SCHEMA);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.SEC_TO_TIME);

        FUNCTION_BLACK_LIST.add(TddlOperatorTable.SESSION_USER);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.SHA1);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.SHA2);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.SIGN);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.SIN);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.SLEEP);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.SOUNDEX);

        // don't have function: sound_like

        FUNCTION_BLACK_LIST.add(TddlOperatorTable.SPACE);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.SQRT);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.ST_Area);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.ST_AsBinary);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.ST_AsGeoJSON);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.ST_AsText);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.ST_Buffer);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.ST_Centroid);

        // don't have function: ST_Collect

        FUNCTION_BLACK_LIST.add(TddlOperatorTable.ST_Contains);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.ST_ConvexHull);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.ST_Crosses);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.ST_Difference);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.ST_Disjoint);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.ST_Distance);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.ST_EndPoint);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.ST_Envelope);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.ST_Equals);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.ST_ExteriorRing);

        // don't have function: ST_FrechetDistance

        FUNCTION_BLACK_LIST.add(TddlOperatorTable.ST_GeoHash);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.ST_GeomCollFromText);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.ST_GeomFromText);

        // don't have function: ST_GeomFromWKB
        // don't have function: ST_HausdorffDistance

        FUNCTION_BLACK_LIST.add(TddlOperatorTable.ST_InteriorRingN);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.ST_Intersection);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.ST_Intersects);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.ST_IsClosed);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.ST_IsEmpty);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.ST_IsValid);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.ST_NumGeometries);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.ST_NumInteriorRing);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.ST_NumPoints);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.ST_Overlaps);

        // don't have function: ST_PointAtDistance

        FUNCTION_BLACK_LIST.add(TddlOperatorTable.ST_PointFromGeoHash);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.ST_PointFromText);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.ST_PointFromWKB);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.ST_PointN);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.ST_PolyFromText);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.ST_PolyFromWKB);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.ST_Simplify);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.ST_SRID);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.ST_StartPoint);

        // don't have function: ST_Transform

        FUNCTION_BLACK_LIST.add(TddlOperatorTable.ST_Union);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.ST_Validate);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.ST_Within);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.ST_X);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.ST_Y);

        // don't have function: STATEMENT_DIGEST, STATEMENT_DIGEST_TEXT

        FUNCTION_BLACK_LIST.add(TddlOperatorTable.STD);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.STDDEV);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.STDDEV_POP);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.STDDEV_SAMP);

        FUNCTION_BLACK_LIST.add(TddlOperatorTable.SUM);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.SYSDATE);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.SYSTEM_USER);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.TAN);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.TIME);

        FUNCTION_BLACK_LIST.add(TddlOperatorTable.TRIM);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.TRUNCATE);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.UNCOMPRESS);

        // don't have function: UNCOMPRESSED_LENGTH, UPDATE_XML, VALIDATE_PASSWORD_STRENGTH,
        // WAIT_FOR_EXECUTED_GTID_SET,WEIGHT_STRING,LOGICAL_XOR,BITWISE_INVERSION，WEIGHT_STRING,
        // LOGICAL_XOR,BITWISE_INVERSION

        FUNCTION_BLACK_LIST.add(TddlOperatorTable.UNHEX);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.UNIX_TIMESTAMP);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.UPPER);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.USER);

        FUNCTION_BLACK_LIST.add(TddlOperatorTable.UUID);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.UUID_SHORT);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.UUID_TO_BIN);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.VALUES);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.VAR_POP);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.VAR_SAMP);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.VARIANCE);
        FUNCTION_BLACK_LIST.add(TddlOperatorTable.VERSION);

        FUNCTION_BLACK_LIST.add(TddlOperatorTable.BITWISE_OR);
    }

}

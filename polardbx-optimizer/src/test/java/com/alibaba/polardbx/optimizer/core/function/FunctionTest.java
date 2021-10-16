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

package com.alibaba.polardbx.optimizer.core.function;

import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import org.apache.calcite.sql.SqlOperator;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author lingce.ldm 2017-10-26 15:12
 */
public class FunctionTest {

    @Test
    @Ignore
    public void unsupportedFunction() {
        String mysqlFunction = " !,\n" +
            "!=,\n" +
            "%,\n" +
            "&,\n" +
            "*,\n" +
            "+,\n" +
            "- BINARY,\n" +
            "- UNARY,\n" +
            "/,\n" +
            "<,\n" +
            "<<,\n" +
            "<=,\n" +
            "<=>,\n" +
            "=,\n" +
            ">,\n" +
            ">=,\n" +
            ">>,\n" +
            "ABS,\n" +
            "ACOS,\n" +
            "ADDDATE,\n" +
            "ADDTIME,\n" +
            "AES_DECRYPT,\n" +
            "AES_ENCRYPT,\n" +
            "AND,\n" +
            "ANY_VALUE,\n" +
            "ASCII,\n" +
            "ASIN,\n" +
            "ASSIGN-EQUAL,\n" +
            "ASSIGN-VALUE,\n" +
            "ATAN,\n" +
            "ATAN2,\n" +
            "BENCHMARK,\n" +
            "BETWEEN AND,\n" +
            "BIN,\n" +
            "BINARY OPERATOR,\n" +
            "BIT_COUNT,\n" +
            "BIT_LENGTH,\n" +
            "CASE OPERATOR,\n" +
            "CAST,\n" +
            "CEIL,\n" +
            "CEILING,\n" +
            "CHAR FUNCTION,\n" +
            "CHARACTER_LENGTH,\n" +
            "CHARSET,\n" +
            "CHAR_LENGTH,\n" +
            "COALESCE,\n" +
            "COERCIBILITY,\n" +
            "COLLATION,\n" +
            "COMPRESS,\n" +
            "CONCAT,\n" +
            "CONCAT_WS,\n" +
            "CONNECTION_ID,\n" +
            "CONV,\n" +
            "CONVERT,\n" +
            "CONVERT_TZ,\n" +
            "COS,\n" +
            "COT,\n" +
            "CRC32,\n" +
            "CURDATE,\n" +
            "CURRENT_DATE,\n" +
            "CURRENT_TIME,\n" +
            "CURRENT_TIMESTAMP,\n" +
            "CURRENT_USER,\n" +
            "CURTIME,\n" +
            "DATABASE,\n" +
            "DATE FUNCTION,\n" +
            "DATEDIFF,\n" +
            "DATE_ADD,\n" +
            "DATE_FORMAT,\n" +
            "DATE_SUB,\n" +
            "DAY,\n" +
            "DAYNAME,\n" +
            "DAYOFMONTH,\n" +
            "DAYOFWEEK,\n" +
            "DAYOFYEAR,\n" +
            "DECODE,\n" +
            "DEFAULT,\n" +
            "DEGREES,\n" +
            "DES_DECRYPT,\n" +
            "DES_ENCRYPT,\n" +
            "DIV,\n" +
            "ELT,\n" +
            "ENCODE,\n" +
            "ENCRYPT,\n" +
            "EXP,\n" +
            "EXPORT_SET,\n" +
            "EXTRACT,\n" +
            "EXTRACTVALUE,\n" +
            "FIELD,\n" +
            "FIND_IN_SET,\n" +
            "FLOOR,\n" +
            "FORMAT,\n" +
            "FOUND_ROWS,\n" +
            "FROM_BASE64,\n" +
            "FROM_DAYS,\n" +
            "FROM_UNIXTIME,\n" +
            "GET_FORMAT,\n" +
            "GET_LOCK,\n" +
            "GREATEST,\n" +
            "HEX,\n" +
            "HOUR,\n" +
            "IF FUNCTION,\n" +
            "IFNULL,\n" +
            "IN,\n" +
            "INET6_ATON,\n" +
            "INET6_NTOA,\n" +
            "INET_ATON,\n" +
            "INET_NTOA,\n" +
            "INSERT FUNCTION,\n" +
            "INSTR,\n" +
            "INTERVAL,\n" +
            "IS NOT NULL,\n" +
            "IS NOT,\n" +
            "IS NULL,\n" +
            "IS,\n" +
            "ISNULL,\n" +
            "IS_FREE_LOCK,\n" +
            "IS_IPV4,\n" +
            "IS_IPV4_COMPAT,\n" +
            "IS_IPV4_MAPPED,\n" +
            "IS_IPV6,\n" +
            "IS_USED_LOCK,\n" +
            "LAST_DAY,\n" +
            "LAST_INSERT_ID,\n" +
            "LCASE,\n" +
            "LEAST,\n" +
            "LEFT,\n" +
            "LENGTH,\n" +
            "LIKE,\n" +
            "LN,\n" +
            "LOAD_FILE,\n" +
            "LOCALTIME,\n" +
            "LOCALTIMESTAMP,\n" +
            "LOCATE,\n" +
            "LOG,\n" +
            "LOG10,\n" +
            "LOG2,\n" +
            "LOWER,\n" +
            "LPAD,\n" +
            "LTRIM,\n" +
            "MAKEDATE,\n" +
            "MAKETIME,\n" +
            "MAKE_SET,\n" +
            "MASTER_POS_WAIT,\n" +
            "MATCH AGAINST,\n" +
            "MD5,\n" +
            "MICROSECOND,\n" +
            "MID,\n" +
            "MINUTE,\n" +
            "MOD,\n" +
            "MONTH,\n" +
            "MONTHNAME,\n" +
            "NAME_CONST,\n" +
            "NOT BETWEEN,\n" +
            "NOT IN,\n" +
            "NOT LIKE,\n" +
            "NOT REGEXP,\n" +
            "NOW,\n" +
            "NULLIF,\n" +
            "OCT,\n" +
            "OCTET_LENGTH,\n" +
            "OLD_PASSWORD,\n" +
            "OR,\n" +
            "ORD,\n" +
            "PASSWORD,\n" +
            "PERIOD_ADD,\n" +
            "PERIOD_DIFF,\n" +
            "PI,\n" +
            "POSITION,\n" +
            "POW,\n" +
            "POWER,\n" +
            "QUARTER,\n" +
            "QUOTE,\n" +
            "RADIANS,\n" +
            "RAND,\n" +
            "RANDOM_BYTES,\n" +
            "REGEXP,\n" +
            "RELEASE_ALL_LOCKS,\n" +
            "RELEASE_LOCK,\n" +
            "REPEAT FUNCTION,\n" +
            "REPLACE FUNCTION,\n" +
            "REVERSE,\n" +
            "RIGHT,\n" +
            "ROUND,\n" +
            "ROW_COUNT,\n" +
            "RPAD,\n" +
            "RTRIM,\n" +
            "SCHEMA,\n" +
            "SECOND,\n" +
            "SEC_TO_TIME,\n" +
            "SESSION_USER,\n" +
            "SHA1,\n" +
            "SHA2,\n" +
            "SIGN,\n" +
            "SIN,\n" +
            "SLEEP,\n" +
            "SOUNDEX,\n" +
            "SOUNDS LIKE,\n" +
            "SPACE,\n" +
            "SQRT,\n" +
            "STRCMP,\n" +
            "STR_TO_DATE,\n" +
            "SUBDATE,\n" +
            "SUBSTR,\n" +
            "SUBSTRING,\n" +
            "SUBSTRING_INDEX,\n" +
            "SUBTIME,\n" +
            "SYSDATE,\n" +
            "SYSTEM_USER,\n" +
            "TAN,\n" +
            "TIME FUNCTION,\n" +
            "TIMEDIFF,\n" +
            "TIMESTAMP FUNCTION,\n" +
            "TIMESTAMPADD,\n" +
            "TIMESTAMPDIFF,\n" +
            "TIME_FORMAT,\n" +
            "TIME_TO_SEC,\n" +
            "TO_BASE64,\n" +
            "TO_DAYS,\n" +
            "TO_SECONDS,\n" +
            "TRIM,\n" +
            "TRUNCATE,\n" +
            "UCASE,\n" +
            "UNCOMPRESS,\n" +
            "UNCOMPRESSED_LENGTH,\n" +
            "UNHEX,\n" +
            "UNIX_TIMESTAMP,\n" +
            "UPDATEXML,\n" +
            "UPPER,\n" +
            "USER,\n" +
            "UTC_DATE,\n" +
            "UTC_TIME,\n" +
            "UTC_TIMESTAMP,\n" +
            "UUID,\n" +
            "UUID_SHORT,\n" +
            "VALIDATE_PASSWORD_STRENGTH,\n" +
            "VALUES,\n" +
            "VERSION,\n" +
            "WEEK,\n" +
            "WEEKDAY,\n" +
            "WEEKOFYEAR,\n" +
            "WEIGHT_STRING,\n" +
            "XOR,\n" +
            "YEAR,\n" +
            "YEARWEEK,\n" +
            "^,\n" +
            "|,\n" +
            "~";

        /**
         * MySQL Functions
         */
        String[] functionArray = mysqlFunction.split(",");
        List<String> mysqlFun = new ArrayList<>();
        for (int i = 0; i < functionArray.length; i++) {
            mysqlFun.add(functionArray[i].trim());
        }
        System.out.println("Number of MySQL functions:" + mysqlFun.size());

        /**
         * CoronaDB Functions
         */
        List<String> tddlFunction = new ArrayList<>();
        TddlOperatorTable operatorTable = TddlOperatorTable.instance();
        for (SqlOperator op : operatorTable.getOperatorList()) {
            tddlFunction.add(op.getName());
        }
        System.out.println("Number of CoronaDB Function:" + tddlFunction.size());

        List<String> unsupportedFun = new ArrayList<>();
        for (int i = 0; i < mysqlFun.size(); i++) {
            String myName = mysqlFun.get(i);
            boolean found = false;
            for (int j = 0; j < tddlFunction.size(); j++) {
                if (myName.equalsIgnoreCase(tddlFunction.get(j))) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                unsupportedFun.add(myName);
            }
        }

        System.out.println("Number:" + unsupportedFun.size());
        for (String name : unsupportedFun) {
            System.out.println(name);
        }
    }
}

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

package com.alibaba.polardbx.qatest.ddl.auto.partition;

import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.validator.DataValidator;
import com.alibaba.polardbx.server.util.StringUtil;
import com.googlecode.protobuf.format.util.HexUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author chenghui.lch
 */
@RunWith(value = Parameterized.class)
public class PartitionBkaJoinColumnTypeTest extends PartitionTestBase {

    public PartitionBkaJoinColumnTypeTest.TestParameter parameter;

    public PartitionBkaJoinColumnTypeTest(PartitionBkaJoinColumnTypeTest.TestParameter parameter) {
        this.parameter = parameter;
    }

    protected String createTbColTemplate = "CREATE TABLE IF NOT EXISTS `%s` (\n"
        + "\t`pk` bigint(11) NOT NULL,\n"
        + "\t`integer_test` int(11) DEFAULT NULL,\n"
        + "\t`varchar_test` varchar(255) DEFAULT NULL,\n"
        + "\t`char_test` char(255) DEFAULT NULL,\n"
        + "\t`blob_test` blob,\n"
        + "\t`tinyint_test` tinyint(4) DEFAULT NULL,\n"
        + "\t`tinyint_1bit_test` tinyint(1) DEFAULT NULL,\n"
        + "\t`smallint_test` smallint(6) DEFAULT NULL,\n"
        + "\t`mediumint_test` mediumint(9) DEFAULT NULL,\n"
        + "\t`bit_test` bit(1) DEFAULT NULL,\n"
        + "\t`bigint_test` bigint(20) DEFAULT NULL,\n"
        + "\t`float_test` float DEFAULT NULL,\n"
        + "\t`double_test` double DEFAULT NULL,\n"
        + "\t`decimal_test` decimal(10, 0) DEFAULT NULL,\n"
        + "\t`date_test` date DEFAULT NULL,\n"
        + "\t`time_test` time DEFAULT NULL,\n"
        + "\t`datetime_test` datetime DEFAULT NULL,\n"
        + "\t`timestamp_test` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
        + "\t`year_test` year(4) DEFAULT NULL,\n"
        + "\t`mediumtext_test` mediumtext,\n"
        + "\tPRIMARY KEY (`pk`)\n"
        + ") ENGINE = InnoDB DEFAULT CHARSET = utf8 ";

    protected String createTbPartTemplate8 = "PARTITION BY KEY(%s) PARTITIONS 8;";
    protected String createTbPartTemplate4 = "PARTITION BY KEY(%s) PARTITIONS 4;";

    protected String insertSqlValueTemplate = "VALUES (0,17,'he343243','he343243',_binary '50',13,19,6,10,_binary '\\0',9,4.56,21.258,100,'2013-04-05','11:23:45','2014-02-12 11:23:45','2014-02-12 03:23:45',2013,'he343243'),(1,18,'word23','word23',_binary '51',14,20,7,11,_binary '\u0001',10,4.32,35.1478,1000,'2015-11-23','06:34:12','2013-04-05 06:34:12','2013-04-04 22:34:12',2014,'word23'),(2,19,'feed32feed','feed32feed',_binary '52',15,21,8,12,_binary '\\0',11,4.23,38.4879,10000,'2010-02-22','08:02:45','2015-11-23 08:02:45','2015-11-23 00:02:45',2015,'feed32feed'),(3,20,'nihaore','nihaore',_binary '53',16,22,9,13,_binary '\u0001',12,5.34,40.47845,100000,'2015-12-02','18:35:23','2010-02-22 18:35:23','2010-02-22 10:35:23',2016,'nihaore'),(4,21,'afdaewer','afdaewer',_binary '54',17,23,10,14,_binary '\\0',13,6.78,48.1478,1000000,'2014-05-26','15:23:34','2014-05-26 20:12:12','2014-05-26 12:12:12',2017,'afdaewer'),(5,22,'hellorew','hellorew',_binary '55',18,24,11,15,_binary '\u0001',14,4.51,50.48745,10000000,'2011-12-23','20:12:12','2011-12-23 12:12:12','2011-12-23 04:12:12',2018,'hellorew'),(6,23,'abdfeed','abdfeed',_binary '56',19,25,12,16,_binary '\\0',15,7.34,55.1478,100000000,'2003-04-05','12:12:12','2003-04-05 12:23:34','2003-04-05 04:23:34',2019,'abdfeed'),(7,24,'cdefeed','cdefeed',_binary '57',20,26,13,17,_binary '\u0001',16,5.78,58.1245,100000000,'2013-02-05','12:23:34','2013-02-05 12:27:32','2013-02-05 04:27:32',2011,'cdefeed'),(8,25,'adaabcwer','adaabcwer',_binary '48',21,27,14,18,_binary '\\0',17,0.45,80.4578,1000000000,'2013-09-02','12:27:32','2013-09-02 14:47:28','2013-09-02 06:47:28',2009,'adaabcwer'),(9,26,NULL,'afsabcabcd',_binary '49',22,28,15,19,_binary '\u0001',18,7.89,90.4587447,10,'2017-03-22','14:47:28','2035-03-22 07:47:28','2035-03-21 23:47:28',2012,'afsabcabcd'),(10,27,'sfdeiekd','sfdeiekd',_binary '50',23,29,16,20,_binary '\\0',19,2.34,180.457845,100,'2011-06-22','07:47:28','2011-06-22 09:12:28','2011-06-22 01:12:28',2013,'sfdeiekd'),(11,28,'einoejk','einoejk',_binary '51',24,30,17,21,_binary '\u0001',20,4.12,200.48787,1000,'2013-03-22','09:12:28','2013-03-22 09:17:28','2013-03-22 01:17:28',2014,'einoejk'),(12,29,'kisfe','kisfe',_binary '52',25,31,18,22,_binary '\\0',21,45.23,250.4874,10000,'2014-02-12','09:17:28','2015-12-02 15:23:34','2015-12-02 07:23:34',2015,'kisfe'),(13,30,'safdwe','safdwe',_binary '53',26,32,19,23,_binary '\u0001',22,4.55,301.457,100000,'2012-12-13','12:23:00','2012-12-13 12:23:00','2012-12-13 04:23:00',2016,'safdwe'),(14,31,'zhuoXUE','',_binary '54',27,33,20,24,_binary '\\0',23,4.56,800.147,1000000,'2013-04-05','11:23:45','2014-02-12 11:23:45','2014-02-12 03:23:45',2017,'zhuoXUE'),(15,32,'zhuoxue_yll','hello1234',_binary '55',28,34,21,25,_binary '\u0001',24,4.32,1110.4747,10000000,'2015-11-23','06:34:12','2013-04-05 06:34:12','2013-04-04 22:34:12',2018,'hello1234'),(16,33,'zhuoxue%yll','he343243',_binary '56',29,35,22,26,_binary '\\0',25,4.23,1414.14747,100000000,'2010-02-22','08:02:45','2015-11-23 08:02:45','2015-11-23 00:02:45',2019,'he343243'),(17,34,'','word23',_binary '57',30,36,23,27,_binary '\u0001',26,5.34,1825.47484,100000000,'2015-12-02','18:35:23','2010-02-22 18:35:23','2010-02-22 10:35:23',2011,'word23'),(18,35,'hello1234','feed32feed',_binary '48',31,37,24,28,_binary '\\0',27,6.78,2000.23232,1000000000,'2014-05-26','15:23:34','2014-05-26 20:12:12','2014-05-26 12:12:12',2009,'feed32feed'),(19,36,NULL,'nihaore',_binary '49',32,38,25,29,_binary '\u0001',28,4.51,10.2145,10,'2011-12-23','20:12:12','2011-12-23 12:12:12','2011-12-23 04:12:12',2012,'nihaore'),(20,37,'word23','afdaewer',_binary '50',33,39,26,30,_binary '\\0',29,7.34,21.258,100,'2003-04-05','12:12:12','2003-04-05 12:23:34','2003-04-05 04:23:34',2013,'afdaewer'),(21,38,'feed32feed','hellorew',_binary '51',34,40,27,31,_binary '\u0001',30,5.78,35.1478,1000,'2013-02-05','12:23:34','2013-02-05 12:27:32','2013-02-05 04:27:32',2014,'hellorew'),(22,39,'nihaore','abdfeed',_binary '52',35,41,28,32,_binary '\\0',31,0.45,38.4879,10000,'2013-09-02','12:27:32','2013-09-02 14:47:28','2013-09-02 06:47:28',2015,'abdfeed'),(23,40,'afdaewer','cdefeed',_binary '53',36,42,29,33,_binary '\u0001',32,7.89,40.47845,100000,'2017-03-22','14:47:28','2035-03-22 07:47:28','2035-03-21 23:47:28',2016,'cdefeed'),(24,41,'hellorew','adaabcwer',_binary '54',37,43,30,34,_binary '\\0',33,2.34,48.1478,1000000,'2011-06-22','07:47:28','2011-06-22 09:12:28','2011-06-22 01:12:28',2017,'adaabcwer'),(25,42,'abdfeed','afsabcabcd',_binary '55',38,44,31,35,_binary '\u0001',34,4.12,50.48745,10000000,'2013-03-22','09:12:28','2013-03-22 09:17:28','2013-03-22 01:17:28',2018,'afsabcabcd'),(26,43,'cdefeed','sfdeiekd',_binary '56',39,45,32,36,_binary '\\0',35,45.23,55.1478,100000000,'2014-02-12','09:17:28','2015-12-02 15:23:34','2015-12-02 07:23:34',2019,'sfdeiekd'),(27,44,'adaabcwer','einoejk',_binary '57',40,46,33,37,_binary '\u0001',36,4.55,58.1245,100000000,'2012-12-13','12:23:00','2012-12-13 12:23:00','2012-12-13 04:23:00',2011,'einoejk'),(28,45,'afsabcabcd','kisfe',_binary '48',41,47,34,38,_binary '\\0',37,4.56,80.4578,1000000000,'2013-04-05','11:23:45','2014-02-12 11:23:45','2014-02-12 03:23:45',2009,'kisfe'),(29,46,NULL,'safdwe',_binary '49',42,48,35,39,_binary '\u0001',38,4.32,90.4587447,10,'2015-11-23','06:34:12','2013-04-05 06:34:12','2013-04-04 22:34:12',2012,'safdwe'),(30,47,'einoejk','',_binary '50',43,49,36,40,_binary '\\0',39,4.23,180.457845,100,'2010-02-22','08:02:45','2015-11-23 08:02:45','2015-11-23 00:02:45',2013,'zhuoXUE'),(31,48,'kisfe','hello1234',_binary '51',44,50,37,41,_binary '\u0001',40,5.34,200.48787,1000,'2015-12-02','18:35:23','2010-02-22 18:35:23','2010-02-22 10:35:23',2014,'hello1234'),(32,49,'safdwe','he343243',_binary '52',45,51,38,42,_binary '\\0',41,6.78,250.4874,10000,'2014-05-26','15:23:34','2014-05-26 20:12:12','2014-05-26 12:12:12',2015,'he343243'),(33,50,'zhuoXUE','word23',_binary '53',46,52,39,43,_binary '\u0001',42,4.51,301.457,100000,'2011-12-23','20:12:12','2011-12-23 12:12:12','2011-12-23 04:12:12',2016,'word23'),(34,51,'zhuoxue_yll','feed32feed',_binary '54',47,53,40,44,_binary '\\0',43,7.34,800.147,1000000,'2003-04-05','12:12:12','2003-04-05 12:23:34','2003-04-05 04:23:34',2017,'feed32feed'),(35,52,'zhuoxue%yll','nihaore',_binary '55',48,54,41,45,_binary '\u0001',44,5.78,1110.4747,10000000,'2013-02-05','12:23:34','2013-02-05 12:27:32','2013-02-05 04:27:32',2018,'nihaore'),(36,53,'','afdaewer',_binary '56',49,55,42,46,_binary '\\0',45,0.45,1414.14747,100000000,'2013-09-02','12:27:32','2013-09-02 14:47:28','2013-09-02 06:47:28',2019,'afdaewer'),(37,54,'hello1234','hellorew',_binary '57',50,56,43,47,_binary '\u0001',46,7.89,1825.47484,100000000,'2017-03-22','14:47:28','2035-03-22 07:47:28','2035-03-21 23:47:28',2011,'hellorew'),(38,55,'he343243','abdfeed',_binary '48',51,57,44,48,_binary '\\0',47,2.34,2000.23232,1000000000,'2011-06-22','07:47:28','2011-06-22 09:12:28','2011-06-22 01:12:28',2009,'abdfeed'),(39,56,NULL,'cdefeed',_binary '49',52,58,45,49,_binary '\u0001',48,4.12,10.2145,10,'2013-03-22','09:12:28','2013-03-22 09:17:28','2013-03-22 01:17:28',2012,'cdefeed'),(40,57,'feed32feed','adaabcwer',_binary '50',53,59,46,50,_binary '\\0',49,45.23,21.258,100,'2014-02-12','09:17:28','2015-12-02 15:23:34','2015-12-02 07:23:34',2013,'adaabcwer'),(41,58,'nihaore','afsabcabcd',_binary '51',54,60,47,51,_binary '\u0001',50,4.55,35.1478,1000,'2012-12-13','12:23:00','2012-12-13 12:23:00','2012-12-13 04:23:00',2014,'afsabcabcd'),(42,59,'afdaewer','sfdeiekd',_binary '52',55,61,48,52,_binary '\\0',51,4.56,38.4879,10000,'2013-04-05','11:23:45','2014-02-12 11:23:45','2014-02-12 03:23:45',2015,'sfdeiekd'),(43,60,'hellorew','einoejk',_binary '53',56,62,49,53,_binary '\u0001',52,4.32,40.47845,100000,'2015-11-23','06:34:12','2013-04-05 06:34:12','2013-04-04 22:34:12',2016,'einoejk'),(44,61,'abdfeed','kisfe',_binary '54',57,63,50,54,_binary '\\0',53,4.23,48.1478,1000000,'2010-02-22','08:02:45','2015-11-23 08:02:45','2015-11-23 00:02:45',2017,'kisfe'),(45,62,'cdefeed','safdwe',_binary '55',58,64,51,55,_binary '\u0001',54,5.34,50.48745,10000000,'2015-12-02','18:35:23','2010-02-22 18:35:23','2010-02-22 10:35:23',2018,'safdwe'),(46,63,'adaabcwer','',_binary '56',59,65,52,56,_binary '\\0',55,6.78,55.1478,100000000,'2014-05-26','15:23:34','2014-05-26 20:12:12','2014-05-26 12:12:12',2019,'zhuoXUE'),(47,0,'afsabcabcd','hello1234',_binary '57',60,66,53,57,_binary '\u0001',56,4.51,58.1245,100000000,'2011-12-23','20:12:12','2011-12-23 12:12:12','2011-12-23 04:12:12',2011,'hello1234'),(48,1,'sfdeiekd','he343243',_binary '48',61,67,54,58,_binary '\\0',57,7.34,80.4578,1000000000,'2003-04-05','12:12:12','2003-04-05 12:23:34','2003-04-05 04:23:34',2009,'he343243'),(49,2,NULL,'word23',_binary '49',62,68,55,59,_binary '\u0001',58,5.78,90.4587447,10,'2013-02-05','12:23:34','2013-02-05 12:27:32','2013-02-05 04:27:32',2012,'word23'),(50,3,'kisfe','feed32feed',_binary '50',63,69,56,60,_binary '\\0',59,0.45,180.457845,100,'2013-09-02','12:27:32','2013-09-02 14:47:28','2013-09-02 06:47:28',2013,'feed32feed'),(51,4,'safdwe','nihaore',_binary '51',64,70,57,61,_binary '\u0001',60,7.89,200.48787,1000,'2017-03-22','14:47:28','2035-03-22 07:47:28','2035-03-21 23:47:28',2014,'nihaore'),(52,5,'zhuoXUE','afdaewer',_binary '52',65,71,58,62,_binary '\\0',61,2.34,250.4874,10000,'2011-06-22','07:47:28','2011-06-22 09:12:28','2011-06-22 01:12:28',2015,'afdaewer'),(53,6,'zhuoxue_yll','hellorew',_binary '53',66,72,59,63,_binary '\u0001',62,4.12,301.457,100000,'2013-03-22','09:12:28','2013-03-22 09:17:28','2013-03-22 01:17:28',2016,'hellorew'),(54,7,'zhuoxue%yll','abdfeed',_binary '54',67,73,60,64,_binary '\\0',63,45.23,800.147,1000000,'2014-02-12','09:17:28','2015-12-02 15:23:34','2015-12-02 07:23:34',2017,'abdfeed'),(55,8,'','cdefeed',_binary '55',68,74,61,65,_binary '\u0001',64,4.55,1110.4747,10000000,'2012-12-13','12:23:00','2012-12-13 12:23:00','2012-12-13 04:23:00',2018,'cdefeed'),(56,9,'hello1234','adaabcwer',_binary '56',69,75,62,66,_binary '\\0',65,4.56,1414.14747,100000000,'2013-04-05','11:23:45','2014-02-12 11:23:45','2014-02-12 03:23:45',2019,'adaabcwer'),(57,10,'he343243','afsabcabcd',_binary '57',70,76,63,67,_binary '\u0001',66,4.32,1825.47484,100000000,'2015-11-23','06:34:12','2013-04-05 06:34:12','2013-04-04 22:34:12',2011,'afsabcabcd');\n";
    protected String joinSqlTemplatePolarx = "/*+TDDL:BKA_JOIN(%s,%s)*/ select t1.pk from %s t1 inner join %s t2 on %s order by t1.pk limit 100";

    private static Map<String,String> typeNameMap = new HashMap<>();
    static {
        typeNameMap.put("integer_test","int");
        typeNameMap.put("varchar_test","var");
        typeNameMap.put("char_test","char");
        typeNameMap.put("blob_test","blob");
        typeNameMap.put("tinyint_test","tint");
        typeNameMap.put("tinyint_1bit_test","tint1");
        typeNameMap.put("smallint_test","sint");
        typeNameMap.put("mediumint_test","mint");
        typeNameMap.put("bit_test","bit");
        typeNameMap.put("bigint_test","bint");
        typeNameMap.put("float_test","f");
        typeNameMap.put("double_test","d");
        typeNameMap.put("decimal_test","deci");
        typeNameMap.put("date_test","date");
        typeNameMap.put("time_test","time");
        typeNameMap.put("datetime_test","dt");
        typeNameMap.put("timestamp_test","ts");
        typeNameMap.put("year_test","y");
        typeNameMap.put("mediumtext_test","mtxt");
    }

    @Parameterized.Parameters(name = "{index}: partColTypeTestCase {0}")
    public static List<PartitionBkaJoinColumnTypeTest.TestParameter> parameters() {
        return Arrays.asList(

            /**
             * ========= Key ===========
             */
            /* integer */
            new PartitionBkaJoinColumnTypeTest.TestParameter(
                new String[] {"integer_test"}/*col*/,
                new String[] {"integer_test"}/*join cols*/,
                new String[] {"", "", ""}/*prepStmts*/
            ),

            /* varchar */
            new PartitionBkaJoinColumnTypeTest.TestParameter(
                new String[] {"varchar_test"}/*col*/,
                new String[] {"varchar_test"}/*join cols*/,
                new String[] {"", "", ""}/*prepStmts*/
            ),

            /* char_test */
            new PartitionBkaJoinColumnTypeTest.TestParameter(
                new String[] {"char_test"}/*col*/,
                new String[] {"char_test"}/*join cols*/,
                new String[] {"", "", ""}/*prepStmts*/
            ),

            /* tinyint_test */
            new PartitionBkaJoinColumnTypeTest.TestParameter(
                new String[] {"tinyint_test"}/*col*/,
                new String[] {"tinyint_test"}/*join cols*/,
                new String[] {"", "", ""}/*prepStmts*/
            ),

            /* smallint_test */
            new PartitionBkaJoinColumnTypeTest.TestParameter(
                new String[] {"smallint_test"}/*col*/,
                new String[] {"smallint_test"}/*join cols*/,
                new String[] {"", "", ""}/*prepStmts*/
            ),

            /* mediumint_test */
            new PartitionBkaJoinColumnTypeTest.TestParameter(
                new String[] {"mediumint_test"}/*col*/,
                new String[] {"mediumint_test"}/*join cols*/,
                new String[] {"", "", ""}/*prepStmts*/
            ),

            /* bigint_test */
            new PartitionBkaJoinColumnTypeTest.TestParameter(
                new String[] {"bigint_test"}/*col*/,
                new String[] {"bigint_test"}/*join cols*/,
                new String[] {"", "", ""}/*prepStmts*/
            ),

            /* date_test */
            new PartitionBkaJoinColumnTypeTest.TestParameter(
                new String[] {"date_test"}/*col*/,
                new String[] {"date_test"}/*join cols*/,
                new String[] {"", "", ""}/*prepStmts*/
            ),

            /* date_test */
            new PartitionBkaJoinColumnTypeTest.TestParameter(
                new String[] {"date_test"}/*col*/,
                new String[] {"date_test"}/*join cols*/,
                new String[] {"", "", ""}/*prepStmts*/
            ),

            /* datetime_test */
            new PartitionBkaJoinColumnTypeTest.TestParameter(
                new String[] {"datetime_test"}/*col*/,
                new String[] {"datetime_test"}/*join cols*/,
                new String[] {"", "", ""}/*prepStmts*/
            ),

            /* timestamp_test */
            new PartitionBkaJoinColumnTypeTest.TestParameter(
                new String[] {"timestamp_test"}/*col*/,
                new String[] {"timestamp_test"}/*join col*/,
                new String[] {"", "", ""}/*prepStmts*/
            ),

            /* integer, varchar */
            new PartitionBkaJoinColumnTypeTest.TestParameter(
                new String[] {"integer_test", "varchar_test"}/*col*/,
                new String[] {"integer_test", "varchar_test"}/*join col*/,
                new String[] {"", "", ""}/*prepStmts*/
            ),

            /* integer, varchar */
            new PartitionBkaJoinColumnTypeTest.TestParameter(
                new String[] {"integer_test", "varchar_test"}/*col*/,
                new String[] {"integer_test", "date_test"}/*join col*/,
                new String[] {"", "", ""}/*prepStmts*/
            ),

            /* integer, varchar */
            new PartitionBkaJoinColumnTypeTest.TestParameter(
                new String[] {"integer_test", "varchar_test"}/*col*/,
                new String[] {"varchar_test", "date_test"}/*join col*/,
                new String[] {"", "", ""}/*prepStmts*/
            ),

            /* integer, varchar */
            new PartitionBkaJoinColumnTypeTest.TestParameter(
                new String[] {"integer_test", "varchar_test"}/*col*/,
                new String[] {"varchar_test"}/*join col*/,
                new String[] {"", "", ""}/*prepStmts*/
            ),

            /* integer, varchar */
            new PartitionBkaJoinColumnTypeTest.TestParameter(
                new String[] {"integer_test", "varchar_test"}/*col*/,
                new String[] {"integer_test"}/*join col*/,
                new String[] {"", "", ""}/*prepStmts*/
            ),

            /* varchar, datetime_test */
            new PartitionBkaJoinColumnTypeTest.TestParameter(
                new String[] {"varchar_test", "datetime_test"}/*col*/,
                new String[] {"varchar_test", "datetime_test"}/*join col*/,
                new String[] {"", "", ""}/*prepStmts*/
            ),

            /* varchar_test, timestamp_test */
            new PartitionBkaJoinColumnTypeTest.TestParameter(
                new String[] {"varchar_test", "timestamp_test"}/*col*/,
                new String[] {"varchar_test", "timestamp_test"}/*join col*/,
                new String[] {"", "", ""}/*prepStmts*/
            ),

            /* "timestamp_test", "datetime_test", "integer_test" */
            new PartitionBkaJoinColumnTypeTest.TestParameter(
                new String[] {"timestamp_test", "datetime_test", "integer_test"}/*col*/,
                new String[] {"timestamp_test", "datetime_test", "integer_test"}/*col*/,
                new String[] {"", "", ""}/*prepStmts*/
            ),

            /* "timestamp_test", "datetime_test", "integer_test" */
            new PartitionBkaJoinColumnTypeTest.TestParameter(
                new String[] {"timestamp_test", "datetime_test", "integer_test"}/*col*/,
                new String[] {"timestamp_test", "datetime_test"}/*col*/,
                new String[] {"", "", ""}/*prepStmts*/
            ),

            /* "timestamp_test", "datetime_test", "integer_test" */
            new PartitionBkaJoinColumnTypeTest.TestParameter(
                new String[] {"timestamp_test", "datetime_test", "integer_test"}/*col*/,
                new String[] {"timestamp_test", }/*col*/,
                new String[] {"", "", ""}/*prepStmts*/
            ),

            /* "timestamp_test", "datetime_test", "integer_test" */
            new PartitionBkaJoinColumnTypeTest.TestParameter(
                new String[] {"timestamp_test", "datetime_test", "integer_test"}/*col*/,
                new String[] {"timestamp_test", "date_test", "datetime_test"}/*col*/,
                new String[] {"", "", ""}/*prepStmts*/
            ),

            /* "timestamp_test", "datetime_test", "integer_test" */
            new PartitionBkaJoinColumnTypeTest.TestParameter(
                new String[] {"timestamp_test", "datetime_test", "integer_test"}/*col*/,
                new String[] {"datetime_test", "integer_test"}/*col*/,
                new String[] {"", "", ""}/*prepStmts*/
            ),

            /* "timestamp_test", "datetime_test", "integer_test" */
            new PartitionBkaJoinColumnTypeTest.TestParameter(
                new String[] {"timestamp_test", "datetime_test", "integer_test"}/*col*/,
                new String[] {"timestamp_test", "date_test"}/*col*/,
                new String[] {"", "", ""}/*prepStmts*/
            ),

            /* integer, datetime */
            new PartitionBkaJoinColumnTypeTest.TestParameter(
                new String[] {"integer_test", "datetime_test"}/*col*/,
                new String[] {"integer_test", "datetime_test"}/*join col*/,
                new String[] {"", "", ""}/*prepStmts*/
            )
        );
    }

    @Test
    public void testSelectBkaJoin() throws SQLException {
        prepareDdl(this.parameter);
        prepareDatas(this.parameter);
        testBkaJoin(this.parameter);
    }

    protected String getTblName(String prefix, String[] partCols) {
        String[] typeNames = new String[partCols.length];
        for (int i = 0; i < partCols.length; i++) {
            typeNames[i] = typeNameMap.get(partCols[i]);
        }
        String tblName = prefix + String.join("_", typeNames);
        return tblName;
    }

    protected void prepareDdl(TestParameter params) {
        // create tbl
        String partColStr = String.join(",", params.partCols);
        String createTbPart8 = String.format(createTbPartTemplate8, partColStr);
        String createTbPart4 = String.format(createTbPartTemplate4, partColStr);

        String tblName1 = getTblName("t1_", params.partCols);
        String tblName2 = getTblName("t2_", params.partCols);

        String createTbSql1Polarx = String.format(createTbColTemplate, tblName1) + createTbPart8;
        String createTbSql2Polarx = String.format(createTbColTemplate, tblName2) + createTbPart4;
        String createTbSql1MySql = String.format(createTbColTemplate, tblName1);
        String createTbSql2MySql = String.format(createTbColTemplate, tblName2);

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTbSql1Polarx);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTbSql2Polarx);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTbSql1MySql);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTbSql2MySql);
    }

    protected void prepareDatas(TestParameter params) {
        String tblName1 = getTblName("t1_", params.partCols);
        String tblName2 = getTblName("t2_", params.partCols);

        String insertSql1 = String.format("INSERT IGNORE INTO %s ", tblName1) + insertSqlValueTemplate;
        String insertSql2 = String.format("INSERT IGNORE INTO %s ", tblName2) + insertSqlValueTemplate;

        JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql2);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, insertSql1);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, insertSql2);
    }

    protected void testBkaJoin(TestParameter params) {
        String tblName1 = getTblName("t1_", params.partCols);
        String tblName2 = getTblName("t2_", params.partCols);

        String[] condExprs = new String[params.joinCols.length];
        for (int i = 0; i < params.joinCols.length; i++) {
            condExprs[i] = String.format("t1.%s=t2.%s", params.joinCols[i], params.joinCols[i]);
        }
        String joinCondStr = String.join(" and ", condExprs);
        String joinSql = String.format(joinSqlTemplatePolarx, tblName1, tblName2,tblName1, tblName2, joinCondStr);
        DataValidator validator = new DataValidator();
        logSql(String.join(",", params.partCols), joinSql);
        validator.selectContentSameAssert(joinSql, new ArrayList<>(), mysqlConnection, tddlConnection);
    }

    protected static class TestParameter {
        public String[] partCols;
        public String[] joinCols;
        public String[] prepareStmts;


        public TestParameter(String[] partCols,
                             String[] joinCols,
                             String[] prepareStmts) {
            this.partCols = partCols;
            this.joinCols = joinCols;
            this.prepareStmts = prepareStmts;

        }

        @Override
        public String toString() {
            String str = String.format("[%s]",
                String.join(",", this.partCols));
            return str;
        }
    }

    protected static void logSql(String caseStr, String sql) {
        log.info(String.format("case=[%s], sql=[\n%s\n]\n", caseStr, sql));
    }


}

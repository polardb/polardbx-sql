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

package com.alibaba.polardbx.qatest.ddl.sharding.gsi.group1;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.util.Litmus;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.qatest.validator.DataValidator.resultSetContentSameAssert;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.number.OrderingComparison.greaterThan;

/**
 * @author chenmo.cm
 */
public class CreateGsiTest extends DDLBaseNewDBTestCase {

    private static final String PRIMARY_TABLE_NAME = "full_sync_source";
    private static final String INDEX_TABLE_NAME = "g_i_c_full_sync_source";

    private static final String FULL_TYPE_TABLE = MessageFormat.format(
        ExecuteTableSelect.FULL_TYPE_TABLE_TEMPLATE,
        PRIMARY_TABLE_NAME,
        "`id`",
        " dbpartition by hash(id) tbpartition by hash(id) tbpartitions 3");

    private static final ImmutableList<String> inserts = ImmutableList.<String>builder()
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_bit_1) values(null,0)                                                                                ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_bit_1) values(null,1)                                                                                ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_bit_1) values(null,2)                                                                                ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_bit_8) values(null,0)                                                                                ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_bit_8) values(null,1)                                                                                ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_bit_8) values(null,2)                                                                                ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_bit_8) values(null,256)                                                                              ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_bit_16) values(null,0)                                                                               ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_bit_16) values(null,1)                                                                               ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_bit_16) values(null,2)                                                                               ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_bit_16) values(null,65535)                                                                           ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_bit_32) values(null,0)                                                                               ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_bit_32) values(null,1)                                                                               ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_bit_32) values(null,2)                                                                               ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_bit_32) values(null,4294967296)                                                                      ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_bit_64) values(null,0)                                                                               ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_bit_64) values(null,1)                                                                               ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_bit_64) values(null,2)                                                                               ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_bit_64) values(null,18446744073709551615)                                                            ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_tinyint_1) values(null,-1)                                                                           ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_tinyint_1) values(null,0)                                                                            ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_tinyint_1) values(null,1)                                                                            ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_tinyint_1) values(null,127)                                                                          ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_tinyint_1_un) values(null,-1)                                                                        ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_tinyint_1_un) values(null,0)                                                                         ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_tinyint_1_un) values(null,1)                                                                         ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_tinyint_1_un) values(null,127)                                                                       ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_tinyint_1_un) values(null,255)                                                                       ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_tinyint_4) values(null,-1)                                                                           ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_tinyint_4) values(null,0)                                                                            ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_tinyint_4) values(null,1)                                                                            ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_tinyint_4) values(null,127)                                                                          ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_tinyint_4_un) values(null,-1)                                                                        ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_tinyint_4_un) values(null,0)                                                                         ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_tinyint_4_un) values(null,1)                                                                         ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_tinyint_4_un) values(null,127)                                                                       ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_tinyint_4_un) values(null,255)                                                                       ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_tinyint_8) values(null,-1)                                                                           ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_tinyint_8) values(null,0)                                                                            ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_tinyint_8) values(null,1)                                                                            ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_tinyint_8) values(null,127)                                                                          ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_tinyint_8_un) values(null,-1)                                                                        ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_tinyint_8_un) values(null,0)                                                                         ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_tinyint_8_un) values(null,1)                                                                         ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_tinyint_8_un) values(null,127)                                                                       ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_tinyint_8_un) values(null,255)                                                                       ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_smallint_1) values(null,-1)                                                                          ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_smallint_1) values(null,0)                                                                           ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_smallint_1) values(null,1)                                                                           ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_smallint_1) values(null,65535)                                                                       ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_smallint_16) values(null,-1)                                                                         ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_smallint_16) values(null,0)                                                                          ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_smallint_16) values(null,1)                                                                          ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_smallint_16) values(null,65535)                                                                      ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_smallint_16_un) values(null,-1)                                                                      ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_smallint_16_un) values(null,0)                                                                       ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_smallint_16_un) values(null,1)                                                                       ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_smallint_16_un) values(null,65535)                                                                   ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_mediumint_1) values(null,-1)                                                                         ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_mediumint_1) values(null,0)                                                                          ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_mediumint_1) values(null,1)                                                                          ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_mediumint_1) values(null,16777215)                                                                   ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_mediumint_24) values(null,-1)                                                                        ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_mediumint_24) values(null,0)                                                                         ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_mediumint_24) values(null,1)                                                                         ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_mediumint_24) values(null,16777215)                                                                  ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_mediumint_24_un) values(null,-1)                                                                     ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_mediumint_24_un) values(null,0)                                                                      ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_mediumint_24_un) values(null,1)                                                                      ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_mediumint_24_un) values(null,16777215)                                                               ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_int_1) values(null,-1)                                                                               ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_int_1) values(null,0)                                                                                ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_int_1) values(null,1)                                                                                ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_int_1) values(null,4294967295)                                                                       ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_int_32) values(null,-1)                                                                              ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_int_32) values(null,0)                                                                               ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_int_32) values(null,1)                                                                               ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_int_32) values(null,4294967295)                                                                      ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_int_32_un) values(null,-1)                                                                           ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_int_32_un) values(null,0)                                                                            ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_int_32_un) values(null,1)                                                                            ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_int_32_un) values(null,4294967295)                                                                   ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_bigint_1) values(null,-1)                                                                            ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_bigint_1) values(null,0)                                                                             ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_bigint_1) values(null,1)                                                                             ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_bigint_1) values(null,18446744073709551615)                                                          ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_bigint_64) values(null,-1)                                                                           ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_bigint_64) values(null,0)                                                                            ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_bigint_64) values(null,1)                                                                            ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_bigint_64) values(null,18446744073709551615)                                                         ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_bigint_64_un) values(null,-1)                                                                        ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_bigint_64_un) values(null,0)                                                                         ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_bigint_64_un) values(null,1)                                                                         ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_bigint_64_un) values(null,18446744073709551615)                                                      ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_decimal) values(null,'100.000')                                                                      ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_decimal) values(null,'100.003')                                                                      ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_decimal) values(null,'-100.003')                                                                     ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_decimal) values(null,'-100.0000001')                                                                 ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_decimal_pr) values(null,'100.000')                                                                   ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_decimal_pr) values(null,'100.003')                                                                   ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_decimal_pr) values(null,'-100.003')                                                                  ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_decimal_pr) values(null,'-100.0000001')                                                              ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_float) values(null,'100.000')                                                                        ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_float) values(null,'100.003')                                                                        ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_float) values(null,'-100.003')                                                                       ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_float) values(null,'-100.0000001')                                                                   ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_float_pr) values(null,'100.000')                                                                     ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_float_pr) values(null,'100.003')                                                                     ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_float_pr) values(null,'-100.003')                                                                    ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_float_pr) values(null,'-100.0000001')                                                                ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_float_un) values(null,'100.000')                                                                     ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_float_un) values(null,'100.003')                                                                     ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_float_un) values(null,'-100.003')                                                                    ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_float_un) values(null,'-100.0000001')                                                                ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_double) values(null,'100.000')                                                                       ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_double) values(null,'100.003')                                                                       ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_double) values(null,'-100.003')                                                                      ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_double) values(null,'-100.0000001')                                                                  ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_double_pr) values(null,'100.000')                                                                    ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_double_pr) values(null,'100.003')                                                                    ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_double_pr) values(null,'-100.003')                                                                   ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_double_pr) values(null,'-100.0000001')                                                               ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_double_un) values(null,'100.000')                                                                    ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_double_un) values(null,'100.003')                                                                    ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_double_un) values(null,'-100.003')                                                                   ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_double_un) values(null,'-100.0000001')                                                               ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_date) values(null,'0000-00-00')                                                                      ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_date) values(null,'9999-12-31')                                                                      ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_date) values(null,'0000-00-00 01:01:01')                                                             ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_date) values(null,'1969-09-00')                                                                      ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_date) values(null,'2018-00-00')                                                                      ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_date) values(null,'2017-12-12')                                                                      ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_datetime) values(null,'0000-00-00 00:00:00')                                                         ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_datetime) values(null,'9999-12-31 23:59:59')                                                         ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_datetime) values(null,'0000-00-00 01:01:01')                                                         ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_datetime) values(null,'1969-09-00 23:59:59')                                                         ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_datetime) values(null,'2018-00-00 00:00:00')                                                         ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_datetime) values(null,'2017-12-12 23:59:59')                                                         ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_datetime_1) values(null,'0000-00-00 00:00:00.0')                                                     ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_datetime_1) values(null,'9999-12-31 23:59:59.9')                                                     ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_datetime_1) values(null,'0000-00-00 01:01:01.12')                                                    ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_datetime_1) values(null,'1969-09-00 23:59:59.06')                                                    ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_datetime_1) values(null,'2018-00-00 00:00:00.04')                                                    ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_datetime_1) values(null,'2017-12-12 23:59:59.045')                                                   ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_datetime_3) values(null,'0000-00-00 00:00:00.000')                                                   ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_datetime_3) values(null,'9999-12-31 23:59:59.999')                                                   ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_datetime_3) values(null,'0000-00-00 01:01:01.121')                                                   ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_datetime_3) values(null,'1969-09-00 23:59:59.0006')                                                  ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_datetime_3) values(null,'2018-00-00 00:00:00.0004')                                                  ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_datetime_3) values(null,'2017-12-12 23:59:59.00045')                                                 ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_datetime_6) values(null,'0000-00-00 00:00:00.000000')                                                ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_datetime_6) values(null,'9999-12-31 23:59:59.999999')                                                ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_datetime_6) values(null,'0000-00-00 01:01:01.121121')                                                ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_datetime_6) values(null,'1969-09-00 23:59:59.0000006')                                               ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_datetime_6) values(null,'2018-00-00 00:00:00.0000004')                                               ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_datetime_6) values(null,'2017-12-12 23:59:59.00000045')                                              ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_timestamp) values(null,'0000-00-00 00:00:00')                                                        ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_timestamp) values(null,'9999-12-31 23:59:59')                                                        ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_timestamp) values(null,'0000-00-00 01:01:01')                                                        ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_timestamp) values(null,'1969-09-00 23:59:59')                                                        ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_timestamp) values(null,'2018-00-00 00:00:00')                                                        ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_timestamp) values(null,'2017-12-12 23:59:59')                                                        ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_timestamp_1) values(null,'0000-00-00 00:00:00.0')                                                    ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_timestamp_1) values(null,'9999-12-31 23:59:59.9')                                                    ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_timestamp_1) values(null,'0000-00-00 01:01:01.12')                                                   ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_timestamp_1) values(null,'1969-09-00 23:59:59.06')                                                   ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_timestamp_1) values(null,'2018-00-00 00:00:00.04')                                                   ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_timestamp_1) values(null,'2017-12-12 23:59:59.045')                                                  ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_timestamp_3) values(null,'0000-00-00 00:00:00.000')                                                  ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_timestamp_3) values(null,'9999-12-31 23:59:59.999')                                                  ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_timestamp_3) values(null,'0000-00-00 01:01:01.121')                                                  ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_timestamp_3) values(null,'1969-09-00 23:59:59.0006')                                                 ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_timestamp_3) values(null,'2018-00-00 00:00:00.0004')                                                 ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_timestamp_3) values(null,'2017-12-12 23:59:59.00045')                                                ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_timestamp_6) values(null,'0000-00-00 00:00:00.000000')                                               ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_timestamp_6) values(null,'9999-12-31 23:59:59.999999')                                               ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_timestamp_6) values(null,'0000-00-00 01:01:01.121121')                                               ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_timestamp_6) values(null,'1969-09-00 23:59:59.0000006')                                              ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_timestamp_6) values(null,'2018-00-00 00:00:00.0000004')                                              ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_timestamp_6) values(null,'2017-12-12 23:59:59.00000045')                                             ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_time) values(null,'00:00:00')                                                                        ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_time) values(null,'01:01:01')                                                                        ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_time) values(null,'23:59:59')                                                                        ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_time_1) values(null,'00:00:00.1')                                                                    ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_time_1) values(null,'01:01:01.6')                                                                    ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_time_1) values(null,'23:59:59.45')                                                                   ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_time_3) values(null,'00:00:00.111')                                                                  ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_time_3) values(null,'01:01:01.106')                                                                  ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_time_3) values(null,'23:59:59.00045')                                                                ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_time_6) values(null,'00:00:00.111111')                                                               ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_time_6) values(null,'01:01:01.106106')                                                               ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_time_6) values(null,'23:59:59.00000045')                                                             ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_year) values(null,'0000')                                                                            ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_year) values(null,'9999')                                                                            ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_year) values(null,'1970')                                                                            ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_year) values(null,'2000')                                                                            ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_year_4) values(null,'0000')                                                                          ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_year_4) values(null,'9999')                                                                          ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_year_4) values(null,'1970')                                                                          ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_year_4) values(null,'2000')                                                                          ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_char) values(null,'11')                                                                              ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_char) values(null,'99')                                                                              ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_char) values(null,'a中国a')                                                                          ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_varchar) values(null,'11')                                                                           ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_varchar) values(null,'99')                                                                           ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_varchar) values(null,'a中国a')                                                                       ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_binary) values(null,'11')                                                                            ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_binary) values(null,'99')                                                                            ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_binary) values(null,'a中国a')                                                                        ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_varbinary) values(null,'11')                                                                         ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_varbinary) values(null,'99')                                                                         ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_varbinary) values(null,'a中国a')                                                                     ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_blob_tiny) values(null,'11')                                                                         ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_blob_tiny) values(null,'99')                                                                         ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_blob_tiny) values(null,'a中国a')                                                                     ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_blob) values(null,'11')                                                                              ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_blob) values(null,'99')                                                                              ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_blob) values(null,'a中国a')                                                                          ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_blob_medium) values(null,'11')                                                                       ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_blob_medium) values(null,'99')                                                                       ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_blob_medium) values(null,'a中国a')                                                                   ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_blob_long) values(null,'11')                                                                         ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_blob_long) values(null,'99')                                                                         ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_blob_long) values(null,'a中国a')                                                                     ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_text_tiny) values(null,'11')                                                                         ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_text_tiny) values(null,'99')                                                                         ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_text_tiny) values(null,'a中国a')                                                                     ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_text) values(null,'11')                                                                              ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_text) values(null,'99')                                                                              ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_text) values(null,'a中国a')                                                                          ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_text_medium) values(null,'11')                                                                       ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_text_medium) values(null,'99')                                                                       ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_text_medium) values(null,'a中国a')                                                                   ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_text_long) values(null,'11')                                                                         ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_text_long) values(null,'99')                                                                         ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_text_long) values(null,'a中国a')                                                                     ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_enum) values(null,'a')                                                                               ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_enum) values(null,'b')                                                                               ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_enum) values(null,NULL)                                                                              ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_set) values(null,'a')                                                                                ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_set) values(null,'b,a')                                                                              ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_set) values(null,'b,c,a')                                                                            ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_set) values(null,'d')                                                                                ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_set) values(null,NULL)                                                                               ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_json) values(null,'{\"k1\": \"v1\", \"k2\": 10}')                                                    ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_json) values(null,'{\"k1\": \"v1\", \"k2\": [10, 20]}')                                              ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id,c_json) values(null,NULL)                                                                              ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id, c_point) VALUE (null,ST_GEOMFROMTEXT('POINT(15 20)'))                                                ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id, c_linestring) VALUE (null,ST_GEOMFROMTEXT('LINESTRING(0 0, 10 10, 20 25, 50 60)'))                   ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id, c_polygon) VALUE (null,ST_GEOMFROMTEXT('POLYGON((0 0,10 0,10 10,0 10,0 0),(5 5,7 5,7 7,5 7, 5 5))')) ;")
        .add("insert into " + PRIMARY_TABLE_NAME
            + "(id, c_geometory) VALUE (null,ST_GEOMFROMTEXT('POINT(15 20)'))                                            ;")
        .build();

    @Before
    public void before() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE IF EXISTS " + PRIMARY_TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, FULL_TYPE_TABLE);

        List<String> failedList = new ArrayList<>();
        // Prepare data
        for (String insert : inserts) {

            Statement stmt = null;
            try {
                stmt = tddlConnection.createStatement();
                stmt.executeUpdate(insert);
            } catch (SQLSyntaxErrorException msee) {
                throw msee;
            } catch (SQLException e) {
                // ignore exception
                failedList.add(insert);
            } finally {
                JdbcUtil.close(stmt);
            }
        }

        System.out.println("Failed inserts: ");
        failedList.forEach(System.out::println);

        final ResultSet resultSet = JdbcUtil.executeQuery("SELECT COUNT(1) FROM " + PRIMARY_TABLE_NAME,
            tddlConnection);
        assertThat(resultSet.next(), is(true));
        assertThat(resultSet.getLong(1), greaterThan(0L));

    }

    /**
     *
     */
    @Test
    public void testBackfill() {
        String tableName = PRIMARY_TABLE_NAME;
        String indexName = INDEX_TABLE_NAME;

        JdbcUtil.executeUpdateSuccess(
            tddlConnection,
            GSI_ALLOW_ADD_HINT +
                "CREATE GLOBAL INDEX " + indexName + " ON " + tableName
                + "(c_bigint_64) "
                + "covering(`c_bit_1`,`c_bit_8`,`c_bit_16`,`c_bit_32`,`c_bit_64`,`c_tinyint_1`,`c_tinyint_1_un`,`c_tinyint_4`,`c_tinyint_4_un`,`c_tinyint_8`,`c_tinyint_8_un`,`c_smallint_1`,`c_smallint_16`,`c_smallint_16_un`,`c_mediumint_1`,`c_mediumint_24`,`c_mediumint_24_un`,`c_int_1`,`c_int_32`,`c_int_32_un`,`c_bigint_1`,`c_bigint_64_un`,`c_decimal`,`c_decimal_pr`,`c_float`,`c_float_pr`,`c_float_un`,`c_double`,`c_double_pr`,`c_double_un`,`c_date`,`c_datetime`,`c_datetime_1`,`c_datetime_3`,`c_datetime_6`,`c_timestamp_1`,`c_timestamp_3`,`c_timestamp_6`,`c_time`,`c_time_1`,`c_time_3`,`c_time_6`,`c_year`,`c_year_4`,`c_char`,`c_varchar`,`c_binary`,`c_varbinary`,`c_blob_tiny`,`c_blob`,`c_blob_medium`,`c_blob_long`,`c_text_tiny`,`c_text`,`c_text_medium`,`c_text_long`,`c_enum`,`c_set`,`c_json`,`c_geometory`,`c_point`,`c_linestring`,`c_polygon`,`c_multipoint`,`c_multilinestring`,`c_multipolygon`,`c_geometrycollection`) "
                + "dbpartition by hash(`c_bigint_64`) tbpartition by hash(`c_bigint_64`) tbpartitions 7\n");

        final ResultSet primaryRs = JdbcUtil.executeQuery(
            "select `id`,`c_bit_1`,`c_bit_8`,`c_bit_16`,`c_bit_32`,`c_bit_64`,`c_tinyint_1`,`c_tinyint_1_un`,`c_tinyint_4`,`c_tinyint_4_un`,`c_tinyint_8`,`c_tinyint_8_un`,`c_smallint_1`,`c_smallint_16`,`c_smallint_16_un`,`c_mediumint_1`,`c_mediumint_24`,`c_mediumint_24_un`,`c_int_1`,`c_int_32`,`c_int_32_un`,`c_bigint_1`,`c_bigint_64_un`,`c_decimal`,`c_decimal_pr`,`c_float`,`c_float_pr`,`c_float_un`,`c_double`,`c_double_pr`,`c_double_un`,`c_date`,`c_datetime`,`c_datetime_1`,`c_datetime_3`,`c_datetime_6`,`c_timestamp_1`,`c_timestamp_3`,`c_timestamp_6`,`c_time`,`c_time_1`,`c_time_3`,`c_time_6`,`c_year`,`c_year_4`,`c_char`,`c_varchar`,`c_binary`,`c_varbinary`,`c_blob_tiny`,`c_blob`,`c_blob_medium`,`c_blob_long`,`c_text_tiny`,`c_text`,`c_text_medium`,`c_text_long`,`c_enum`,`c_set`,`c_json`,`c_geometory`,`c_point`,`c_linestring`,`c_polygon`,`c_multipoint`,`c_multilinestring`,`c_multipolygon`,`c_geometrycollection` from "
                + tableName + " order by id",
            tddlConnection);
        final ResultSet indexRs = JdbcUtil.executeQuery(
            "select `id`,`c_bit_1`,`c_bit_8`,`c_bit_16`,`c_bit_32`,`c_bit_64`,`c_tinyint_1`,`c_tinyint_1_un`,`c_tinyint_4`,`c_tinyint_4_un`,`c_tinyint_8`,`c_tinyint_8_un`,`c_smallint_1`,`c_smallint_16`,`c_smallint_16_un`,`c_mediumint_1`,`c_mediumint_24`,`c_mediumint_24_un`,`c_int_1`,`c_int_32`,`c_int_32_un`,`c_bigint_1`,`c_bigint_64_un`,`c_decimal`,`c_decimal_pr`,`c_float`,`c_float_pr`,`c_float_un`,`c_double`,`c_double_pr`,`c_double_un`,`c_date`,`c_datetime`,`c_datetime_1`,`c_datetime_3`,`c_datetime_6`,`c_timestamp_1`,`c_timestamp_3`,`c_timestamp_6`,`c_time`,`c_time_1`,`c_time_3`,`c_time_6`,`c_year`,`c_year_4`,`c_char`,`c_varchar`,`c_binary`,`c_varbinary`,`c_blob_tiny`,`c_blob`,`c_blob_medium`,`c_blob_long`,`c_text_tiny`,`c_text`,`c_text_medium`,`c_text_long`,`c_enum`,`c_set`,`c_json`,`c_geometory`,`c_point`,`c_linestring`,`c_polygon`,`c_multipoint`,`c_multilinestring`,`c_multipolygon`,`c_geometrycollection` from "
                + indexName + " order by id",
            tddlConnection);

        resultSetContentSameAssert(primaryRs, indexRs, false);
    }

    /**
     *
     */
    @Test
    public void testBackfill1() {
        String tableName = PRIMARY_TABLE_NAME;
        String indexName = INDEX_TABLE_NAME;

        JdbcUtil.executeUpdateSuccess(
            tddlConnection,
            GSI_ALLOW_ADD_HINT + "ALTER TABLE " + tableName + " ADD GLOBAL INDEX " + indexName + "(c_bigint_64) "
                + "covering(`c_bit_1`,`c_bit_8`,`c_bit_16`,`c_bit_32`,`c_bit_64`,`c_tinyint_1`,`c_tinyint_1_un`,`c_tinyint_4`,`c_tinyint_4_un`,`c_tinyint_8`,`c_tinyint_8_un`,`c_smallint_1`,`c_smallint_16`,`c_smallint_16_un`,`c_mediumint_1`,`c_mediumint_24`,`c_mediumint_24_un`,`c_int_1`,`c_int_32`,`c_int_32_un`,`c_bigint_1`,`c_bigint_64_un`,`c_decimal`,`c_decimal_pr`,`c_float`,`c_float_pr`,`c_float_un`,`c_double`,`c_double_pr`,`c_double_un`,`c_date`,`c_datetime`,`c_datetime_1`,`c_datetime_3`,`c_datetime_6`,`c_timestamp_1`,`c_timestamp_3`,`c_timestamp_6`,`c_time`,`c_time_1`,`c_time_3`,`c_time_6`,`c_year`,`c_year_4`,`c_char`,`c_varchar`,`c_binary`,`c_varbinary`,`c_blob_tiny`,`c_blob`,`c_blob_medium`,`c_blob_long`,`c_text_tiny`,`c_text`,`c_text_medium`,`c_text_long`,`c_enum`,`c_set`,`c_json`,`c_geometory`,`c_point`,`c_linestring`,`c_polygon`,`c_multipoint`,`c_multilinestring`,`c_multipolygon`,`c_geometrycollection`) "
                + "dbpartition by hash(`c_bigint_64`) tbpartition by hash(`c_bigint_64`) tbpartitions 7\n");

        final ResultSet primaryRs = JdbcUtil.executeQuery(
            "select `id`,`c_bit_1`,`c_bit_8`,`c_bit_16`,`c_bit_32`,`c_bit_64`,`c_tinyint_1`,`c_tinyint_1_un`,`c_tinyint_4`,`c_tinyint_4_un`,`c_tinyint_8`,`c_tinyint_8_un`,`c_smallint_1`,`c_smallint_16`,`c_smallint_16_un`,`c_mediumint_1`,`c_mediumint_24`,`c_mediumint_24_un`,`c_int_1`,`c_int_32`,`c_int_32_un`,`c_bigint_1`,`c_bigint_64_un`,`c_decimal`,`c_decimal_pr`,`c_float`,`c_float_pr`,`c_float_un`,`c_double`,`c_double_pr`,`c_double_un`,`c_date`,`c_datetime`,`c_datetime_1`,`c_datetime_3`,`c_datetime_6`,`c_timestamp_1`,`c_timestamp_3`,`c_timestamp_6`,`c_time`,`c_time_1`,`c_time_3`,`c_time_6`,`c_year`,`c_year_4`,`c_char`,`c_varchar`,`c_binary`,`c_varbinary`,`c_blob_tiny`,`c_blob`,`c_blob_medium`,`c_blob_long`,`c_text_tiny`,`c_text`,`c_text_medium`,`c_text_long`,`c_enum`,`c_set`,`c_json`,`c_geometory`,`c_point`,`c_linestring`,`c_polygon`,`c_multipoint`,`c_multilinestring`,`c_multipolygon`,`c_geometrycollection` from "
                + tableName + " order by id",
            tddlConnection);
        final ResultSet indexRs = JdbcUtil.executeQuery(
            "select `id`,`c_bit_1`,`c_bit_8`,`c_bit_16`,`c_bit_32`,`c_bit_64`,`c_tinyint_1`,`c_tinyint_1_un`,`c_tinyint_4`,`c_tinyint_4_un`,`c_tinyint_8`,`c_tinyint_8_un`,`c_smallint_1`,`c_smallint_16`,`c_smallint_16_un`,`c_mediumint_1`,`c_mediumint_24`,`c_mediumint_24_un`,`c_int_1`,`c_int_32`,`c_int_32_un`,`c_bigint_1`,`c_bigint_64_un`,`c_decimal`,`c_decimal_pr`,`c_float`,`c_float_pr`,`c_float_un`,`c_double`,`c_double_pr`,`c_double_un`,`c_date`,`c_datetime`,`c_datetime_1`,`c_datetime_3`,`c_datetime_6`,`c_timestamp_1`,`c_timestamp_3`,`c_timestamp_6`,`c_time`,`c_time_1`,`c_time_3`,`c_time_6`,`c_year`,`c_year_4`,`c_char`,`c_varchar`,`c_binary`,`c_varbinary`,`c_blob_tiny`,`c_blob`,`c_blob_medium`,`c_blob_long`,`c_text_tiny`,`c_text`,`c_text_medium`,`c_text_long`,`c_enum`,`c_set`,`c_json`,`c_geometory`,`c_point`,`c_linestring`,`c_polygon`,`c_multipoint`,`c_multilinestring`,`c_multipolygon`,`c_geometrycollection` from "
                + indexName + " order by id",
            tddlConnection);

        resultSetContentSameAssert(primaryRs, indexRs, false);
    }

    @Ignore("Support create gsi on table whose pk without auto_increment property")
    public void testAutoIncrementRestriction() {
        final String TABLE_PK_NO_AI_NAME = "gsi_ai_restricted";
        final String GSI_PK_NO_AI_NAME = "g_i_ai_restricted";
        final String PARTITION_DEF = "dbpartition by hash(c1) tbpartition by hash(c1) tbpartitions 2";

        final String SINGLE_PK_NO_AI = MessageFormat.format("create table {0}("
            + "id bigint not null, "
            + "c1 bigint default null, "
            + "c2 varchar(256) default null, "
            + "primary key(id),"
            + "key i_c2(c1, c2)"
            + ") {1}", TABLE_PK_NO_AI_NAME, PARTITION_DEF);
        final String CREATE_GSI_TMPL = "create global index {0} on {1}(id) covering(c2) dbpartition by hash(id)";

        JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE IF EXISTS " + TABLE_PK_NO_AI_NAME);

        JdbcUtil.executeUpdateSuccess(tddlConnection, SINGLE_PK_NO_AI);

        String createGsiSql =
            MessageFormat.format(GSI_ALLOW_ADD_HINT + CREATE_GSI_TMPL, GSI_PK_NO_AI_NAME, TABLE_PK_NO_AI_NAME);
        JdbcUtil.executeUpdateFailed(tddlConnection, createGsiSql,
            "is not AUTO_INCREMENT, which is not allowed to add global index.");

        createGsiSql = MessageFormat
            .format(GSI_ALLOW_ADD_HINT_NO_RESTRICTION + CREATE_GSI_TMPL, GSI_PK_NO_AI_NAME, TABLE_PK_NO_AI_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsiSql);

        JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE IF EXISTS " + TABLE_PK_NO_AI_NAME);
    }

    @Test
    public void testCreateTableWithGSICrossSchema() {
        //cross schema
        getTddlConnection2();
        final String table = "t_cs_order";
        final String dropTable = MessageFormat.format("DROP TABLE IF EXISTS `{0}`.`{1}`;",
            tddlDatabase2, table);
        final String createTableWithGSI = MessageFormat.format("CREATE TABLE `{0}`.`{1}` (\n" +
                "  `id` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP,\n" +
                "  `order_id` varchar(20) DEFAULT NULL,\n" +
                "  `buyer_id` varchar(20) DEFAULT NULL,\n" +
                "  `seller_id` varchar(20) DEFAULT NULL,\n" +
                "  `order_snapshot` longtext DEFAULT NULL,\n" +
                "  `order_detail` longtext DEFAULT NULL,\n" +
                "  PRIMARY KEY (`id`),\n" +
                "  KEY `l_i_order` (`order_id`),\n" +
                "  GLOBAL INDEX `g_i_cs_seller` (`seller_id`) dbpartition by hash(`seller_id`),\n" +
                "  UNIQUE GLOBAL INDEX `g_i_cs_buyer` (`buyer_id`) COVERING (order_snapshot) dbpartition by hash(`buyer_id`)\n"
                +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8 dbpartition by hash(`order_id`);",
            tddlDatabase2, table);

        JdbcUtil.executeUpdateSuccess(tddlConnection, dropTable);

        JdbcUtil.executeUpdateFailed(tddlConnection, createTableWithGSI,
            "so please login with corresponding schema");

        JdbcUtil.executeUpdateSuccess(tddlConnection, GSI_ALLOW_ADD_HINT_NO_RESTRICTION + createTableWithGSI);
        JdbcUtil.executeUpdateSuccess(tddlConnection, dropTable);

        // Again.
        JdbcUtil.executeUpdateSuccess(tddlConnection, GSI_ALLOW_ADD_HINT_NO_RESTRICTION + createTableWithGSI);
        JdbcUtil.executeUpdateSuccess(tddlConnection, dropTable);

        JdbcUtil.executeUpdateSuccess(tddlConnection, dropTable);
    }

    @Test
    public void testCreateGSICrossSchema() {
        //cross schema
        getTddlConnection2();
        final String table = "t_cs_order";
        final String gsi = "g_i_cs_seller";
        final String dropTable = MessageFormat.format("DROP TABLE IF EXISTS `{0}`.`{1}`;",
            tddlDatabase2, table);
        final String createTable = MessageFormat.format("CREATE TABLE `{0}`.`{1}` (\n" +
                "  `id` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP,\n" +
                "  `order_id` varchar(20) DEFAULT NULL,\n" +
                "  `buyer_id` varchar(20) DEFAULT NULL,\n" +
                "  `seller_id` varchar(20) DEFAULT NULL,\n" +
                "  PRIMARY KEY (`id`)\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8 dbpartition by hash(`order_id`);",
            tddlDatabase2, table);
        final String createGSI = MessageFormat.format(GSI_ALLOW_ADD_HINT +
                "alter table `{0}`.`{1}` add GLOBAL unique INDEX {2} using hash (`seller_id`) covering (`order_id`) dbpartition by hash(`seller_id`) tbpartition by hash(`seller_id`) tbpartitions 2;",
            tddlDatabase2, table, gsi);

        JdbcUtil.executeUpdateSuccess(tddlConnection, dropTable);

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);
        JdbcUtil.executeUpdateFailed(tddlConnection, createGSI,
            "Adding global index on other schema is forbidden, so please login with corresponding schema."
            , "Table 't_cs_order' doesn't exist");

        JdbcUtil.executeUpdateSuccess(tddlConnection, dropTable);
    }

    @Test
    public void testCreateGsiUniHash() {
        final String table = "t_uh_order";
        final String gsi = "g_i_uh_seller";
        final String dropTable = MessageFormat.format("DROP TABLE IF EXISTS `{0}`;",
            table);
        final String createTable = MessageFormat.format("CREATE TABLE `{0}` (\n" +
                "  `id` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP,\n" +
                "  `order_id` varchar(20) DEFAULT NULL,\n" +
                "  `buyer_id` varchar(20) DEFAULT NULL,\n" +
                "  `seller_id` varchar(20) DEFAULT NULL,\n" +
                "  PRIMARY KEY (`id`)\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8 dbpartition by UNI_HASH(`order_id`);",
            table);
        final String createGsi = MessageFormat.format(GSI_ALLOW_ADD_HINT +
                "alter table `{0}` add unique GLOBAL INDEX {1} using hash (`seller_id`) dbpartition by hash(`seller_id`) tbpartition by hash(`seller_id`) tbpartitions 2;",
            table, gsi);

        dropTableWithGsi(table, ImmutableList.of(gsi));

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);

        final String tableDef = MessageFormat.format("CREATE TABLE `{0}` (\n" +
                "  `id` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP,\n" +
                "  `order_id` varchar(20) DEFAULT NULL,\n" +
                "  `buyer_id` varchar(20) DEFAULT NULL,\n" +
                "  `seller_id` varchar(20) DEFAULT NULL,\n" +
                "  PRIMARY KEY (`id`),\n" +
                "  UNIQUE GLOBAL INDEX {1} using hash (`seller_id`) COVERING(id, ORDER_ID) dbpartition by hash(`seller_id`) tbpartition by hash(`seller_id`) tbpartitions 2\n"
                +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8 dbpartition by UNI_HASH(`order_id`) tbpartition by UNI_HASH(`order_id`) tbpartitions 2;",
            table, gsi);

        final TableChecker tableChecker = getTableChecker(tddlConnection, table);
        tableChecker.identicalTableDefinitionTo(tableDef, true, Litmus.THROW);

        JdbcUtil.executeUpdateSuccess(tddlConnection, dropTable);
    }

    @Test
    public void testLocalIndexCopy() throws Exception {
        final String table = "t_idx_order";
        final String gsi = "g_i_idx_seller";
        final String dropTable = MessageFormat.format("DROP TABLE IF EXISTS `{0}`;",
            table);
        final String createTable = MessageFormat.format("CREATE TABLE `{0}` (\n" +
                "  `id` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP,\n" +
                "  `x` int default null,\n" +
                "  `order_id` varchar(20) DEFAULT NULL,\n" +
                "  `buyer_id` varchar(20) DEFAULT NULL,\n" +
                "  `seller_id` varchar(20) DEFAULT NULL,\n" +
                "  PRIMARY KEY (`id`),\n" +
                "  key `idx` (`id`),\n" +
                "  key `idx0` (`id`,`buyer_id`),\n" +
                "  key `idx1` (`id`,`seller_id`),\n" +
                "  key `idx2` (`id`,`order_id`),\n" +
                "  key `idx3` (`seller_id`,`buyer_id`),\n" +
                "  key `idx4` (`seller_id`),\n" +
                "  key `idx5` (`seller_id`,`order_id`),\n" +
                "  key `idx6` (`seller_id`,`x`),\n" +
                "  key `idx7` (`x`),\n" +
                "  key `idx8` (`order_id`)" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8 dbpartition by UNI_HASH(`order_id`);",
            table);
        final String createGsi = MessageFormat.format(GSI_ALLOW_ADD_HINT +
                "alter table `{0}` add unique GLOBAL INDEX {1} using hash (`seller_id`) covering (`buyer_id`) dbpartition by hash(`seller_id`) tbpartition by hash(`seller_id`) tbpartitions 2;",
            table, gsi);

        dropTableWithGsi(table, ImmutableList.of(gsi));

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);

        final String tableDef = MessageFormat.format("CREATE TABLE {0} (\n" +
                "  `id` bigint(11) NOT NULL,\n" +
                "  `order_id` varchar(20) DEFAULT NULL,\n" +
                "  `buyer_id` varchar(20) DEFAULT NULL,\n" +
                "  `seller_id` varchar(20) DEFAULT NULL,\n" +
                "  PRIMARY KEY (`id`),\n" +
                "  UNIQUE KEY `auto_shard_key_seller_id` (`seller_id`) USING BTREE,\n" +
                "  KEY `idx` (`id`),\n" +
                "  KEY `idx0` (`id`,`buyer_id`),\n" +
                "  KEY `idx1` (`id`,`seller_id`),\n" +
                "  KEY `idx2` (`id`,`order_id`),\n" +
                "  KEY `idx3` (`seller_id`,`buyer_id`),\n" +
                "  KEY `idx5` (`seller_id`,`order_id`),\n" +
                "  KEY `idx8` (`order_id`)" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8  dbpartition by hash(`seller_id`) tbpartition by hash(`seller_id`) tbpartitions 2",
            gsi);

        final TableChecker tableChecker = getTableChecker(tddlConnection, gsi);
        tableChecker.identicalTableDefinitionAndKeysTo(tableDef, true, Litmus.THROW);

        JdbcUtil.executeUpdateSuccess(tddlConnection, dropTable);
    }

    /**
     * <pre>
     * 1. Add single column index on sharding key of primary table
     * 2. Add single column index on sharding key of index table
     * 3. Add unified index on all gsi index columns
     * </pre>
     */
    @Test
    public void testLocalIndexOnIndexTable() throws Exception {
        final String table = "t_idx_order";
        final String gsi = "g_i_idx_seller";
        final String dropTable = MessageFormat.format("DROP TABLE IF EXISTS `{0}`;", table);
        final String createTable = MessageFormat.format("CREATE TABLE `{0}` (\n"
                + "\t`pk` bigint(11) NOT NULL,\n"
                + "\t`c1` bigint(20) DEFAULT NULL,\n"
                + "\t`c2` bigint(20) DEFAULT NULL,\n"
                + "\t`c3` bigint(20) DEFAULT NULL,\n"
                + "\t`c4` bigint(20) DEFAULT NULL,\n"
                + "\t`c5` varchar(255) DEFAULT NULL,\n"
                + "\t`c6` datetime DEFAULT NULL,\n"
                + "\t`c7` text,\n"
                + "\t`c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
                + "\tPRIMARY KEY (`pk`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8  dbpartition by hash(`c1`) tbpartition by hash(`c1`) tbpartitions 7",
            table);
        final String createGsi = MessageFormat.format(GSI_ALLOW_ADD_HINT +
                "alter table `{0}` add GLOBAL INDEX {1} using hash (`c2`, c3) covering (`c4`) dbpartition by hash(`c2`) tbpartition by hash(`c3`) tbpartitions 2;",
            table, gsi);

        dropTableWithGsi(table, ImmutableList.of(gsi));

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);

        final String tableDef = MessageFormat.format("CREATE TABLE `{0}` (\n"
                + "  `pk` bigint(11) NOT NULL,\n"
                + "  `c1` bigint(20) DEFAULT NULL,\n"
                + "  `c2` bigint(20) DEFAULT NULL,\n"
                + "  `c3` bigint(20) DEFAULT NULL,\n"
                + "  `c4` bigint(20) DEFAULT NULL,\n"
                + "  PRIMARY KEY (`pk`),\n"
                + "  KEY `auto_shard_key_c1` (`c1`) USING BTREE,\n"
                + "  KEY `auto_shard_key_c2` (`c2`) USING BTREE,\n"
                + "  KEY `auto_shard_key_c3` (`c3`) USING BTREE,\n"
                + "  KEY `i_c2_c3` (`c2`,`c3`) USING BTREE\n"
                + ") ENGINE=InnoDB DEFAULT CHARSET=utf8  dbpartition by hash(`c2`) tbpartition by hash(`c3`) tbpartitions 2",
            gsi);

        final TableChecker tableChecker = getTableChecker(tddlConnection, gsi);
        tableChecker.identicalTableDefinitionAndKeysTo(tableDef, true, Litmus.THROW);

        JdbcUtil.executeUpdateSuccess(tddlConnection, dropTable);
    }

    /**
     * <pre>
     * 1. Add single column index on sharding key of primary table
     * 2. Add single column index only on sharding key of index table
     * 3. Add unified index on all gsi index columns
     * </pre>
     */
    @Test
    public void testLocalIndexOnIndexTable1() throws Exception {
        final String table = "t_idx_order";
        final String gsi = "g_i_idx_seller";
        final String dropTable = MessageFormat.format("DROP TABLE IF EXISTS `{0}`;", table);
        final String createTable = MessageFormat.format("CREATE TABLE `{0}` (\n"
                + "\t`pk` bigint(11) NOT NULL,\n"
                + "\t`c1` bigint(20) DEFAULT NULL,\n"
                + "\t`c2` bigint(20) DEFAULT NULL,\n"
                + "\t`c3` bigint(20) DEFAULT NULL,\n"
                + "\t`c4` bigint(20) DEFAULT NULL,\n"
                + "\t`c5` varchar(255) DEFAULT NULL,\n"
                + "\t`c6` datetime DEFAULT NULL,\n"
                + "\t`c7` text,\n"
                + "\t`c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
                + "\tPRIMARY KEY (`pk`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8  dbpartition by hash(`c1`) tbpartition by hash(`c1`) tbpartitions 7",
            table);
        final String createGsi = MessageFormat.format(GSI_ALLOW_ADD_HINT +
                "alter table `{0}` add GLOBAL INDEX {1} using hash (`c2`, c3) dbpartition by hash(`c2`) tbpartition by hash(`c2`) tbpartitions 2;",
            table, gsi);

        dropTableWithGsi(table, ImmutableList.of(gsi));

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);

        final String tableDef = MessageFormat.format("CREATE TABLE `{0}` (\n"
                + "  `pk` bigint(11) NOT NULL,\n"
                + "  `c1` bigint(20) DEFAULT NULL,\n"
                + "  `c2` bigint(20) DEFAULT NULL,\n"
                + "  `c3` bigint(20) DEFAULT NULL,\n"
                + "  PRIMARY KEY (`pk`),\n"
                + "  KEY `auto_shard_key_c1` (`c1`) USING BTREE,\n"
                + "  KEY `auto_shard_key_c2` (`c2`) USING BTREE,\n"
                + "  KEY `i_c2_c3` (`c2`,`c3`) USING BTREE\n"
                + ") ENGINE=InnoDB DEFAULT CHARSET=utf8  dbpartition by hash(`c2`) tbpartition by hash(`c2`) tbpartitions 2",
            gsi);

        final TableChecker tableChecker = getTableChecker(tddlConnection, gsi);
        tableChecker.identicalTableDefinitionAndKeysTo(tableDef, true, Litmus.THROW);

        JdbcUtil.executeUpdateSuccess(tddlConnection, dropTable);
    }

    /**
     * <pre>
     * 1. Add single column index on sharding key of primary table
     * 2. Add single column index only on sharding key of index table
     * 3. Add unified index on all gsi index columns with column length
     * </pre>
     */
    @Test
    public void testLocalIndexOnIndexTable2() throws Exception {
        final String table = "t_idx_order";
        final String gsi = "g_i_idx_seller";
        final String dropTable = MessageFormat.format("DROP TABLE IF EXISTS `{0}`;", table);
        final String createTable = MessageFormat.format("CREATE TABLE `{0}` (\n"
                + "\t`pk` bigint(11) NOT NULL,\n"
                + "\t`c1` bigint(20) DEFAULT NULL,\n"
                + "\t`c2` bigint(20) DEFAULT NULL,\n"
                + "\t`c3` bigint(20) DEFAULT NULL,\n"
                + "\t`c4` bigint(20) DEFAULT NULL,\n"
                + "\t`c5` varchar(255) DEFAULT NULL,\n"
                + "\t`c6` datetime DEFAULT NULL,\n"
                + "\t`c7` text,\n"
                + "\t`c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
                + "\tPRIMARY KEY (`pk`),\n"
                + "\tKEY `idx_c2` USING BTREE (`c2`)" +
                ") ENGINE = InnoDB DEFAULT CHARSET = utf8  dbpartition by hash(`c1`) tbpartition by hash(`c1`) tbpartitions 7",
            table);
        final String createGsi = MessageFormat.format(GSI_ALLOW_ADD_HINT +
                "alter table `{0}` add GLOBAL INDEX {1} using hash (`c2`, c3, c5(128)) dbpartition by hash(`c2`) tbpartition by hash(`c2`) tbpartitions 2;",
            table, gsi);

        dropTableWithGsi(table, ImmutableList.of(gsi));

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);

        final String tableDef = MessageFormat.format("CREATE TABLE `{0}` (\n"
                + "  `pk` bigint(11) NOT NULL,\n"
                + "  `c1` bigint(20) DEFAULT NULL,\n"
                + "  `c2` bigint(20) DEFAULT NULL,\n"
                + "  `c3` bigint(20) DEFAULT NULL,\n"
                + "  `c5` varchar(255) DEFAULT NULL,\n"
                + "  PRIMARY KEY (`pk`),\n"
                + "  KEY `idx_c2` (`c2`) USING BTREE,\n"
                + "  KEY `auto_shard_key_c1` (`c1`) USING BTREE,\n"
                + "  KEY `i_c2_c3_c5` (`c2`,`c3`,`c5`(128)) USING BTREE\n"
                + ") ENGINE=InnoDB DEFAULT CHARSET=utf8  dbpartition by hash(`c2`) tbpartition by hash(`c2`) tbpartitions 2",
            gsi);

        final TableChecker tableChecker = getTableChecker(tddlConnection, gsi);
        tableChecker.identicalTableDefinitionAndKeysTo(tableDef, true, Litmus.THROW);

        JdbcUtil.executeUpdateSuccess(tddlConnection, dropTable);
    }

    /**
     * <pre>
     * 1. Add single column index on sharding key of primary table
     * 2. Skip add index if exists index of which first column is sharding key
     * 3. Add unified index on all gsi index columns, and do not check duplicate index
     * </pre>
     */
    @Test
    public void testLocalIndexOnIndexTable3() throws Exception {
        final String table = "t_idx_order";
        final String gsi = "g_i_idx_seller";
        final String dropTable = MessageFormat.format("DROP TABLE IF EXISTS `{0}`;", table);
        final String createTable = MessageFormat.format("CREATE TABLE `{0}` (\n"
                + "\t`pk` bigint(11) NOT NULL,\n"
                + "\t`c1` bigint(20) DEFAULT NULL,\n"
                + "\t`c2` bigint(20) DEFAULT NULL,\n"
                + "\t`c3` bigint(20) DEFAULT NULL,\n"
                + "\t`c4` bigint(20) DEFAULT NULL,\n"
                + "\t`c5` varchar(255) DEFAULT NULL,\n"
                + "\t`c6` datetime DEFAULT NULL,\n"
                + "\t`c7` text,\n"
                + "\t`c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
                + "\tPRIMARY KEY (`pk`),\n"
                + "\tKEY `idx_c2_c6` USING BTREE (`c2`, c6)" +
                ") ENGINE = InnoDB DEFAULT CHARSET = utf8  dbpartition by hash(`c1`) tbpartition by hash(`c1`) tbpartitions 7",
            table);
        final String createGsi = MessageFormat.format(GSI_ALLOW_ADD_HINT +
                "alter table `{0}` add GLOBAL INDEX {1} using hash (`c2`, c6) dbpartition by hash(`c2`) tbpartition by hash(`c2`) tbpartitions 2;",
            table, gsi);

        dropTableWithGsi(table, ImmutableList.of(gsi));

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);

        final String tableDef = MessageFormat.format("CREATE TABLE `{0}` (\n"
                + "  `pk` bigint(11) NOT NULL,\n"
                + "  `c1` bigint(20) DEFAULT NULL,\n"
                + "  `c2` bigint(20) DEFAULT NULL,\n"
                + "  `c6` datetime DEFAULT NULL,\n"
                + "  PRIMARY KEY (`pk`),\n"
                + "  KEY `idx_c2_c6` USING BTREE (`c2`, c6),\n"
                + "  KEY `auto_shard_key_c1` (`c1`) USING BTREE\n"
                + ") ENGINE=InnoDB DEFAULT CHARSET=utf8  dbpartition by hash(`c2`) tbpartition by hash(`c2`) tbpartitions 2",
            gsi);

        final TableChecker tableChecker = getTableChecker(tddlConnection, gsi);
        tableChecker.identicalTableDefinitionAndKeysTo(tableDef, true, Litmus.THROW);

        JdbcUtil.executeUpdateSuccess(tddlConnection, dropTable);
    }

    /**
     * <pre>
     * 1. Add single column index on sharding key of primary table
     * 2. Add single column index on sharding key of index table
     * 3. Add unified index on all gsi index columns
     * </pre>
     */
    @Test
    public void testLocalIndexOnIndexTable4() throws Exception {
        final String table = "t_idx_order";
        final String gsi = "g_i_idx_seller";
        final String dropTable = MessageFormat.format("DROP TABLE IF EXISTS `{0}`;", table);
        final String createTable = MessageFormat.format("CREATE TABLE `{0}` (\n"
                + "\t`pk` bigint(11) NOT NULL,\n"
                + "\t`c1` bigint(20) DEFAULT NULL,\n"
                + "\t`c2` bigint(20) DEFAULT NULL,\n"
                + "\t`c3` bigint(20) DEFAULT NULL,\n"
                + "\t`c4` bigint(20) DEFAULT NULL,\n"
                + "\t`c5` varchar(255) DEFAULT NULL,\n"
                + "\t`c6` datetime DEFAULT NULL,\n"
                + "\t`c7` text,\n"
                + "\t`c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
                + "\tPRIMARY KEY (`pk`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8  dbpartition by hash(`c1`) tbpartition by hash(`c1`) tbpartitions 7",
            table);
        final String createGsi = MessageFormat.format(GSI_ALLOW_ADD_HINT +
                "CREATE GLOBAL INDEX {1} ON {0} (`c2`, c3) covering (`c4`) dbpartition by hash(`c2`) tbpartition by hash(`c3`) tbpartitions 2;",
            table, gsi);

        dropTableWithGsi(table, ImmutableList.of(gsi));

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);

        final String tableDef = MessageFormat.format("CREATE TABLE `{0}` (\n"
                + "  `pk` bigint(11) NOT NULL,\n"
                + "  `c1` bigint(20) DEFAULT NULL,\n"
                + "  `c2` bigint(20) DEFAULT NULL,\n"
                + "  `c3` bigint(20) DEFAULT NULL,\n"
                + "  `c4` bigint(20) DEFAULT NULL,\n"
                + "  PRIMARY KEY (`pk`),\n"
                + "  KEY `auto_shard_key_c1` (`c1`) USING BTREE,\n"
                + "  KEY `auto_shard_key_c2` (`c2`) USING BTREE,\n"
                + "  KEY `auto_shard_key_c3` (`c3`) USING BTREE,\n"
                + "  KEY `i_c2_c3` (`c2`,`c3`) USING BTREE\n"
                + ") ENGINE=InnoDB DEFAULT CHARSET=utf8  dbpartition by hash(`c2`) tbpartition by hash(`c3`) tbpartitions 2",
            gsi);

        final TableChecker tableChecker = getTableChecker(tddlConnection, gsi);
        tableChecker.identicalTableDefinitionAndKeysTo(tableDef, true, Litmus.THROW);

        JdbcUtil.executeUpdateSuccess(tddlConnection, dropTable);
    }

    /**
     * <pre>
     * 1. Add single column index on sharding key of primary table
     * 2. Add single column index only on sharding key of index table
     * 3. Add unified index on all gsi index columns
     * </pre>
     */
    @Test
    public void testLocalIndexOnIndexTable5() throws Exception {
        final String table = "t_idx_order";
        final String gsi = "g_i_idx_seller";
        final String dropTable = MessageFormat.format("DROP TABLE IF EXISTS `{0}`;", table);
        final String createTable = MessageFormat.format("CREATE TABLE `{0}` (\n"
                + "\t`pk` bigint(11) NOT NULL,\n"
                + "\t`c1` bigint(20) DEFAULT NULL,\n"
                + "\t`c2` bigint(20) DEFAULT NULL,\n"
                + "\t`c3` bigint(20) DEFAULT NULL,\n"
                + "\t`c4` bigint(20) DEFAULT NULL,\n"
                + "\t`c5` varchar(255) DEFAULT NULL,\n"
                + "\t`c6` datetime DEFAULT NULL,\n"
                + "\t`c7` text,\n"
                + "\t`c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
                + "\tPRIMARY KEY (`pk`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8  dbpartition by hash(`c1`) tbpartition by hash(`c1`) tbpartitions 7",
            table);
        final String createGsi = MessageFormat.format(GSI_ALLOW_ADD_HINT +
                "CREATE GLOBAL INDEX {1} ON {0} (`c2`, c3) dbpartition by hash(`c2`) tbpartition by hash(`c2`) tbpartitions 2;",
            table, gsi);

        dropTableWithGsi(table, ImmutableList.of(gsi));

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);

        final String tableDef = MessageFormat.format("CREATE TABLE `{0}` (\n"
                + "  `pk` bigint(11) NOT NULL,\n"
                + "  `c1` bigint(20) DEFAULT NULL,\n"
                + "  `c2` bigint(20) DEFAULT NULL,\n"
                + "  `c3` bigint(20) DEFAULT NULL,\n"
                + "  PRIMARY KEY (`pk`),\n"
                + "  KEY `auto_shard_key_c1` (`c1`) USING BTREE,\n"
                + "  KEY `auto_shard_key_c2` (`c2`) USING BTREE,\n"
                + "  KEY `i_c2_c3` (`c2`,`c3`) USING BTREE\n"
                + ") ENGINE=InnoDB DEFAULT CHARSET=utf8  dbpartition by hash(`c2`) tbpartition by hash(`c2`) tbpartitions 2",
            gsi);

        final TableChecker tableChecker = getTableChecker(tddlConnection, gsi);
        tableChecker.identicalTableDefinitionAndKeysTo(tableDef, true, Litmus.THROW);

        JdbcUtil.executeUpdateSuccess(tddlConnection, dropTable);
    }

    /**
     * <pre>
     * 1. Add single column index on sharding key of primary table
     * 2. Add single column index only on sharding key of index table
     * 3. Add unified index on all gsi index columns with column length
     * </pre>
     */
    @Test
    public void testLocalIndexOnIndexTable6() throws Exception {
        final String table = "t_idx_order";
        final String gsi = "g_i_idx_seller";
        final String dropTable = MessageFormat.format("DROP TABLE IF EXISTS `{0}`;", table);
        final String createTable = MessageFormat.format("CREATE TABLE `{0}` (\n"
                + "\t`pk` bigint(11) NOT NULL,\n"
                + "\t`c1` bigint(20) DEFAULT NULL,\n"
                + "\t`c2` bigint(20) DEFAULT NULL,\n"
                + "\t`c3` bigint(20) DEFAULT NULL,\n"
                + "\t`c4` bigint(20) DEFAULT NULL,\n"
                + "\t`c5` varchar(255) DEFAULT NULL,\n"
                + "\t`c6` datetime DEFAULT NULL,\n"
                + "\t`c7` text,\n"
                + "\t`c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
                + "\tPRIMARY KEY (`pk`),\n"
                + "\tKEY `idx_c2` USING BTREE (`c2`)" +
                ") ENGINE = InnoDB DEFAULT CHARSET = utf8  dbpartition by hash(`c1`) tbpartition by hash(`c1`) tbpartitions 7",
            table);
        final String createGsi = MessageFormat.format(GSI_ALLOW_ADD_HINT +
                "CREATE GLOBAL INDEX {1} ON {0} (`c2`, c3, c5(128)) dbpartition by hash(`c2`) tbpartition by hash(`c2`) tbpartitions 2;",
            table, gsi);

        dropTableWithGsi(table, ImmutableList.of(gsi));

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);

        final String tableDef = MessageFormat.format("CREATE TABLE `{0}` (\n"
                + "  `pk` bigint(11) NOT NULL,\n"
                + "  `c1` bigint(20) DEFAULT NULL,\n"
                + "  `c2` bigint(20) DEFAULT NULL,\n"
                + "  `c3` bigint(20) DEFAULT NULL,\n"
                + "  `c5` varchar(255) DEFAULT NULL,\n"
                + "  PRIMARY KEY (`pk`),\n"
                + "  KEY `idx_c2` (`c2`) USING BTREE,\n"
                + "  KEY `auto_shard_key_c1` (`c1`) USING BTREE,\n"
                + "  KEY `i_c2_c3_c5` (`c2`,`c3`,`c5`(128)) USING BTREE\n"
                + ") ENGINE=InnoDB DEFAULT CHARSET=utf8  dbpartition by hash(`c2`) tbpartition by hash(`c2`) tbpartitions 2",
            gsi);

        final TableChecker tableChecker = getTableChecker(tddlConnection, gsi);
        tableChecker.identicalTableDefinitionAndKeysTo(tableDef, true, Litmus.THROW);

        JdbcUtil.executeUpdateSuccess(tddlConnection, dropTable);
    }

    /**
     * <pre>
     * 1. Add single column index on sharding key of primary table
     * 2. Skip add index if exists index of which first column is sharding key
     * 3. Add unified index on all gsi index columns, and do not check duplicate index
     * </pre>
     */
    @Test
    public void testLocalIndexOnIndexTable7() throws Exception {
        final String table = "t_idx_order";
        final String gsi = "g_i_idx_seller";
        final String dropTable = MessageFormat.format("DROP TABLE IF EXISTS `{0}`;", table);
        final String createTable = MessageFormat.format("CREATE TABLE `{0}` (\n"
                + "\t`pk` bigint(11) NOT NULL,\n"
                + "\t`c1` bigint(20) DEFAULT NULL,\n"
                + "\t`c2` bigint(20) DEFAULT NULL,\n"
                + "\t`c3` bigint(20) DEFAULT NULL,\n"
                + "\t`c4` bigint(20) DEFAULT NULL,\n"
                + "\t`c5` varchar(255) DEFAULT NULL,\n"
                + "\t`c6` datetime DEFAULT NULL,\n"
                + "\t`c7` text,\n"
                + "\t`c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
                + "\tPRIMARY KEY (`pk`),\n"
                + "\tKEY `idx_c2_c6` USING BTREE (`c2`, c6)" +
                ") ENGINE = InnoDB DEFAULT CHARSET = utf8  dbpartition by hash(`c1`) tbpartition by hash(`c1`) tbpartitions 7",
            table);
        final String createGsi = MessageFormat.format(GSI_ALLOW_ADD_HINT +
                "CREATE GLOBAL INDEX {1} ON {0} (`c2`, c6) dbpartition by hash(`c2`) tbpartition by hash(`c2`) tbpartitions 2;",
            table, gsi);

        dropTableWithGsi(table, ImmutableList.of(gsi));

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);

        final String tableDef = MessageFormat.format("CREATE TABLE `{0}` (\n"
                + "  `pk` bigint(11) NOT NULL,\n"
                + "  `c1` bigint(20) DEFAULT NULL,\n"
                + "  `c2` bigint(20) DEFAULT NULL,\n"
                + "  `c6` datetime DEFAULT NULL,\n"
                + "  PRIMARY KEY (`pk`),\n"
                + "  KEY `idx_c2_c6` USING BTREE (`c2`, c6),\n"
                + "  KEY `auto_shard_key_c1` (`c1`) USING BTREE\n"
                + ") ENGINE=InnoDB DEFAULT CHARSET=utf8  dbpartition by hash(`c2`) tbpartition by hash(`c2`) tbpartitions 2",
            gsi);

        final TableChecker tableChecker = getTableChecker(tddlConnection, gsi);
        tableChecker.identicalTableDefinitionAndKeysTo(tableDef, true, Litmus.THROW);

        JdbcUtil.executeUpdateSuccess(tddlConnection, dropTable);
    }

    /**
     * <pre>
     * 1. Add single column index on sharding key of primary table
     * 2. Skip add index if exists index of which first column is sharding key
     * 3. Add unified index on all gsi index columns, and do not check duplicate index
     * </pre>
     */
    @Test
    public void testLocalIndexOnIndexTable8() throws Exception {
        final String table = "t_idx_order";
        final String gsi = "g_i_idx_seller";
        final String dropTable = MessageFormat.format("DROP TABLE IF EXISTS `{0}`;", table);
        final String createTable = MessageFormat.format("CREATE TABLE `{0}` (\n"
                + "\t`pk` bigint(11) NOT NULL,\n"
                + "\t`c1` bigint(20) DEFAULT NULL,\n"
                + "\t`c2` bigint(20) UNIQUE DEFAULT NULL,\n"
                + "\t`c3` bigint(20) NOT NULL,\n"
                + "\t`c4` bigint(20) DEFAULT NULL,\n"
                + "\t`c5` varchar(255) DEFAULT NULL,\n"
                + "\t`c6` datetime DEFAULT NULL,\n"
                + "\t`c7` text,\n"
                + "\t`c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
                + "\tPRIMARY KEY (`pk`, c3)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8  dbpartition by hash(`c1`) tbpartition by hash(`c1`) tbpartitions 7",
            table);
        final String createGsi = MessageFormat.format(GSI_ALLOW_ADD_HINT +
                "CREATE GLOBAL INDEX {1} ON {0} (`c2`, c6) dbpartition by hash(`c2`) tbpartition by hash(`c2`) tbpartitions 2;",
            table, gsi);

        dropTableWithGsi(table, ImmutableList.of(gsi));

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);

        final String tableDef = MessageFormat.format("CREATE TABLE `{0}` (\n"
                + "  `pk` bigint(11) NOT NULL,\n"
                + "  `c1` bigint(20) DEFAULT NULL,\n"
                + "  `c2` bigint(20) DEFAULT NULL,\n"
                + "  `c3` bigint(20) NOT NULL,\n"
                + "  `c6` datetime DEFAULT NULL,\n"
                + "  PRIMARY KEY (`pk`, `c3`),\n"
                + "  UNIQUE KEY `c2` (`c2`),\n"
                + "  KEY `auto_shard_key_c1` (`c1`) USING BTREE,\n"
                + "  KEY `i_c2_c6` (`c2`,`c6`) USING BTREE\n"
                + ") ENGINE=InnoDB DEFAULT CHARSET=utf8  dbpartition by hash(`c2`) tbpartition by hash(`c2`) tbpartitions 2",
            gsi);

        final TableChecker tableChecker = getTableChecker(tddlConnection, gsi);
        tableChecker.identicalTableDefinitionAndKeysTo(tableDef, true, Litmus.THROW);

        JdbcUtil.executeUpdateSuccess(tddlConnection, dropTable);
    }

    /**
     * <pre>
     * 1. Add single column index on sharding key of primary table
     * 2. Skip add index if exists index of which first column is sharding key
     * 3. Add unified index on all gsi index columns, and do not check duplicate index
     * </pre>
     */
    @Test
    public void testLocalIndexOnIndexTable9() throws Exception {
        final String table = "t_idx_order";
        final String gsi = "g_i_idx_seller";
        final String dropTable = MessageFormat.format("DROP TABLE IF EXISTS `{0}`;", table);
        final String createTable = MessageFormat.format("CREATE TABLE `{0}` (\n"
                + "\t`pk` bigint(11) NOT NULL,\n"
                + "\t`c1` bigint(20) DEFAULT NULL,\n"
                + "\t`c2` bigint(20) UNIQUE DEFAULT NULL,\n"
                + "\t`c3` bigint(20) NOT NULL,\n"
                + "\t`c4` bigint(20) DEFAULT NULL,\n"
                + "\t`c5` varchar(255) DEFAULT NULL,\n"
                + "\t`c6` datetime DEFAULT NULL,\n"
                + "\t`c7` text,\n"
                + "\t`c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
                + "\tPRIMARY KEY (`pk`, c3)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8  dbpartition by hash(`c1`) tbpartition by hash(`c1`) tbpartitions 7",
            table);
        final String createGsi = MessageFormat.format(GSI_ALLOW_ADD_HINT +
                "CREATE UNIQUE GLOBAL INDEX {1} ON {0} (`c2`, c5(32), c6) dbpartition by hash(`c2`) tbpartition by hash(c5) tbpartitions 2;",
            table, gsi);

        dropTableWithGsi(table, ImmutableList.of(gsi));

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);

        final String tableDef = MessageFormat.format("CREATE TABLE `{0}` (\n"
                + "  `pk` bigint(11) NOT NULL,\n"
                + "  `c1` bigint(20) DEFAULT NULL,\n"
                + "  `c2` bigint(20) DEFAULT NULL,\n"
                + "  `c3` bigint(20) NOT NULL,\n"
                + "  `c5` varchar(255) DEFAULT NULL,\n"
                + "  `c6` datetime DEFAULT NULL,\n"
                + "  PRIMARY KEY (`pk`, `c3`),\n"
                + "  UNIQUE KEY `c2` (`c2`),\n"
                + "  KEY `auto_shard_key_c1` (`c1`) USING BTREE,\n"
                + "  UNIQUE KEY `i_c2_c5_c6` (`c2`,`c5`(32),`c6`) USING BTREE\n"
                + ") ENGINE=InnoDB DEFAULT CHARSET=utf8  dbpartition by hash(`c2`) tbpartition by hash(`c5`) tbpartitions 2",
            gsi);

        final TableChecker tableChecker = getTableChecker(tddlConnection, gsi);
        tableChecker.identicalTableDefinitionAndKeysTo(tableDef, true, Litmus.THROW);

        JdbcUtil.executeUpdateSuccess(tddlConnection, dropTable);
    }

    /**
     * <pre>
     * 1. Add single column index on sharding key of primary table
     * 2. Add single column index on sharding key of index table
     * 3. Add unified index on all gsi index columns
     * </pre>
     */
    @Test
    public void testLocalIndexOnIndexTable10() throws Exception {
        final String table = "idx_order_table";
        final String gsi = "idx_order_table_seller";
        final String dropTable = MessageFormat.format("DROP TABLE IF EXISTS `{0}`;", table);
        final String createTable = MessageFormat.format("CREATE TABLE `{0}` (\n"
                + "\t`id` BIGINT(20) NOT NULL AUTO_INCREMENT,\n"
                + "\t`shop` VARCHAR(8) NOT NULL,\n"
                + "\t`order_no` VARCHAR(8) NOT NULL,\n"
                + "\t`other` VARCHAR(20) NULL,\n"
                + "\t`create_time` TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),\n"
                + "\t`update_time` TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),\n"
                + "\tPRIMARY KEY (`id`),\n"
                + "\tUNIQUE KEY `uk_orderno` (`order_no`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8  dbpartition by UNI_HASH(`shop`) tbpartition by UNI_HASH(`shop`)",
            table);
        final String createGsi = MessageFormat.format(GSI_ALLOW_ADD_HINT +
                "CREATE UNIQUE GLOBAL INDEX {1} ON {0} (`order_no`) COVERING (`id`, `shop`) dbpartition by hash(`order_no`);",
            table, gsi);

        dropTableWithGsi(table, ImmutableList.of(gsi));

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);

        final String tableDef = MessageFormat.format("CREATE TABLE `{0}` (\n"
                + "  `id` bigint(20) NOT NULL,\n"
                + "  `shop` varchar(8) NOT NULL,\n"
                + "  `order_no` varchar(8) NOT NULL,\n"
                + "  PRIMARY KEY (`id`),\n"
                + "\tUNIQUE KEY `uk_orderno` (`order_no`) USING BTREE,\n"
                + "\tKEY `auto_shard_key_shop` (`shop`) USING BTREE\n"
                + ") ENGINE=InnoDB DEFAULT CHARSET=utf8  dbpartition by hash(`order_no`)",
            gsi);

        final TableChecker tableChecker = getTableChecker(tddlConnection, gsi);
        tableChecker.identicalTableDefinitionAndKeysTo(tableDef, true, Litmus.THROW);

        JdbcUtil.executeUpdateSuccess(tddlConnection, dropTable);
    }

    @Test
    public void testLocalIndexOnIndexTable11() throws Exception {
        final String table = "idx_table";
        final String gsi = "idx_table_gsi";
        final String dropTable = MessageFormat.format("DROP TABLE IF EXISTS `{0}`;", table);
        final String createTable = MessageFormat.format("CREATE TABLE `{0}` (\n"
                + "\t`pk` bigint(11) NOT NULL AUTO_INCREMENT, \n"
                + "\t`c1` bigint(20) DEFAULT NULL, \n"
                + "\t`c2` bigint(20) DEFAULT NULL, \n"
                + "\t`c3` bigint(20) DEFAULT NULL, \n"
                + "\t`c4` bigint(20) DEFAULT NULL, \n"
                + "\t`c5` bigint(20) DEFAULT NULL, \n"
                + "\t`create_time` TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),\n"
                + "\t`update_time` TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),\n"
                + "\tPRIMARY KEY (`pk`),\n"
                + "\tINDEX c3_4(`c3`,`c4`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8 dbpartition by hash(c1) tbpartition by hash(c2) tbpartitions 2;",
            table);
        final String createGsi = MessageFormat.format(GSI_ALLOW_ADD_HINT +
                "CREATE UNIQUE GLOBAL INDEX {1} ON {0} (`c3`,`c4`) dbpartition by hash(`c3`);",
            table, gsi);

        dropTableWithGsi(table, ImmutableList.of(gsi));

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);

        final String tableDef = MessageFormat.format("CREATE TABLE `{0}` (\n"
                + "  `pk` bigint(11) NOT NULL,\n"
                + "  `c1` bigint(20) DEFAULT NULL,\n"
                + "  `c2` bigint(20) DEFAULT NULL,\n"
                + "  `c3` bigint(20) DEFAULT NULL,\n"
                + "  `c4` bigint(20) DEFAULT NULL,\n"
                + "  PRIMARY KEY (`pk`),\n"
                + "  UNIQUE KEY `auto_shard_key_c3_c4` (`c3`,`c4`) USING BTREE,\n"
                + "  KEY `auto_shard_key_c1` (`c1`) USING BTREE,\n"
                + "  KEY `auto_shard_key_c2` (`c2`) USING BTREE\n"
                + ") ENGINE=InnoDB DEFAULT CHARSET=utf8 dbpartition by hash(`c3`)",
            gsi);

        final TableChecker tableChecker = getTableChecker(tddlConnection, gsi);
        tableChecker.identicalTableDefinitionAndKeysTo(tableDef, true, Litmus.THROW);

        JdbcUtil.executeUpdateSuccess(tddlConnection, dropTable);
    }

    /***
     * Let DN rather CN validate whether the GSI key is too long.
     * The following test cases depend on CN's validation, so ignore them.
     */
    @Ignore
    @Test
    public void testCreateGsiColumnTooLong() {
        final String table = "t_idx_order";
        final String gsi = "g_i_idx_seller";
        final String gsi2 = "g_i_idx_seller2";
        final String dropTable = MessageFormat.format("DROP TABLE IF EXISTS `{0}`;", table);
        final String createTable = MessageFormat.format("CREATE TABLE `{0}` (\n"
                + "\t`pk` bigint(11) NOT NULL,\n"
                + "\t`vc1025` varchar(1025) DEFAULT NULL,\n"
                + "\t`vb3073` varbinary(3073) DEFAULT NULL,\n"
                + "\t`vclatin13073` varchar(3073) CHARACTER SET latin1 DEFAULT NULL,\n"
                + "\tPRIMARY KEY (`pk`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8 dbpartition by hash(`pk`)",
            table);

        dropTableWithGsi(table, ImmutableList.of(gsi, gsi2));

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        // 创建GSI, 不指定长度且超长/指定长度且超长/指定字符集且超长, 报错
        JdbcUtil.executeUpdateFailed(tddlConnection,
            "CREATE GLOBAL INDEX "
                + gsi + " ON " + table + " (vc1025) dbpartition by hash(vc1025)",
            "too long, max key length is");
        JdbcUtil.executeUpdateFailed(tddlConnection,
            "CREATE GLOBAL INDEX "
                + gsi + " ON " + table + " (vc1025(1025)) dbpartition by hash(vc1025)",
            "too long, max key length is");
        JdbcUtil.executeUpdateFailed(tddlConnection,
            "CREATE UNIQUE GLOBAL INDEX "
                + gsi + " ON " + table + " (vclatin13073) dbpartition by hash(vclatin13073)",
            "too long, max key length is");

        // 测试varbinary的情况
        JdbcUtil.executeUpdateFailed(tddlConnection,
            "CREATE GLOBAL INDEX "
                + gsi2 + " ON " + table + " (vb3073) dbpartition by hash(vb3073)",
            "too long, max key length is");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "CREATE UNIQUE GLOBAL INDEX "
                + gsi2 + " ON " + table + " (vb3073(767)) dbpartition by hash(vb3073)");
//
        JdbcUtil.executeUpdateSuccess(tddlConnection, dropTable);

    }

    /***
     * Let DN rather CN validate whether the GSI key is too long.
     * The following test cases depend on CN's validation, so ignore them.
     */
    @Ignore
    @Test
    public void testAlterTableAddGsiWithHintColumnTooLong() {
        final String primaryTable = "t_idx_order";
        final String indexTable = "g_i_idx_seller";

        dropTableWithGsi(primaryTable, ImmutableList.of(indexTable));
        String sql = String.format("create table "
            + "%s(a int, b varchar(1025), d varbinary(3073)"
            + ", primary key(a)"
            + ") dbpartition by hash(a) dbpartitions 2", primaryTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format(HINT_ALLOW_ALTER_GSI_INDIRECTLY
                + "alter table %s add global index %s (b) dbpartition by hash(b);",
            primaryTable, indexTable);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "too long, max key length is");

        sql = String.format(HINT_ALLOW_ALTER_GSI_INDIRECTLY
                + "alter table %s add unique global index %s (d) dbpartition by hash(d);",
            primaryTable, indexTable);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "too long, max key length is");

        dropTableWithGsi(primaryTable, ImmutableList.of(indexTable));
    }

    @Test
    @Ignore
    public void testCreateWithoutIndexName0() {
        final String primaryTable = "t_idx_order";

        dropTableWithGsi(primaryTable, ImmutableList.of());
        String sql = String.format("CREATE TABLE `%s` (\n"
                + "  `id` bigint(11) NOT NULL AUTO_INCREMENT,\n"
                + "  `x` int,\n"
                + "  `order_id` varchar(20) DEFAULT NULL,\n"
                + "  `seller_id` varchar(20) DEFAULT NULL,\n"
                + "  PRIMARY KEY (`id`),\n"
                + "  GLOBAL INDEX (`seller_id`) dbpartition by hash(`seller_id`) tbpartition by hash(`seller_id`)  tbpartitions 2\n"
                + ") ENGINE=InnoDB DEFAULT CHARSET=utf8 dbpartition by hash(`order_id`) tbpartition by hash(`order_id`) tbpartitions 2;\n",
            primaryTable);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Global (clustered) secondary index must have a name.");

        dropTableWithGsi(primaryTable, ImmutableList.of());
        sql = String.format("CREATE TABLE `%s` (\n"
                + "  `id` bigint(11) NOT NULL AUTO_INCREMENT,\n"
                + "  `x` int,\n"
                + "  `order_id` varchar(20) DEFAULT NULL,\n"
                + "  `seller_id` varchar(20) DEFAULT NULL,\n"
                + "  PRIMARY KEY (`id`),\n"
                + "  UNIQUE GLOBAL INDEX (`seller_id`) dbpartition by hash(`seller_id`) tbpartition by hash(`seller_id`)  tbpartitions 2\n"
                + ") ENGINE=InnoDB DEFAULT CHARSET=utf8 dbpartition by hash(`order_id`) tbpartition by hash(`order_id`) tbpartitions 2;\n",
            primaryTable);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Global (clustered) secondary index must have a name.");

        dropTableWithGsi(primaryTable, ImmutableList.of());
    }

    @Test
    public void testCreateWithoutIndexName1() {
        final String primaryTable = "t_idx_order";

        dropTableWithGsi(primaryTable, ImmutableList.of());
        String sql = String.format("CREATE TABLE `%s` (\n"
                + "  `id` bigint(11) NOT NULL AUTO_INCREMENT,\n"
                + "  `x` int,\n"
                + "  `order_id` varchar(20) DEFAULT NULL,\n"
                + "  `seller_id` varchar(20) DEFAULT NULL,\n"
                + "  PRIMARY KEY (`id`),\n"
                + "  KEY `xxx` (`seller_id`, `x`)\n"
                + ") ENGINE=InnoDB DEFAULT CHARSET=utf8 dbpartition by hash(`order_id`) tbpartition by hash(`order_id`) tbpartitions 2;\n",
            primaryTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format(
            "CREATE GLOBAL INDEX on `%s`(`seller_id`) covering (`seller_id`,`x`) dbpartition by hash(`seller_id`) tbpartition by hash(`seller_id`)  tbpartitions 2;",
            primaryTable);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Global (clustered) secondary index must have a name.");

        sql = String.format(
            "alter table `%s` add GLOBAL INDEX (`seller_id`) covering (`seller_id`,`x`) dbpartition by hash(`seller_id`) tbpartition by hash(`seller_id`)  tbpartitions 2;",
            primaryTable);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Global (clustered) secondary index must have a name.");

        sql = String.format(
            "CREATE UNIQUE GLOBAL INDEX on `%s`(`seller_id`) covering (`seller_id`,`x`) dbpartition by hash(`seller_id`) tbpartition by hash(`seller_id`)  tbpartitions 2;",
            primaryTable);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Global (clustered) secondary index must have a name.");

        sql = String.format(
            "alter table `%s` add UNIQUE GLOBAL INDEX (`seller_id`) covering (`seller_id`,`x`) dbpartition by hash(`seller_id`) tbpartition by hash(`seller_id`)  tbpartitions 2;",
            primaryTable);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Global (clustered) secondary index must have a name.");

        dropTableWithGsi(primaryTable, ImmutableList.of());
    }

    @Test
    public void testCreateWithoutPartition0() {
        final String primaryTable = "t_idx_order";
        final String indexTable = "g_i_idx_seller";

        dropTableWithGsi(primaryTable, ImmutableList.of());
        String sql = String.format("CREATE TABLE `%s` (\n"
                + "  `id` bigint(11) NOT NULL AUTO_INCREMENT,\n"
                + "  `x` int,\n"
                + "  `order_id` varchar(20) DEFAULT NULL,\n"
                + "  `seller_id` varchar(20) DEFAULT NULL,\n"
                + "  PRIMARY KEY (`id`),\n"
                + "  GLOBAL INDEX `%s`(`seller_id`)\n"
                + ") ENGINE=InnoDB DEFAULT CHARSET=utf8 dbpartition by hash(`order_id`) tbpartition by hash(`order_id`) tbpartitions 2;\n",
            primaryTable, indexTable);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "global secondary index must have dbpartition/partition");

        dropTableWithGsi(primaryTable, ImmutableList.of());
        sql = String.format("CREATE TABLE `%s` (\n"
                + "  `id` bigint(11) NOT NULL AUTO_INCREMENT,\n"
                + "  `x` int,\n"
                + "  `order_id` varchar(20) DEFAULT NULL,\n"
                + "  `seller_id` varchar(20) DEFAULT NULL,\n"
                + "  PRIMARY KEY (`id`),\n"
                + "  UNIQUE GLOBAL INDEX `%s`(`seller_id`)\n"
                + ") ENGINE=InnoDB DEFAULT CHARSET=utf8 dbpartition by hash(`order_id`) tbpartition by hash(`order_id`) tbpartitions 2;\n",
            primaryTable, indexTable);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "global secondary index must have dbpartition/partition");

        dropTableWithGsi(primaryTable, ImmutableList.of());
        sql = String.format("CREATE TABLE `%s` (\n"
                + "  `id` bigint(11) NOT NULL AUTO_INCREMENT,\n"
                + "  `x` int,\n"
                + "  `order_id` varchar(20) DEFAULT NULL,\n"
                + "  `seller_id` varchar(20) DEFAULT NULL,\n"
                + "  PRIMARY KEY (`id`),\n"
                + "  GLOBAL INDEX `%s`(`seller_id`) tbpartition by hash(`seller_id`)  tbpartitions 2\n"
                + ") ENGINE=InnoDB DEFAULT CHARSET=utf8 dbpartition by hash(`order_id`) tbpartition by hash(`order_id`) tbpartitions 2;\n",
            primaryTable, indexTable);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql,
            "Global (clustered) secondary index should not specify tbpartition without dbpartition.");

        dropTableWithGsi(primaryTable, ImmutableList.of());
        sql = String.format("CREATE TABLE `%s` (\n"
                + "  `id` bigint(11) NOT NULL AUTO_INCREMENT,\n"
                + "  `x` int,\n"
                + "  `order_id` varchar(20) DEFAULT NULL,\n"
                + "  `seller_id` varchar(20) DEFAULT NULL,\n"
                + "  PRIMARY KEY (`id`),\n"
                + "  UNIQUE GLOBAL INDEX `%s`(`seller_id`) tbpartition by hash(`seller_id`)  tbpartitions 2\n"
                + ") ENGINE=InnoDB DEFAULT CHARSET=utf8 dbpartition by hash(`order_id`) tbpartition by hash(`order_id`) tbpartitions 2;\n",
            primaryTable, indexTable);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql,
            "Global (clustered) secondary index should not specify tbpartition without dbpartition.");

        dropTableWithGsi(primaryTable, ImmutableList.of());
    }

    @Test
    public void testCreateWithoutPartition1() {
        final String primaryTable = "t_idx_order";
        final String indexTable = "g_i_idx_seller";

        dropTableWithGsi(primaryTable, ImmutableList.of());
        String sql = String.format("CREATE TABLE `%s` (\n"
                + "  `id` bigint(11) NOT NULL AUTO_INCREMENT,\n"
                + "  `x` int,\n"
                + "  `order_id` varchar(20) DEFAULT NULL,\n"
                + "  `seller_id` varchar(20) DEFAULT NULL,\n"
                + "  PRIMARY KEY (`id`),\n"
                + "  KEY `xxx` (`seller_id`, `x`)\n"
                + ") ENGINE=InnoDB DEFAULT CHARSET=utf8 dbpartition by hash(`order_id`) tbpartition by hash(`order_id`) tbpartitions 2;\n",
            primaryTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format(
            "CREATE GLOBAL INDEX `%s` on `%s`(`seller_id`) covering (`order_id`,`x`);",
            indexTable, primaryTable);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "global (clustered) secondary index must have dbpartition");

        sql = String.format(
            "alter table `%s` add GLOBAL INDEX `%s` (`seller_id`) covering (`order_id`,`x`);",
            primaryTable, indexTable);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "global (clustered) secondary index must have dbpartition");

        sql = String.format(
            "CREATE UNIQUE GLOBAL INDEX `%s` on `%s`(`seller_id`) covering (`order_id`,`x`);",
            indexTable, primaryTable);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "global (clustered) secondary index must have dbpartition");

        sql = String.format(
            "alter table `%s` add UNIQUE GLOBAL INDEX `%s` (`seller_id`) covering (`order_id`,`x`);",
            primaryTable, indexTable);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "global (clustered) secondary index must have dbpartition");

        sql = String.format(
            "CREATE GLOBAL INDEX `%s` on `%s`(`seller_id`) covering (`order_id`,`x`) tbpartition by hash(`seller_id`)  tbpartitions 2;",
            indexTable, primaryTable);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql,
            "Global (clustered) secondary index should not specify tbpartition without dbpartition.");

        sql = String.format(
            "alter table `%s` add GLOBAL INDEX `%s` (`seller_id`) covering (`order_id`,`x`) tbpartition by hash(`seller_id`)  tbpartitions 2;",
            primaryTable, indexTable);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql,
            "Global (clustered) secondary index should not specify tbpartition without dbpartition.");

        sql = String.format(
            "CREATE UNIQUE GLOBAL INDEX `%s` on `%s`(`seller_id`) covering (`order_id`,`x`) tbpartition by hash(`seller_id`)  tbpartitions 2;",
            indexTable, primaryTable);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql,
            "Global (clustered) secondary index should not specify tbpartition without dbpartition.");

        sql = String.format(
            "alter table `%s` add UNIQUE GLOBAL INDEX `%s` (`seller_id`) covering (`order_id`,`x`) tbpartition by hash(`seller_id`)  tbpartitions 2;",
            primaryTable, indexTable);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql,
            "Global (clustered) secondary index should not specify tbpartition without dbpartition.");

        dropTableWithGsi(primaryTable, ImmutableList.of());
    }

    @Test
    public void testCreateWithMultiAlter() {
        final String primaryTable = "t_idx_order";
        final String indexTable = "g_i_idx_seller";

        dropTableWithGsi(primaryTable, ImmutableList.of());
        String sql = String.format("CREATE TABLE `%s` (\n"
                + "  `id` bigint(11) NOT NULL AUTO_INCREMENT,\n"
                + "  `x` int,\n"
                + "  `order_id` varchar(20) DEFAULT NULL,\n"
                + "  `seller_id` varchar(20) DEFAULT NULL,\n"
                + "  PRIMARY KEY (`id`)\n"
                + ") ENGINE=InnoDB DEFAULT CHARSET=utf8 dbpartition by hash(`order_id`) tbpartition by hash(`order_id`) tbpartitions 2;\n",
            primaryTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format(
            "alter table `%s`\n"
                + "  add GLOBAL INDEX `%s`(`seller_id`) covering (`seller_id`,`x`) dbpartition by hash(`seller_id`),\n"
                + "  add column c2 int,\n"
                + "  add column c3 int;",
            primaryTable, indexTable);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql,
            "do not support mix add global index with other alter statements");

        sql = String.format(
            "alter table `%s`\n"
                + "  add GLOBAL INDEX `%s`(`seller_id`) covering (`seller_id`,`x`) dbpartition by hash(`seller_id`),\n"
                + "  drop column c2,\n"
                + "  add column c3 int;",
            primaryTable, indexTable);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql,
            "do not support mix add global index with other alter statements");

        sql = String.format(
            "alter table `%s`\n"
                + "  add column c1 int,\n"
                + "  add GLOBAL INDEX `%s`(`seller_id`) covering (`seller_id`,`x`) dbpartition by hash(`seller_id`),\n"
                + "  add column c2 int,\n"
                + "  add column c3 int;",
            primaryTable, indexTable);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql,
            "do not support mix add global index with other alter statements");

        dropTableWithGsi(primaryTable, ImmutableList.of());
    }

    @Test
    public void testCreateGsiNameTooLong() {
        final String table = "t_primary_order";
        final String gsi = "finally_one_column_with_the_vary_length_of_65_aaaaaaaaaaaaaaaaaaa";
        final String dropTable = MessageFormat.format("DROP TABLE IF EXISTS `{0}`;", table);
        final String createTable = MessageFormat.format("CREATE TABLE `{0}` (\n"
                + "\t`pk` bigint(11) NOT NULL,\n"
                + "\t`c1` varchar(32) DEFAULT NULL,\n"
                + "\tPRIMARY KEY (`pk`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8 dbpartition by hash(`pk`)",
            table);

        dropTableWithGsi(table, ImmutableList.of());

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        // 创建GSI, 不指定长度且超长/指定长度且超长/指定字符集且超长, 报错
        JdbcUtil.executeUpdateFailed(tddlConnection,
            "CREATE GLOBAL INDEX "
                + gsi + " ON " + table + " (c1) dbpartition by hash(c1)",
            "too long (max = 64)");
        JdbcUtil.executeUpdateFailed(tddlConnection,
            "CREATE UNIQUE GLOBAL INDEX "
                + gsi + " ON " + table + " (c1) dbpartition by hash(c1)",
            "too long (max = 64)");

        JdbcUtil.executeUpdateFailed(tddlConnection,
            "ALTER TABLE " + table + " ADD GLOBAL INDEX "
                + gsi + " (c1) dbpartition by hash(c1)",
            "too long (max = 64)");
        JdbcUtil.executeUpdateFailed(tddlConnection,
            "ALTER TABLE " + table + " ADD UNIQUE GLOBAL INDEX "
                + gsi + " (c1) dbpartition by hash(c1)",
            "too long (max = 64)");

        JdbcUtil.executeUpdateSuccess(tddlConnection, dropTable);
    }

    public void testCreateGsiDoubleDrop0() {
        final String primaryTable = "t_idx_order_dp0";
        final String indexTable = "g_i_idx_seller_dp0";

        dropTableWithGsi(primaryTable, ImmutableList.of());

        String sql = String.format("CREATE TABLE `%s` (\n"
                + "  `id` bigint(11) NOT NULL AUTO_INCREMENT,\n"
                + "  `x` int,\n"
                + "  `order_id` varchar(20) DEFAULT NULL,\n"
                + "  `seller_id` varchar(20) DEFAULT NULL,\n"
                + "  PRIMARY KEY (`id`)\n"
                + ") ENGINE=InnoDB DEFAULT CHARSET=utf8 dbpartition by hash(`order_id`) tbpartition by hash(`order_id`) tbpartitions 2;\n",
            primaryTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format(
            "CREATE GLOBAL INDEX `%s` on `%s`(`seller_id`) dbpartition by hash(`seller_id`) tbpartition by hash(`seller_id`)  tbpartitions 2;",
            indexTable, primaryTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        // Double drop.
        sql = String.format(
            "/*+TDDL: cmd_extra(ENABLE_ASYNC_DDL=true, PURE_ASYNC_DDL_MODE=true)*/\n"
                + "drop index `%s` on `%s`;",
            indexTable, primaryTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        // Wait tail task all finished.
        final long start = System.currentTimeMillis();
        List<Map<String, String>> full_ddl = null;
        while (System.currentTimeMillis() - start < 20_000) {
            full_ddl = showFullDDL();
            if (full_ddl.isEmpty()) {
                break;
            }
        }
        Assert.assertTrue(full_ddl != null && full_ddl.isEmpty());

        // Assert that global index dropped.
        sql = String.format("show global index from `%s`", primaryTable);
        final List<List<Object>> result = JdbcUtil.getAllResult(JdbcUtil.executeQuery(sql, tddlConnection));
        Assert.assertTrue(result.isEmpty());

        dropTableWithGsi(primaryTable, ImmutableList.of());
    }

    @Test
    @Ignore("New DDL engine solve this problem")
    public void testCreateGsiDoubleDrop1() {
        final String primaryTable = "t_idx_order_dp1";
        final String indexTable = "g_i_idx_seller_dp1";

        dropTableWithGsi(primaryTable, ImmutableList.of());

        String sql = String.format("CREATE TABLE `%s` (\n"
                + "  `id` bigint(11) NOT NULL AUTO_INCREMENT,\n"
                + "  `x` int,\n"
                + "  `order_id` varchar(20) DEFAULT NULL,\n"
                + "  `seller_id` varchar(20) DEFAULT NULL,\n"
                + "  PRIMARY KEY (`id`)\n"
                + ") ENGINE=InnoDB DEFAULT CHARSET=utf8 dbpartition by hash(`order_id`) tbpartition by hash(`order_id`) tbpartitions 2;\n",
            primaryTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format(
            "CREATE GLOBAL INDEX `%s` on `%s`(`seller_id`) dbpartition by hash(`seller_id`) tbpartition by hash(`seller_id`)  tbpartitions 2;",
            indexTable, primaryTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        // Double drop.
        sql = String.format(
            "/*+TDDL: cmd_extra(FORCE_DDL_ON_LEGACY_ENGINE=TRUE, ENABLE_ASYNC_DDL=true, PURE_ASYNC_DDL_MODE=true)*/\n"
                + "alter table `%s` drop index `%s`;",
            primaryTable, indexTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        // Wait tail task all finished.
        final long start = System.currentTimeMillis();
        List<Map<String, String>> full_ddl = null;
        while (System.currentTimeMillis() - start < 20_000) {
            full_ddl = showFullDDL();
            if (full_ddl.isEmpty()) {
                break;
            }
        }
//        Assert.assertTrue(full_ddl != null && full_ddl.isEmpty());

        // Assert that global index dropped.
        sql = String.format("show global index from `%s`", primaryTable);
        final List<List<Object>> result = JdbcUtil.getAllResult(JdbcUtil.executeQuery(sql, tddlConnection));
        Assert.assertTrue(result.isEmpty());

        dropTableWithGsi(primaryTable, ImmutableList.of());
    }
}

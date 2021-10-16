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

package com.alibaba.polardbx.druid.bvt.sql.mysql;

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLLimit;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import junit.framework.TestCase;
import org.junit.Assert;

import java.util.Arrays;
import java.util.List;

public class SQLUtilsTest extends TestCase {

    public void test_toString() throws Exception {
        SQLCharExpr cai = new SQLCharExpr("cai");
        Assert.assertEquals("'cai'", cai.toString());

        String s = SQLUtils.toNormalizeMysqlString(cai);
        Assert.assertEquals("cai",s);
    }

    public void test_format() throws Exception {
        String formattedSql = SQLUtils.format("select * from t where id = ?", JdbcConstants.MYSQL,
                                              Arrays.<Object> asList("abc"));
        Assert.assertEquals("SELECT *" + //
                            "\nFROM t" + //
                            "\nWHERE id = 'abc'", formattedSql);
    }

    public void test_format_0() throws Exception {
        String sql = "select \"%\"'温'\"%\" FROM dual;";
        SQLUtils.parseStatements(sql, JdbcConstants.MYSQL);
        String formattedSql = SQLUtils.formatMySql(sql);
        Assert.assertEquals("SELECT '%温%'\n" +
                "FROM dual;", formattedSql);
    }

    public void test_format_1() throws Exception {
        String sql = "select * from t where tname LIKE \"%\"'温'\"%\"";
        String formattedSql = SQLUtils.formatMySql(sql);
        Assert.assertEquals("SELECT *\n" +
                "FROM t\n" +
                "WHERE tname LIKE '%温%'", formattedSql);
    }


    public void test_format_3() throws Exception {
        String sql = "select lottery_notice_issue,lottery_notice_date,lottery_notice_result from tb_lottery_notice where lottery_type_id=8 and lottery_notice_issue<=2014066 UNION ALL SELECT NULL, NULL, NULL, NULL, NULL, NULL# and lottery_notice_issue>=2014062 order by lottery_notice_issue desc";
        String formattedSql = SQLUtils.formatMySql(sql);
        String expected = "SELECT lottery_notice_issue, lottery_notice_date, lottery_notice_result\n" +
                "FROM tb_lottery_notice\n" +
                "WHERE lottery_type_id = 8\n" +
                "\tAND lottery_notice_issue <= 2014066\n" +
                "UNION ALL\n" +
                "SELECT NULL, NULL, NULL, NULL, NULL\n" +
                "\t, NULL# and lottery_notice_issue>=2014062 order by lottery_notice_issue desc";
        Assert.assertEquals(expected, formattedSql);
    }
    public void test_4() throws Exception {
        String sql = "/*+ engine=MPP */ SELECT ceiling(DECIMAL '123456789012345678');";
        List<SQLStatement> sqlStatements = SQLUtils.parseStatements(sql, DbType.mysql);
        SQLStatement statement = sqlStatements.get(0);
        Assert.assertEquals("/*+ engine=MPP */\n" + "SELECT ceiling(DECIMAL '123456789012345678');", statement.toString());
    }

    public void test_toExpr() throws Exception {
        String owner = "CS_TAOBAO_XETL_DEV.18415F5F_20160712_KMD_TDI_0";
        SQLExpr expr = SQLUtils.toSQLExpr(owner);
    }

    public void test_toExpr2() throws Exception {
        String owner = "CASE table_type WHEN 'VIEW' THEN 'VIEW' WHEN 'BASE TABLE' THEN 'BASE TABLE' ELSE 'BASE TABLE' END";
        SQLExpr expr = SQLUtils.toSQLExpr(owner);
        System.out.println(expr);
    }

    public void test_toExpr3() throws Exception {
        String owner = "-100000000037";
        SQLExpr expr = SQLUtils.toSQLExpr(owner, DbType.mysql);
        assertEquals("-100000000037", expr.toString());
    }

    public void test_toExpr4() throws Exception {
        String owner = "boolean 'true'";
        SQLExpr expr = SQLUtils.toSQLExpr(owner, DbType.mysql);
        assertEquals("true", expr.toString());
    }

    public void test_toExpr5() throws Exception {
        String owner = "varchar 'true'";
        SQLExpr expr = SQLUtils.toSQLExpr(owner, DbType.mysql);
        assertEquals("'true'", expr.toString());
    }

    public void test_toExpr6() throws Exception {
        String owner = "cast(-infinity() as JSON)";
        SQLExpr expr = SQLUtils.toSQLExpr(owner, DbType.mysql);
        assertEquals("CAST(-infinity() AS JSON)", expr.toString());
    }

    public void test_toExpr7() throws Exception {
        String owner = "cast(TIME '03:04:05.321' as time with time zone)";
        SQLExpr expr = SQLUtils.toSQLExpr(owner, DbType.mysql);
        assertEquals("CAST(TIME '03:04:05.321' AS time WITH TIME ZONE)", expr.toString());
    }

    public void test_toExpr8() throws Exception {
        String owner = "CAST(row(FALSE, ARRAY [1, 2], MAP(ARRAY[1, 3], ARRAY[2.0, 4.0])) AS ROW(col0 boolean , col1 array(integer), col2 map(integer, double))).col1";
        SQLExpr expr = SQLUtils.toSQLExpr(owner, DbType.mysql);
        assertEquals("CAST(row(false, ARRAY[1, 2], MAP(ARRAY[1, 3], ARRAY[2.0, 4.0])) AS ROW(col0 boolean,col1 ARRAY(integer),col2 MAP<integer, double>)).col1", expr.toString());
    }

    public void test_toExpr9() throws Exception {
        String owner = "CAST(NULL AS ROW(UNKNOWN)) IS DISTINCT FROM CAST(NULL AS ROW(UNKNOWN))";
        SQLExpr expr = SQLUtils.toSQLExpr(owner, DbType.mysql);
        assertEquals("CAST(NULL AS ROW(UNKNOWN)) IS DISTINCT FROM CAST(NULL AS ROW(UNKNOWN))", expr.toString());
    }

    public void test_toExpr10() throws Exception {
        String owner = "POSITION('' in '')";
        SQLExpr expr = SQLUtils.toSQLExpr(owner, DbType.mysql);
        assertEquals("LOCATE('', '')", expr.toString());
    }

    public void test_toExpr11() throws Exception {
        String owner = ".5";
        SQLExpr expr = SQLUtils.toSQLExpr(owner, DbType.mysql);
        assertEquals(".5", expr.toString());
    }

    public void test_toExpr12() throws Exception {
        String owner = "VARCHAR 'abc'";
        SQLExpr expr = SQLUtils.toSQLExpr(owner, DbType.mysql);
        assertEquals("'abc'", expr.toString());
    }

    public void test_toExpr13() throws Exception {
        String owner = "(`bound_string` LIKE `bound_pattern`)";
        SQLExpr expr = SQLUtils.toSQLExpr(owner, DbType.mysql);
        assertEquals("`bound_string` LIKE `bound_pattern`", expr.toString());
    }

    public void test_toExpr14() throws Exception {
        String owner = "- - int_test + double_test * 49";
        SQLExpr expr = SQLUtils.toSQLExpr(owner, DbType.mysql);
        assertEquals("-(-int_test) + double_test * 49", expr.toString());
    }

    public void test_toExpr15() throws Exception {
        String owner = "+ + int_test + double_test * 49";
        SQLExpr expr = SQLUtils.toSQLExpr(owner, DbType.mysql);
        assertEquals("+(+int_test) + double_test * 49", expr.toString());
    }

    public void test_toExpr16() throws Exception {
        String owner = "`outline`";
        SQLExpr expr = SQLUtils.toSQLExpr(owner, DbType.mysql);
        assertEquals("`outline`", expr.toString());
    }

    public void test_select_expr() throws Exception {
        String sql = "select REAL'12.34' + REAL'56.78'";
        SQLSelectStatement stmt = (SQLSelectStatement) SQLUtils.parseSingleMysqlStatement(sql);
        SQLExpr expr = stmt.getSelect().getQueryBlock().getSelectList().get(0).getExpr();
        assertEquals("REAL '12.34' + REAL '56.78'", expr.toString());
    }

    public void test_no_format() throws Exception {
        String sql = "alter table t1 add column a varchar ";
        SQLStatement statement = SQLUtils.parseSingleMysqlStatement(sql);
        String format = SQLUtils.toMySqlString(statement, new SQLUtils.FormatOption(true, false));
        assertEquals("ALTER TABLE t1 ADD COLUMN a varchar", format);
    }

    public void test_toString_default() throws Exception {
        String abc = SQLUtils.toMySqlStringIfNotNull(null, "abc");
        assertEquals("abc", abc);
    }

    public void test_normalize() throws Exception {
        String abc = SQLUtils.normalize("` 列 1`", DbType.mysql);
        assertEquals("列 1", abc);
    }

    public void test_normalize_no_trim() throws Exception {
        final String[] test = {
            "` 列 1`",
            "` 列`` 1`",
            "` 列```` 1`",
            "` 列``2`` 1`",
            "` test``table `",
            "` test````table `",
            "\" test````table \"",
            "\" test\"\"table \"",
            "\" test\"\"\"\"table \""
        };
        final String[] res = {
            " 列 1",
            " 列` 1",
            " 列`` 1",
            " 列`2` 1",
            " test`table ",
            " test``table ",
            " test````table ",
            " test\"table ",
            " test\"\"table "
        };
        for (int i = 0; i < test.length; ++i) {
            String abc = SQLUtils.normalizeNoTrim(test[i]);
            assertEquals(res[i], abc);
        }
    }

    public void test_splitInsert() throws Exception {
        String sql = "insert t1 (a, b, c) values('1', '2', '3'), ('2', '2', '3'), ('3', '2', '3'),"
                     + "('4', '2', '3'), ('5', '2', '3');";
        List<SQLInsertStatement> sqlInsertStatements = SQLUtils.splitInsertValues(DbType.mysql, sql, 2);
        assertEquals(3, sqlInsertStatements.size());
        assertEquals("INSERT INTO t1(a, b, c)\n" + "VALUES ('1', '2', '3'), ('2', '2', '3');", sqlInsertStatements.get(0).toString());
        assertEquals("INSERT INTO t1(a, b, c)\n" + "VALUES ('3', '2', '3'), ('4', '2', '3');", sqlInsertStatements.get(1).toString());
        assertEquals("INSERT INTO t1(a, b, c)\n" + "VALUES ('5', '2', '3');", sqlInsertStatements.get(2).toString());
    }

    public void test_splitInsert2() throws Exception {
        String sql = "insert t1 (a, b, c) values('1', '2', '3'), ('2', '2', '3'), ('3', '2', '3'),"
                     + "('4', '2', '3'), ('5', '2', '3');";
        List<SQLInsertStatement> sqlInsertStatements = SQLUtils.splitInsertValues(DbType.mysql, sql, 5);
        assertEquals(1, sqlInsertStatements.size());
        assertEquals("INSERT INTO t1 (a, b, c)\n" + "VALUES ('1', '2', '3'),\n" + "\t('2', '2', '3'),\n"
                     + "\t('3', '2', '3'),\n" + "\t('4', '2', '3'),\n" + "\t('5', '2', '3');", sqlInsertStatements.get(0).toString());
    }

    public void test_splitInsert3() throws Exception {
        String sql = "insert into table1 (col1, col2, col3,col4)  values (1,1,1,1), (2,2,2,2), (3,3,3,3), (4,4,4,4);";
        List<SQLInsertStatement> sqlInsertStatements = SQLUtils.splitInsertValues(DbType.mysql, sql, 2);
        assertEquals(2, sqlInsertStatements.size());
        assertEquals("INSERT INTO table1(col1, col2, col3, col4)\n" + "VALUES (1, 1, 1, 1), (2, 2, 2, 2);", sqlInsertStatements.get(0).toString());
        assertEquals("INSERT INTO table1(col1, col2, col3, col4)\n" + "VALUES (3, 3, 3, 3), (4, 4, 4, 4);", sqlInsertStatements.get(1).toString());
    }

    public void test_limit1() throws Exception {
        String sql = "insert overwrite table stat_meiyan_parquet.meiyan_odz_daily_user\n" +
                "select\n" +
                "        'android',server_id,imei,app_version,lower(sdk_channel) as channel,\n" +
                "        city_id,province_id,country_id,\n" +
                "        '' as device_brand,device_model,resolution,\n" +
                "        case when is_app_new =2  then 'new'\n" +
                "        else 'active' end as user_type\n" +
                "from\n" +
                "        (select\n" +
                "                t.server_id,\n" +
                "                if(current_imei is null and length(trim(imei)) > 10,imei,current_imei) as imei,\n" +
                "                max(app_version) over(partition by t.server_id) as app_version,\n" +
                "                sdk_channel,row_number() over(partition by t.server_id order by time desc) as row_num,\n" +
                "                city_id,province_id,country_id,device_model,resolution,\n" +
                "                case when t1.server_id is not null then 2 else 1 end as is_app_new,\n" +
                "                0 as is_back,\n" +
                "        0 as is_first_launch\n" +
                "        from\n" +
                "        (select * from  bigdataop_parquet.stat_sdk_android\n" +
                "           where date_p = 20181201\n" +
                "                and app_key_p = 'F9CC8787275D8691'\n" +
                "                and app_version is not null\n" +
                "                and server_id is not null\n" +
                "                and event_id = 'app_start'\n" +
                "                and app_version not regexp '[a-zA-Z]'\n" +
                "        )t\n" +
                " left join\n" +
                "  (select server_id ,date_p\n" +
                "    from stat_sdk_parquet.sdk_odz_new_device_info\n" +
                "   where date_p =20181201\n" +
                "     and os_p in ('android')\n" +
                "     and app_key_p in ('F9CC8787275D8691')) t1 on t.server_id=t1.server_id\n" +
                "        )t\n" +
                "where row_num = 1\n" +
                "union all\n" +
                "select\n" +
                "        'ios',d.server_id,idfa,app_version,\n" +
                "        nvl(last_channel,channel) as channel,\n" +
                "        city_id,province_id,country_id,\n" +
                "        '' device_brand,device_model,resolution,\n" +
                "        case when is_app_new = 2 then 'new'\n" +
                "        else 'active' end as user_type\n" +
                "from\n" +
                "        (select\n" +
                "                server_id,idfa,app_version,city_id,province_id,country_id,device_model,resolution, is_app_new\n" +
                "        from\n" +
                "                (select\n" +
                "                        t.server_id,\n" +
                "                        idfa,\n" +
                "                        max(app_version) over(partition by t.server_id) as app_version,\n" +
                "                        row_number() over(partition by t.server_id order by time desc) as row_num,\n" +
                "                        city_id,province_id,country_id,device_model,resolution,\n" +
                "                        case when t1.server_id is not null then 2 else 1 end as is_app_new\n" +
                "                from\n" +
                "                (select * from bigdataop_parquet.stat_sdk_ios\n" +
                "                where date_p = 20181201\n" +
                "                        and app_key_p = 'BDFAFB4ACC7885EE'\n" +
                "                        and app_version is not null\n" +
                "                        and event_id = 'app_start'\n" +
                "                        and app_version not regexp '[a-zA-Z]'\n" +
                "                 ) t\n" +
                "    left join\n" +
                "     (select server_id ,date_p\n" +
                "       from stat_sdk_parquet.sdk_odz_new_device_info\n" +
                "      where date_p =20181201\n" +
                "        and os_p in ('ios')\n" +
                "        and app_key_p in ('BDFAFB4ACC7885EE')) t1 on t.server_id=t1.server_id\n" +
                "                )t\n" +
                "        where row_num = 1\n" +
                "        )d\n" +
                "left join\n" +
                "        (\n" +
                "        select\n" +
                "                server_id,if(channel is null or regexp_replace(channel,' ','') = '','unknown',lower(regexp_replace(channel,' ',''))) as channel\n" +
                "        from\n" +
                "                (\n" +
                "                select\n" +
                "                        server_id,channel,row_number() over(partition by server_id order by time desc) as row_num\n" +
                "                from bigdataop_parquet.sdk_channel_data\n" +
                "                where type_p = 'ios'\n" +
                "                        and app_key = 'BDFAFB4ACC7885EE'\n" +
                "                        and channel is not null\n" +
                "                )t\n" +
                "        where row_num = 1\n" +
                "        )tt on d.server_id = tt.server_id\n" +
                "left join\n" +
                "        (\n" +
                "        select\n" +
                "                server_id,last_channel\n" +
                "        from\n" +
                "                (select\n" +
                "                        server_id,if(channel is null or regexp_replace(channel,' ','') = '','unknown',lower(regexp_replace(channel,' ',''))) as last_channel,\n" +
                "                        row_number() over(partition by server_id order by receive_time desc) as row_num\n" +
                "                from stat_sdk_parquet.sdk_odz_back_visit_data\n" +
                "                where type_p = 'ios'\n" +
                "                        and app_key = 'BDFAFB4ACC7885EE'\n" +
                "                        and channel is not null\n" +
                "                )r\n" +
                "        where row_num = 1\n" +
                "        )back\n" +
                "on d.server_id = back.server_id";

        SQLLimit limit = SQLUtils.getLimit(sql, DbType.mysql);
        Assert.assertNull(limit);
    }

    public void test_limit2() throws Exception {
        String sql = "insert into t1 select * from t2 limit 1,2";

        SQLLimit limit = SQLUtils.getLimit(sql, DbType.mysql);
        assertEquals(limit.getOffset().toString(), "1");
        assertEquals(limit.getRowCount().toString(), "2");
    }

    public void test_float_bug() throws Exception {
        String sql = "db123.1t";

        SQLExpr expr = SQLUtils.toSQLExpr(sql);
        assertEquals("db123.1t",expr.toString());
        assertTrue(expr instanceof SQLPropertyExpr);
    }

    public void test_normalize_trim() throws Exception {
        String sql = "`db123.1t `";

        String normalize = SQLUtils.normalize(sql, false);
        assertEquals("db123.1t ", normalize);

        String normalize2 = SQLUtils.normalize(sql, true);
        assertEquals("db123.1t", normalize2);
    }
}

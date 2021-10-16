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

package com.alibaba.polardbx.parser;

import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.parse.FastSqlParserException;
import com.alibaba.polardbx.optimizer.parse.visitor.ContextParameters;
import com.alibaba.polardbx.optimizer.parse.visitor.FastSqlToCalciteNodeVisitor;
import junit.framework.TestCase;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;

import java.util.List;

public class CalciteReplaceTest extends TestCase {
    public void test_0() throws Exception {
        String sql =
            "/* ApplicationName=DBeaver 5.2.5 - Main */ SELECT * FROM ( SELECT pa.UserID,oi.OrderNo,oi.AttendSchoolID,oi.CourseType,SUM(IF(pa.PaymentType=3,-pa.Amount,pa.Amount)) Amount  FROM gd_edw.person_achievement pa  LEFT JOIN gd_edw.order_info oi ON oi.OrderNo = pa.OrderNo  WHERE pa.amount>0 AND pa.YearMonth>='2019-02-01' AND pa.YearMonth<='2019-02-28 23:59:59' AND oi.CourseType = 1000057  GROUP BY pa.UserID,oi.OrderNo,oi.AttendSchoolID,oi.CourseType ) z_q WHERE z_q.orderno = 'DD2019020251480045'";
        perf(sql);
    }

    public void test_1() throws Exception {
        String sql =
            "/* ApplicationName=DBeaver 5.2.5 - Main */ SELECT  consult.clueguid,first_value(consult.consultchannelid) OVER (PARTITION BY consult.ClueGuid ORDER BY consult.ConsultTime) firstchannelcode2018   FROM gd_edw.cluerepeatconsult consult  LIMIT 100";
        perf(sql);
    }

    public void test_2() throws Exception {
        String sql =
            "select timestampdiff(minute,ac.created_at,ac.updated_at)  from approval_contract_with_landlord_v3 ac";
        perf(sql);
    }

    public void test_3() throws Exception {
        String sql = "select 'abEEE_c' like 'ab%/_c' escape '/'";
        perf(sql);
    }

    public void test_4() throws Exception {
        String sql =
            "select         tag,order_num         from         store_top_tag_1d         where         biz_day = 20190223 and store_id = 1101000111101002101002 order by order_num asc limit 5";
        perf(sql);
    }

    public void test_5() throws Exception {
        String sql =
            "/* ApplicationName=IntelliJ IDEA 2018.3.5 */ select format(cast(o.payprice + od.VoucherPrice * vu.platform_ratio / 100 as numeric),2)  from t_order o         LEFT JOIN t_machine m on m.machineId = o.machineId         LEFT JOIN t_machine_type t on t.MACHINETYPEID = m.PARENTTYPEID         LEFT JOIN t_shop s on s.shopId = o.shopId         LEFT JOIN t_member mem on mem.ID = o.UserId         LEFT JOIN t_order_discounts od on od.OrderNo = o.orderNo         LEFT JOIN voucher_user vu on vu.id = od.VoucherId  where o.orderno = '0120190301172815303173'";
        perf(sql);
    }

    public void test_6() throws Exception {
        String sql =
            "select    count(seqno)   , count(seqno_gz)   , count(accountid)   , count(user_id)   , count(member_id)   , sum(pay_ord_act_cnt_001 )   , sum(pay_ord_act_qty_001 )   , sum(pay_ord_act_amt_001 )   , sum(pay_ord_act_amt_002 )   , sum(pay_ord_act_amt_003 )   , sum(pay_ord_act_amt_005 )   , sum(pay_ord_act_cnt_001_total )   , sum(pay_ord_act_amt_001_total )   , sum(pay_ord_act_cnt_002_total ) from dws_nm_trd_mbr_ord_d_bak_20190226 union all  select    count(seqno)   , count(seqno_gz)   , count(accountid)   , count(user_id)   , count(member_id)   , sum(pay_ord_act_cnt_001 )   , sum(pay_ord_act_qty_001 )   , sum(pay_ord_act_amt_001 )   , sum(pay_ord_act_amt_002 )   , sum(pay_ord_act_amt_003 )   , sum(pay_ord_act_amt_005 )   , sum(pay_ord_act_cnt_001_total )   , sum(pay_ord_act_amt_001_total )   , sum(pay_ord_act_cnt_002_total ) from dws_nm_trd_mbr_ord_d  LIMIT 1000";
        perf(sql);
    }

    public void test_7() throws Exception {
        String sql =
            "(SELECT 2019 AS id, 2019 AS label LIMIT 10000) UNION (SELECT 2018 AS id, 2018 AS label LIMIT 10000) UNION (SELECT 2017 AS id, 2017 AS label LIMIT 10000)";
        perf(sql);
    }

    public void test_8() throws Exception {
        String sql =
            "SELECT 5 as 图6 ,count(DISTINCT item_id) as 数目 FROM tianchi_integrate_feature_table WHERE ((((qx_bc_type='C'))  AND  (qx_category IN ('50006228', '121474019', '121390012', '121406022'))  AND  ( ((qx_ipv_1m_001>=1.0 AND qx_ipv_1m_001<=9.9999999E7)) ))) AND  (qx_biz_name IN ('MERCHANT'))  AND  qx_image_position in (5)";
        perf(sql);
    }

    public void test_9() throws Exception {
        String sql =
            "/*+ engine= mpp*/         with             now_table as (    select     comm_hour,comm_date,     sum(event_access)*sum(event_show15s)/sum(event_response15s)/1000.0 as now_radi,     sum(event_revenue)/100000.0 as now_revenue,     sum(event_show15s)/1000.0 as now_imp,     sum(event_response15s)/1000.0 as eventResponse15s    from (     SELECT      comm_date,      cast(substr(cast(comm_minute as varchar),1,10) as bigint) as comm_hour,      0 as event_access,      SUM(event_revenue) as event_revenue,      SUM(event_show15s) as event_show15s,      SUM(event_response15s) as event_response15s     FROM add_rt_adx_event_minute     WHERE comm_date BETWEEN 20190307 and 20190308     and cast(substr(cast(comm_minute as varchar),1,10) as bigint) BETWEEN 2019030719 AND 2019030818            AND ad_adtype_id = cast(1 as bigint)                  GROUP BY cast(substr(cast(comm_minute as varchar),1,10) as bigint),comm_date     union all     SELECT      comm_date,      cast(substr(cast(comm_minute as varchar),1,10) as bigint) as comm_hour,      SUM(event_access) as event_access,      0 as event_revenue,      0 as event_show15s,      0 as event_response15s     FROM add_rt_adx_access_minute     WHERE comm_date BETWEEN 20190307 and 20190308     and cast(substr(cast(comm_minute as varchar),1,10) as bigint)   BETWEEN 2019030719 AND 2019030818            AND ad_adtype_id  = cast(1 as bigint)                 GROUP BY cast(substr(cast(comm_minute as varchar),1,10) as bigint),comm_date    ) t group by  comm_hour,comm_date         )             select cast(comm_date as VARCHAR) now_date,substr(cast(comm_hour as varchar),9,2) comm_hour, now_radi,now_imp,now_revenue         from now_table         order by now_date,comm_hour         limit 100";
        perf(sql);
    }

    public void test_10() throws Exception {
        String sql =
            "SELECT     acr.amount,     acr.transaction_no as transactionNo,     aai.currency_code as currency    FROM     o_ironforge_account_charge_record acr    LEFT JOIN     o_ironforge_account_account_info aai     ON acr.account_id = aai.account_id    WHERE     acr.date_created >= '2019-03-03 14:00:00'    AND acr.date_created <= '2019-03-04 13:59:59'    AND trim(acr.charge_method) = 'OL'              AND acr.amount >= 100000.00000";
        perf(sql);
    }

    public void test_11() throws Exception {
        String sql =
            "SELECT count(distinct utdid) from (  select utdid from result_auc_user_cluster where instruct_id = 1329 EXCEPT   select utdid from result_auc_user_cluster where instruct_id = 1330 UNION ALL  select utdid from result_auc_user_cluster where instruct_id = 1331 INTERSECT  select utdid from result_auc_user_cluster where instruct_id = 1326 EXCEPT select utdid from result_auc_user_cluster where instruct_id = 1301           ) LIMIT 1000";
        try {
            perf(sql);
        } catch (FastSqlParserException t) {
            //ignore
        }
    }

    private void perf(String sql) {
        SqlParser calciteParser = SqlParser.create(sql);
        SqlNode node = null;

        try {
            node = calciteParser.parseQuery();
        } catch (Exception ex) {
            return;
        }

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        SQLStatement stmt = statementList.get(0);

        FastSqlToCalciteNodeVisitor visitor =
            new FastSqlToCalciteNodeVisitor(new ContextParameters(false), new ExecutionContext());
        stmt.accept(visitor);

        SqlNode node2 = visitor.getSqlNode();

        String nodeSql = node.toString();
        String nodeSql2 = node2.toString();

//        long hash = CalciteFileReplaceTest.hash(nodeSql);
//        long hash2 = CalciteFileReplaceTest.hash(nodeSql2);
//
//        if (hash == hash2) {
//            return;
//        }
//
//        assertEquals(nodeSql.toLowerCase()
//                , nodeSql2.toLowerCase());
    }
}

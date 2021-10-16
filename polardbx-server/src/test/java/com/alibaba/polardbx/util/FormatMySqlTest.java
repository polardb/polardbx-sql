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

package com.alibaba.polardbx.util;

import com.alibaba.druid.sql.SQLUtils;
import org.junit.Test;

/**
 * Created by lee on 6/12/17.
 */
public class FormatMySqlTest {
    @Test
    public void test_0() {
        String sql =
            "select * from (select s.paper_id, s.status, s.create_time, s.report2, s.score, p.title as title,p.type as type, p.duration, p.user_id as teacher_id, u.username as teacher_name, p.score as paper_total_score, p.is_publish_book from student_paper as s left outer join paper as p on s.paper_id=p.paper_id left outer join users as u on p.user_id=u.id where s.uid=130907 and s.status in (1,2) and p.type <> 4 and p.type <> 6 and p.type <> 9 union select s.paper_id, s.status, s.create_time, s.report2, s.score, p.title as title,p.type as type, p.duration, p.user_id as teacher_id, u.username as teacher_name, p.score as paper_total_score, p.is_publish_book from student_paper as s left outer join paper as p on s.paper_id=p.paper_id left outer join users as u on p.user_id=u.id where s.uid=130907 and s.status in (1,2) and p.type = 4 and p.parent_id is null) as a order by a.create_time desc limit 200";
        //String sql="SELECT\n"
        //    + "\t\tIF(C.dsnsrmc is null,'',C.dsnsrmc) AS dsNsrmc,\n"
        //    + "\t\tIF(C.dsnsrsbh is\n"
        //    + "\t\tnull,'',C.dsnsrsbh) AS dsnsrsbh,\n"
        //    + "\t\tIF(C.dsnsrzdjxh is\n"
        //    + "\t\tnull,'',C.dsnsrzdjxh) AS dszdjxh,\n"
        //    + "\t\tA.glqx_bz AS glqxbz,\n"
        //    + "\t\tC.gsnsrsbh,\n"
        //    + "\t\tC.gsnsrzdjxh AS gszdjxh,\n"
        //    + "\t\tCOALESCE(C.gsnsrmc,C.dsnsrmc) nsrmc,\n"
        //    + "\t\tC.nsrztid,\n"
        //    + "\t\tIF(C.HYYH_BZ is null,'',C.HYYH_BZ) AS\n"
        //    + "\t\thyyh_bz,\n"
        //    + "\t\tB.qybdid,\n"
        //    + "\t\tC.shxydm,\n"
        //    + "\t\tC.ssdabh,\n"
        //    + "\t\tA.yhid,\n"
        //    + "\t\tA.yhnsrsfid,\n"
        //    + "\t\tA.sf_dm AS\n"
        //    + "\t\tyhsfdm,\n"
        //    + "\t\tE.sf_mc AS yhsfmc,\n"
        //    + "\t\tC.zjjg_bz zjjgbz,\n"
        //    + "\t\tD.gsnsrmc AS zzNsrmc,\n"
        //    + "\t\tD.nsrztid AS zzNsrztid,\n"
        //    + "\t\tcase when C.TY_BZ='Y' or D.TY_BZ='Y' then 'Y'  else 'N' end as tybz\n"
        //    + "\t\tFROM YH_YHNSRSF A\t\t\n"
        //    + "\t\tjoin DM_YH_YHSF E on A.YHID = '63eb46ab91ca4cc88003a3662963ed56' and A.SF_DM = E.SF_DM\n"
        //    + "\t\tjoin\n"
        //    + "\t\tYH_QYSQYHSF B on A.YHNSRSFID = B.YHNSRSFID\n"
        //    + "\t\tjoin YH_NSRZT C on B.NSRZTID\n"
        //    + "\t\t= C.NSRZTID\n"
        //    + "\t\tjoin YH_NSRZT D on A.NSRZTID = D.NSRZTID\"";
        System.out.println(SQLUtils.formatMySql(sql));
    }
}

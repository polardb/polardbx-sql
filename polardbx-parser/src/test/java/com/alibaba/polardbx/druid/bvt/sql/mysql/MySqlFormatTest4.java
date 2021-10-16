/*
 * Copyright 1999-2017 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.polardbx.druid.bvt.sql.mysql;

import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.visitor.VisitorFeature;
import com.alibaba.polardbx.druid.util.JdbcUtils;
import junit.framework.TestCase;

public class MySqlFormatTest4 extends TestCase {

    public void test_0() throws Exception {
        String sql = "SELECT id,task_id,task_source, housedel_code, del_type, office_address, company_code, brand, " +
                "class1_code, class2_code, class2_name, status, create_time, end_time, creator_ucid, creator_name, " +
                "org_code, prove_id, prove_time, audit_ucid, audit_name, audit_reject_reason, audit_content, " +
                "audit_time, pass_mode, sms_content, sms_time, lianjia_app_content, lianjia_app_time \r\n FROM " +
                "sh_true_house_task \r\n  WHERE office_address = 0 AND status = 0 ORDER     BY id DESC";

        assertEquals("SELECT id, task_id, task_source, housedel_code, del_type\n" +
                "\t, office_address, company_code, brand, class1_code, class2_code\n" +
                "\t, class2_name, status, create_time, end_time, creator_ucid\n" +
                "\t, creator_name, org_code, prove_id, prove_time, audit_ucid\n" +
                "\t, audit_name, audit_reject_reason, audit_content, audit_time, pass_mode\n" +
                "\t, sms_content, sms_time, lianjia_app_content, lianjia_app_time\n" +
                "FROM sh_true_house_task\n" +
                "WHERE office_address = 0\n" +
                "\tAND status = 0\n" +
                "ORDER BY id DESC", SQLUtils.format(sql, JdbcUtils.MYSQL));

        assertEquals("SELECT id, task_id, task_source, housedel_code, del_type , office_address, company_code, brand, class1_code, class2_code , class2_name, status, create_time, end_time, creator_ucid , creator_name, org_code, prove_id, prove_time, audit_ucid , audit_name, audit_reject_reason, audit_content, audit_time, pass_mode , sms_content, sms_time, lianjia_app_content, lianjia_app_time FROM sh_true_house_task WHERE office_address = 0 AND status = 0 ORDER BY id DESC", SQLUtils.format(sql, JdbcUtils.MYSQL, new SQLUtils.FormatOption(VisitorFeature.OutputUCase)));
    }
}

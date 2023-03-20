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

package com.alibaba.polardbx.qatest.module;

import com.alibaba.polardbx.gms.module.Module;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.BaseTestCase;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Locale;

import static com.alibaba.polardbx.druid.util.StringUtils.isEmpty;

/**
 * @author fangwu
 */
public class ModuleTest extends BaseTestCase {

    @Test
    public void testModuleEvent() {
        String sql = "select * from information_Schema.module";
        try (Connection c = this.getPolardbxConnection()) {
            ResultSet resultSet = c.createStatement().executeQuery(sql);
            resultSet.next();
            assertRow(resultSet);
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    private void assertRow(ResultSet resultSet) throws SQLException {
        String moduleName = resultSet.getString("MODULE_NAME");
        switch (moduleName) {
        case "SPM":
            String args = resultSet.getString("STATS").toLowerCase(Locale.ROOT);
            Assert.assertTrue(args.contains("enable_spm:true"));
            Assert.assertTrue(args.contains("enable_spm_background_task:true"));
            Assert.assertTrue(args.contains("enable_spm_evolution_by_time:false"));
            Assert.assertTrue(args.contains("spm_enable_pqo:false"));
            args = resultSet.getString("SCHEDULE_JOBS").toLowerCase(Locale.ROOT);
            Assert.assertTrue(isEmpty(args) || args.contains("baseline_sync,enabled,every hour"));
            args = resultSet.getString("VIEWS").toLowerCase(Locale.ROOT);
            Assert.assertTrue(args.contains("spm"));
            Assert.assertTrue(args.contains("plan_cache"));
            Assert.assertTrue(args.contains("plan_cache_capacity"));
            break;
        case "STATISTICS":
            args = resultSet.getString("STATS").toLowerCase(Locale.ROOT);
            Assert.assertTrue(args.contains("background_statistic_collection_expire_time:604800"));
            Assert.assertTrue(args.contains("enable_background_statistic_collection:true"));
            Assert.assertTrue(args.contains("enable_statistic_feedback:true"));
            Assert.assertTrue(args.contains("histogram_bucket_size:64"));
            Assert.assertTrue(args.contains("sample_percentage:-1.0"));
            Assert.assertTrue(args.contains("statistic_visit_dn_timeout:60000"));
            args = resultSet.getString("SCHEDULE_JOBS").toLowerCase(Locale.ROOT);
            Assert.assertTrue(isEmpty(args) || args.contains("statistic_sample_sketch,enabled,at 00:00 every 7 days"));
            Assert.assertTrue(isEmpty(args) || args.contains("statistic_rowcount_collection,enabled,every hour"));
            args = resultSet.getString("VIEWS").toLowerCase(Locale.ROOT);
            Assert.assertTrue(args.contains("virtual_statistic"));
            Assert.assertTrue(args.contains("statistics"));
            Assert.assertTrue(args.contains("column_statistics"));
            break;
        }
    }
}

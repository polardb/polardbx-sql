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

package com.alibaba.polardbx.qatest.ddl.sharding.gsi.group3;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * @version 1.0
 */
public class GsiRollbackTest extends DDLBaseNewDBTestCase {

    private static final String ROLLBACK_TABLE_NAME = "gsi_rollback_test";
    private static final String ROLLBACK_INDEX_NAME = "g_i_rollback_test";

    private static final String BAD_TABLE = "CREATE TABLE `" + ROLLBACK_TABLE_NAME + "` (\n"
        + "        `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '卡劵id',\n"
        + "        `gmt_create` datetime NOT NULL,\n"
        + "        `gmt_modified` datetime NOT NULL,\n"
        + "        `resource_id` bigint(20) UNSIGNED DEFAULT NULL COMMENT '对应的资源id',\n"
        + "        `supplier_id` bigint(20) UNSIGNED DEFAULT NULL COMMENT '供应商id',\n"
        + "        `batch_stock_id` bigint(20) UNSIGNED NOT NULL COMMENT '劵所属批次在批次表的id',\n"
        + "        `batch_id` varchar(100) NOT NULL COMMENT '批次id（活动id）',\n"
        + "        `goods_id` bigint(20) UNSIGNED DEFAULT NULL COMMENT '对应的商品id',\n"
        + "        `coupon_status` int(11) NOT NULL,\n"
        + "        `recon_status` int(11) DEFAULT '0' COMMENT '对账结果状态',\n"
        + "        `sales_activity_id` int(10) UNSIGNED DEFAULT NULL COMMENT '销售活动id-默认正常销售1',\n"
        + "        `order_id` bigint(20) UNSIGNED DEFAULT NULL COMMENT '订单id',\n"
        + "        `order_seq_num` int(10) UNSIGNED DEFAULT NULL COMMENT '每个订单中购买的序号',\n"
        + "        `coupon_code` varchar(1000) DEFAULT NULL,\n"
        + "        `coupon_passwd` varchar(100) DEFAULT NULL,\n"
        + "        `ext` text COMMENT '扩展字段',\n"
        + "        `gmt_inactive` datetime DEFAULT NULL COMMENT '完成入库的时间（未激活）',\n"
        + "        `gmt_active` datetime DEFAULT NULL COMMENT '完成激活的时间',\n"
        + "        `gmt_saleable` datetime DEFAULT NULL COMMENT '已出库（可以销售）的时间',\n"
        + "        `gmt_sold_out` datetime DEFAULT NULL COMMENT '售出的时间',\n"
        + "        `gmt_converted` datetime DEFAULT NULL COMMENT '在商家那已兑换的时间',\n"
        + "        `gmt_used` datetime DEFAULT NULL COMMENT '在商家使用的时间',\n"
        + "        `output_batch_id` bigint(20) UNSIGNED DEFAULT NULL,\n"
        + "        `distributor_id` bigint(20) UNSIGNED DEFAULT NULL,\n"
        + "        PRIMARY KEY (`id`),\n"
        + "        KEY `goods_id_status_index` USING BTREE (`goods_id`, `coupon_status`),\n"
        + "        KEY `idx_order_id` USING BTREE (`order_id`),\n"
        + "        KEY `idx_output_batch` USING BTREE (`output_batch_id`),\n"
        + "        KEY `idx_coupon_code` USING BTREE (`coupon_code`(45)),\n"
        + "        KEY `batch_id_index` USING BTREE (`batch_id`),\n"
        + "        KEY `batch_id_status_index` USING BTREE (`batch_id`, `coupon_status`),\n"
        + "        KEY `batch_stock_id_index` USING BTREE (`batch_stock_id`),\n"
        + "        KEY `resource_id_index` USING BTREE (`resource_id`)\n"
        + ") ENGINE = InnoDB AUTO_INCREMENT = 668800049 DEFAULT CHARSET = utf8mb4 ROW_FORMAT = DYNAMIC  dbpartition by hash(`id`)";
    private static final String BAD_GSI =
        "CREATE UNIQUE GLOBAL INDEX `" + ROLLBACK_INDEX_NAME + "` ON `" + ROLLBACK_TABLE_NAME
            + "`(`coupon_passwd`(101))\n"
            + "    COVERING(`resource_id`, `batch_stock_id`, `supplier_id`, `coupon_status`, `order_id`)\n"
            + "    dbpartition by hash(`coupon_passwd`) tbpartition by hash(`coupon_passwd`) tbpartitions 3";

    @Before
    public void before() {
        dropTableWithGsi(ROLLBACK_TABLE_NAME, ImmutableList.of(ROLLBACK_INDEX_NAME));
    }

    @After
    public void after() {
        dropTableWithGsi(ROLLBACK_TABLE_NAME, ImmutableList.of(ROLLBACK_INDEX_NAME));
    }

    // 测试所有gsi表都创建失败，处于creating状态下的回滚
    @Test
    public void testAllGsiFail() throws Exception {
        JdbcUtil.executeUpdateSuccess(tddlConnection, BAD_TABLE);

        JdbcUtil.executeUpdateFailed(tddlConnection, BAD_GSI, "Incorrect prefix key");

        // Get the job.
        long startTime = System.currentTimeMillis();
        String jobId = null;
        while (System.currentTimeMillis() - startTime < 10000) {
            List<Map<String, String>> fullDDL = showFullDDL();
            // Assert running.
            Optional<Map<String, String>> jobOp = fullDDL.stream()
                .filter(m -> m.get("OBJECT_NAME").equalsIgnoreCase(ROLLBACK_TABLE_NAME))
                .findFirst();
            if (jobOp.isPresent() && jobOp.get().get("STATE").equals("PENDING")) {
                jobId = jobOp.get().get("JOB_ID");
                break;
            }

            // Sleep for a while.
            try {
                TimeUnit.MILLISECONDS.sleep(100);
            } catch (InterruptedException e) {
                // ignore exception
            }
        }

        //new engine will auto rollback, so if jobId is null, just skip calling rollback
        if (jobId != null) {
            Assert.assertNotNull("Job not found.", jobId);
            try (ResultSet rs = JdbcUtil.executeQuery("rollback ddl " + jobId, tddlConnection)) {
                List<List<String>> res = JdbcUtil.getAllStringResult(rs, false, null);
                Assert.assertEquals(1, res.size());
                Assert.assertEquals(3, res.get(0).size());
                Assert.assertEquals("SUCCESS", res.get(0).get(1));
                Assert.assertEquals("OK", res.get(0).get(2));
            }
        }

        try (ResultSet rs = JdbcUtil.executeQuery("show global index", tddlConnection)) {
            List<List<String>> res = JdbcUtil.getAllStringResult(rs, false, null);
            Assert.assertFalse("Bad rollback.",
                res.stream().anyMatch(line -> line.get(1).equalsIgnoreCase(ROLLBACK_TABLE_NAME)));
        }
    }

}

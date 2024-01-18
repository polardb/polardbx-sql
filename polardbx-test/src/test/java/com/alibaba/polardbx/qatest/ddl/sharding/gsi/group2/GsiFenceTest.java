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

package com.alibaba.polardbx.qatest.ddl.sharding.gsi.group2;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.apache.commons.lang3.StringUtils;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * @version 1.0
 */
public class GsiFenceTest extends DDLBaseNewDBTestCase {

    private void cleanPending(String obj) {
        List<Map<String, String>> fullDDL = showFullDDL();
        Optional<Map<String, String>> jobOp = fullDDL.stream()
            .filter(m -> m.get("OBJECT_NAME").equals(obj))
            .findFirst();
        if (!jobOp.isPresent()) {
            return;
        }
        if (StringUtils.equalsIgnoreCase(jobOp.get().get("ENGINE"), "DAG")) {
            jobOp.ifPresent(stringStringMap -> JdbcUtil
                .executeUpdateSuccess(tddlConnection, "rollback ddl " + stringStringMap.get("JOB_ID")));
        } else {
            jobOp.ifPresent(stringStringMap -> JdbcUtil
                .executeUpdateSuccess(tddlConnection, "rollback ddl " + stringStringMap.get("JOB_ID")));
        }

    }

    @Test
    @Ignore("新DDL引擎会自动回滚")
    public void testFenceOnDropCreating() {
        final String primaryTable = "gsi_fence";
        final String indexTable = "g_i_fence";

        final String createTable = "CREATE TABLE " + quoteSpecialName(primaryTable) + " (\n"
            + "\t`id` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "\t`order_id` varchar(20) DEFAULT NULL,\n"
            + "\t`buyer_id` varchar(20) DEFAULT NULL,\n"
            + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
            + "\t`order_snapshot` longtext,\n"
            + "\t`order_detail` longtext,\n"
            + "\tPRIMARY KEY (`id`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8  dbpartition by hash(`order_id`)";

        cleanPending(primaryTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "drop table if exists " + quoteSpecialName(primaryTable));
        JdbcUtil.executeUpdateSuccess(tddlConnection, "drop table if exists " + quoteSpecialName(indexTable));

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "insert into " + quoteSpecialName(primaryTable)
            + "(order_id, buyer_id) values(\"d\", \"d\"),(\"e\",\"d\");");

        JdbcUtil.executeUpdateFailed(tddlConnection,
            "create unique global index " + quoteSpecialName(indexTable) + " on " + quoteSpecialName(primaryTable)
                + "(buyer_id) dbpartition by hash(buyer_id);", "ERR_GLOBAL_SECONDARY_INDEX_BACKFILL_DUPLICATE_ENTRY");

        // Fence.
        JdbcUtil.executeUpdateFailed(tddlConnection,
            "drop index " + quoteSpecialName(indexTable) + " on " + quoteSpecialName(primaryTable) + ";",
            "ERR_PAUSED_DDL_JOB_EXISTS", "doesn't exist");

        // Clean up.
        try {
            cleanPending(primaryTable);
        } catch (Throwable t) {
            if (StringUtils.containsIgnoreCase(t.getMessage(), "The DDL job has been cancelled or interrupted")) {
                //ignore
            } else {
                throw t;
            }
        }
        JdbcUtil.executeUpdateSuccess(tddlConnection, "drop table if exists " + quoteSpecialName(primaryTable));
        JdbcUtil.executeUpdateSuccess(tddlConnection, "drop table if exists " + quoteSpecialName(indexTable));
    }

}

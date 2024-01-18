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

package com.alibaba.polardbx.gms.metadb.cdc;

import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.gms.util.MetaDbLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.compress.utils.Lists;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * @author yudong
 * @since 2023/6/9 15:01
 **/
public class RplStatMetricsAccessor extends AbstractAccessor {
    private static final String RPL_STAT_METRICS_TABLE = "rpl_stat_metrics";
    private static final String SELECT_ALL =
        "select `rpl_stat_metrics`.*, rt.type as task_type, rt.state_machine_id as state_machine_id , rm.channel as channel, rt.id as sub_channel  from `rpl_stat_metrics` ,`rpl_task` as rt ,`rpl_state_machine` as rm  where `rpl_stat_metrics`.task_id = rt.id and rt.state_machine_id = rm.id";

    public List<RplStatMetrics> getAllMetrics() {
        try {
            List<RplStatMetrics> result = MetaDbUtil.query(SELECT_ALL,
                RplStatMetrics.class, connection);
            return result;
        } catch (Exception e) {
            MetaDbLogUtil.META_DB_LOG.error("Failed to query the system table '" + RPL_STAT_METRICS_TABLE + "'", e);
            return Lists.newArrayList();
        }
    }

}

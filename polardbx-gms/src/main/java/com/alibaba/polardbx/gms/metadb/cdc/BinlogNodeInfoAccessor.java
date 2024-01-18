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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.gms.util.MetaDbLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * @author yudong
 * @since 2023/3/1 10:04
 **/
@Slf4j
public class BinlogNodeInfoAccessor extends AbstractAccessor {
    private static final String CDC_NODE_INFO_TABLE = "binlog_node_info";
    private static final String SELECT_DAEMON_MASTER_TARGET =
        "select ip, daemon_port from `" + CDC_NODE_INFO_TABLE +
            "` where `cluster_type` = 'BINLOG' and `role` = 'M' ";
    private static final String SELECT_DAEMON_MASTER_TARGET_WITH_INST_ID =
        "select ip, daemon_port from `" + CDC_NODE_INFO_TABLE +
            "` where `cluster_type` = 'BINLOG' and `role` = 'M' and `polarx_inst_id` = ? ";
    private static final String SELECT_DAEMON_MASTER_TARGET_WITH_GROUP =
        "select ip, daemon_port from `" + CDC_NODE_INFO_TABLE
            + "` where `group_name` = ? and `role` = 'M' ";
    private static final String SELECT_REPLICA_DAEMON_MASTER_TARGET =
        "select ip, daemon_port from `" + CDC_NODE_INFO_TABLE
            + "` where `cluster_type` = 'REPLICA' and `role` = 'M' ";

    public Optional<BinlogNodeInfoRecord> getDaemonMaster() {
        try {
            List<BinlogNodeInfoRecord> result = null;
            boolean hasPolarxInstId = MetaDbUtil.hasColumn(CDC_NODE_INFO_TABLE, "polarx_inst_id");
            if (hasPolarxInstId) {
                Map<Integer, ParameterContext> params = new HashMap<>();
                MetaDbUtil.setParameter(1, params, ParameterMethod.setString, InstIdUtil.getInstId());
                result = MetaDbUtil.query(SELECT_DAEMON_MASTER_TARGET_WITH_INST_ID, params,
                    BinlogNodeInfoRecord.class, connection);
            }

            if (!hasPolarxInstId || CollectionUtils.isEmpty(result)) {
                log.warn("failed to get daemon master with instId, try to get without instId");
                result = MetaDbUtil.query(SELECT_DAEMON_MASTER_TARGET, BinlogNodeInfoRecord.class, connection);
            }
            return CollectionUtils.isEmpty(result) ? Optional.empty() : Optional.of(result.get(0));
        } catch (Exception e) {
            MetaDbLogUtil.META_DB_LOG.error("Failed to query the system table " + CDC_NODE_INFO_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                CDC_NODE_INFO_TABLE, e.getMessage());
        }
    }

    public Optional<BinlogNodeInfoRecord> getDaemonMaster(String groupName) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(1);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, groupName);
            List<BinlogNodeInfoRecord> result =
                MetaDbUtil.query(SELECT_DAEMON_MASTER_TARGET_WITH_GROUP, params, BinlogNodeInfoRecord.class,
                    connection);
            return CollectionUtils.isEmpty(result) ? Optional.empty() : result.stream().findFirst();
        } catch (Exception e) {
            MetaDbLogUtil.META_DB_LOG.error("Failed to query the system table " + CDC_NODE_INFO_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                CDC_NODE_INFO_TABLE, e.getMessage());
        }
    }

    public Optional<BinlogNodeInfoRecord> getReplicaDaemonMaster() {
        try {
            List<BinlogNodeInfoRecord> result =
                MetaDbUtil.query(SELECT_REPLICA_DAEMON_MASTER_TARGET, BinlogNodeInfoRecord.class, connection);
            // if no replica cluster, then return main cdc daemon node
            // else return master node of random replica cluster
            if (CollectionUtils.isEmpty(result)) {
                return getDaemonMaster();
            }
            return result.stream().findFirst();
        } catch (Exception e) {
            MetaDbLogUtil.META_DB_LOG.error("Failed to query the system table " + CDC_NODE_INFO_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                CDC_NODE_INFO_TABLE, e.getMessage());
        }
    }

}

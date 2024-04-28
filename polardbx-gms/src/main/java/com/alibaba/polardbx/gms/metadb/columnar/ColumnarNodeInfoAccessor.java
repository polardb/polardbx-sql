package com.alibaba.polardbx.gms.metadb.columnar;

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

@Slf4j
public class ColumnarNodeInfoAccessor extends AbstractAccessor {
    private static final String COLUMNAR_NODE_INFO_TABLE = "columnar_node_info";
    private static final String SELECT_DAEMON_MASTER_TARGET =
        "select ip, daemon_port from `" + COLUMNAR_NODE_INFO_TABLE +
            "` where `cluster_type` = 'COLUMNAR' and `role` = 'M' ";
    private static final String SELECT_DAEMON_MASTER_TARGET_WITH_INST_ID =
        "select ip, daemon_port from `" + COLUMNAR_NODE_INFO_TABLE +
            "` where `cluster_type` = 'COLUMNAR' and `role` = 'M' and `polarx_inst_id` = ? ";

    public Optional<ColumnarNodeInfoRecord> getDaemonMaster() {
        try {
            List<ColumnarNodeInfoRecord> result = null;
            boolean hasPolarxInstId = MetaDbUtil.hasColumn(COLUMNAR_NODE_INFO_TABLE, "polarx_inst_id");
            if (hasPolarxInstId) {
                Map<Integer, ParameterContext> params = new HashMap<>();
                MetaDbUtil.setParameter(1, params, ParameterMethod.setString, InstIdUtil.getInstId());
                result = MetaDbUtil.query(SELECT_DAEMON_MASTER_TARGET_WITH_INST_ID, params,
                    ColumnarNodeInfoRecord.class, connection);
            }

            if (!hasPolarxInstId || CollectionUtils.isEmpty(result)) {
                log.warn("failed to get daemon master with instId, try to get without instId");
                result = MetaDbUtil.query(SELECT_DAEMON_MASTER_TARGET, ColumnarNodeInfoRecord.class, connection);
            }
            return CollectionUtils.isEmpty(result) ? Optional.empty() : Optional.of(result.get(0));
        } catch (Exception e) {
            MetaDbLogUtil.META_DB_LOG.error("Failed to query the system table " + COLUMNAR_NODE_INFO_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                COLUMNAR_NODE_INFO_TABLE, e.getMessage());
        }
    }
}

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

@Slf4j
public class BinlogDumperAccessor extends AbstractAccessor {
    private static final String CDC_DUMPER_INFO_TABLE = "binlog_dumper_info";
    private static final String SELECT_DUMPER_MASTER_TARGET =
        "select ip, port, role from `" + CDC_DUMPER_INFO_TABLE
            + "` where `role` = 'M' and `status` = 0";
    private static final String SELECT_DUMPER_MASTER_TARGET_WITH_INST_ID =
        "select ip, port, role from `" + CDC_DUMPER_INFO_TABLE
            + "` where `role` = 'M' and `status` = 0 and `polarx_inst_id` = ? ";

    public Optional<BinlogDumperRecord> getDumperMaster() {
        try {
            List<BinlogDumperRecord> result = null;
            boolean hasPolarxInstId = MetaDbUtil.hasColumn(CDC_DUMPER_INFO_TABLE, "polarx_inst_id");
            if (hasPolarxInstId) {
                Map<Integer, ParameterContext> params = new HashMap<>();
                MetaDbUtil.setParameter(1, params, ParameterMethod.setString, InstIdUtil.getInstId());
                result = MetaDbUtil.query(SELECT_DUMPER_MASTER_TARGET_WITH_INST_ID, params, BinlogDumperRecord.class,
                    connection);
            }

            if (!hasPolarxInstId || CollectionUtils.isEmpty(result)) {
                log.warn("failed to get dumper master with instId, try to get without instId");
                result = MetaDbUtil.query(SELECT_DUMPER_MASTER_TARGET, BinlogDumperRecord.class, connection);
            }
            return CollectionUtils.isEmpty(result) ? Optional.empty() : Optional.of(result.get(0));
        } catch (Exception e) {
            MetaDbLogUtil.META_DB_LOG.error(
                "Failed to query the system table '" + SELECT_DUMPER_MASTER_TARGET_WITH_INST_ID
                    + "'",
                e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                CDC_DUMPER_INFO_TABLE, e.getMessage());
        }
    }

}

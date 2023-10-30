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
import org.apache.commons.collections.CollectionUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * @author yudong
 * @since 2023/6/9 15:01
 **/
public class BinlogTaskInfoAccessor extends AbstractAccessor {
    private static final String BINLOG_TASK_INFO_TABLE = "binlog_task_info";
    private static final String SELECT_FINAL_TASK_INFO =
        "select ip, port, role, sources_list from `" + BINLOG_TASK_INFO_TABLE
            + "` where `role` = 'Final' and `status` = 0 and `polarx_inst_id` = ? ";

    public Optional<BinlogTaskRecord> getFinalTaskInfo() {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, InstIdUtil.getInstId());
            List<BinlogTaskRecord> result = MetaDbUtil.query(SELECT_FINAL_TASK_INFO, params,
                BinlogTaskRecord.class, connection);
            return CollectionUtils.isEmpty(result) ? Optional.empty() : Optional.of(result.get(0));
        } catch (Exception e) {
            MetaDbLogUtil.META_DB_LOG.error("Failed to query the system table '" + SELECT_FINAL_TASK_INFO + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                BINLOG_TASK_INFO_TABLE, e.getMessage());
        }
    }

}

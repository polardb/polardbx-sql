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

package com.alibaba.polardbx.gms.topology;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.AddressUtils;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.ha.impl.StorageHaManager;
import com.alibaba.polardbx.gms.ha.impl.StorageInstHaContext;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AffinityInfoAccessor extends AbstractAccessor {
    private static final Logger logger = LoggerFactory.getLogger(AffinityInfoAccessor.class);

    private static final String STORAGE_INFO_TABLE = GmsSystemTables.STORAGE_INFO;
    private static final String SERVER_INFO_TABLE = GmsSystemTables.SERVER_INFO;

    private static final String CN_AFFINITY_DN_DETAILS_SQL =
        "select server_info.ip, server_info.port, storage_info.storage_inst_id, CONCAT(storage_info.ip, \":\", storage_info.port) as address "
            + "from " + STORAGE_INFO_TABLE + " storage_info " + " left join " + SERVER_INFO_TABLE + " server_info "
            + " on server_info.ip=storage_info.ip "
            + " where storage_info.storage_master_inst_id = storage_info.storage_inst_id and storage_info.is_vip = 0 and storage_info.inst_kind = 0;";

    public List<AffinityInfoRecord> getAffinityInfoList() {
        try {
            return MetaDbUtil.query(CN_AFFINITY_DN_DETAILS_SQL, null, AffinityInfoRecord.class, connection);
        } catch (Exception e) {
            logger.error("Failed to query affinity info", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                STORAGE_INFO_TABLE + ", " + SERVER_INFO_TABLE,
                e.getMessage());
        }
    }
}

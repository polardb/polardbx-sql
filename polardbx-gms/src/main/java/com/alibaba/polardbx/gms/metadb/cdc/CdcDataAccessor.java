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

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.MetaDbLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class CdcDataAccessor extends AbstractAccessor {
    private static final String CDC_DUMPER_TABLE = "binlog_dumper_info";
    private static final String SELECT_CDC_TARGET =
        "select ip, port, role from `" + CDC_DUMPER_TABLE + "` where `role` = 'M' and `status` = 0";

    private static final String SELECT_CDC_X_TARGET =
        "select ip, port, role from `" + CDC_DUMPER_TABLE
            + "` where `status` = 0 and `role` = ? and `stream_name` = ? ";

    public List<CdcDumperRecord> getAllCdcDumpers() {
        try {
            return MetaDbUtil.query(SELECT_CDC_TARGET, CdcDumperRecord.class, connection);
        } catch (Exception e) {
            MetaDbLogUtil.META_DB_LOG.error("Failed to query the system table '" + SELECT_CDC_TARGET + "'", e);
            return null;
        }
    }

    public List<CdcDumperRecord> getDumper(String role, String streamName) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, role);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, streamName);
            return MetaDbUtil.query(SELECT_CDC_X_TARGET, params, CdcDumperRecord.class, connection);
        } catch (Exception e) {
            MetaDbLogUtil.META_DB_LOG.error("Failed to query the system table '" + SELECT_CDC_TARGET + "'", e);
            return null;
        }
    }
}

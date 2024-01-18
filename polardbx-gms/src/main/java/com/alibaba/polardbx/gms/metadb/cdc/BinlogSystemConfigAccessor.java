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
import com.alibaba.polardbx.gms.util.MetaDbLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.google.common.collect.Maps;

import java.util.Map;

import static com.alibaba.polardbx.gms.metadb.GmsSystemTables.BINLOG_SYSTEM_CONFIG_TABLE;

/**
 * @author yudong
 * @since 2023/6/20 17:12
 **/
public class BinlogSystemConfigAccessor extends AbstractAccessor {
    private final static String INSERT_BINLOG_SYSTEM_CONFIG =
        String.format("replace into %s(`config_key`, `config_value`) values (?, ?)", BINLOG_SYSTEM_CONFIG_TABLE);

    public int insert(String configKey, String configValue) {
        try {
            Map<Integer, ParameterContext> insertParams = Maps.newHashMap();
            MetaDbUtil.setParameter(1, insertParams, ParameterMethod.setString, configKey);
            MetaDbUtil.setParameter(2, insertParams, ParameterMethod.setString, configValue);
            return MetaDbUtil.insert(INSERT_BINLOG_SYSTEM_CONFIG, insertParams, connection);
        } catch (Exception e) {
            MetaDbLogUtil.META_DB_LOG
                .error("Failed to insert the system table '" + BINLOG_SYSTEM_CONFIG_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "insert",
                BINLOG_SYSTEM_CONFIG_TABLE,
                e.getMessage());
        }
    }

}

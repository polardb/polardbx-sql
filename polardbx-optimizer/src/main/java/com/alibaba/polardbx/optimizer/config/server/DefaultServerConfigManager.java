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

package com.alibaba.polardbx.optimizer.config.server;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;

import java.util.List;

/**
 * @author chenghui.lch 2018年5月22日 下午3:22:32
 * @since 5.0.0
 */
public class DefaultServerConfigManager implements IServerConfigManager {

    protected Object ds = null;

    public DefaultServerConfigManager(Object ds) {
        this.ds = ds;
    }

    @Override
    public Object getAndInitDataSourceByDbName(String dbName) {
        return ds;
    }

    @Override
    public Pair<String, String> findGroupByUniqueId(long uniqueId) {
        return null;
    }

    @Override
    public DdlContext restoreDDL(String schemaName, Long jobId) {
        throw new UnsupportedOperationException("Unexpected DDL execution in " + schemaName);
    }

    @Override
    public long submitRebalanceDDL(String schemaName, String sql) {
        throw new UnsupportedOperationException("Unexpected DDL execution in " + schemaName);
    }

    @Override
    public long submitSubDDL(String schemaName, long parentJobId, long parentTaskId, boolean forRollback, String sql) {
        throw new UnsupportedOperationException("Unexpected DDL execution in " + schemaName);
    }

}

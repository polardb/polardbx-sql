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

package com.alibaba.polardbx.gms.metadb.table;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.util.HashMap;
import java.util.Map;

/**
 * @author wenki
 */
public class ColumnarTaskAccessor extends AbstractAccessor {

    private static final String COLUMNAR_TASK_TABLE = wrap(GmsSystemTables.COLUMNAR_TASK);

    private static final String WHERE_BY_CLUSTER_ID_AND_TASK_NAME = " where cluster_id = ? and task_name = ?";

    private static final String UPDATE_HEARTBEAT =
        "update" + COLUMNAR_TASK_TABLE + "set gmt_heartbeat = now()" + WHERE_BY_CLUSTER_ID_AND_TASK_NAME;

    public void updateHeartbeat(String clusterId, String taskName) {
        Map<Integer, ParameterContext> params = new HashMap<>(1);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, clusterId);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setString, taskName);
        update(UPDATE_HEARTBEAT, COLUMNAR_TASK_TABLE, params);
    }
}

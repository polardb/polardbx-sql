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

import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;

import java.util.List;

/**
 * @author wenki
 */
public class ColumnarTaskConfigAccessor extends AbstractAccessor {
    private static final String COLUMNAR_TASK_CONFIG_TABLE = wrap(GmsSystemTables.COLUMNAR_TASK_CONFIG);

    private static final String FROM_TABLE = " from " + COLUMNAR_TASK_CONFIG_TABLE;

    private static final String SELECT_ALL_COLUMNS =
        "select `cluster_id`, `container_id`, `task_name`, `ip`, `port`, `config`, `role`";

    private static final String WHERE_BY_CONTAINER_ID = " where container_id = ?";

    private static final String SELECT_BY_CONTAINER_ID =
        SELECT_ALL_COLUMNS + FROM_TABLE + WHERE_BY_CONTAINER_ID;

    public List<ColumnarTaskConfigRecord> queryByContainerId(String containerId) {
        return query(SELECT_BY_CONTAINER_ID, COLUMNAR_TASK_CONFIG_TABLE, ColumnarTaskConfigRecord.class, containerId);
    }
}

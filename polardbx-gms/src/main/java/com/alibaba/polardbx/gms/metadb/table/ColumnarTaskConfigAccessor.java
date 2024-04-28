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

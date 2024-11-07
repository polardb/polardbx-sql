/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.gms.metadb.table;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.DdlMetaLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import org.apache.commons.collections.CollectionUtils;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ColumnarPartitionEvolutionAccessor extends AbstractAccessor {
    private static final Logger LOGGER = LoggerFactory.getLogger("oss");

    private static final String COLUMNAR_PARTITION_EVOLUTION_TABLE = wrap(GmsSystemTables.COLUMNAR_PARTITION_EVOLUTION);

    private static final String COLUMNAR_TABLE_EVOLUTION_TABLE = wrap(GmsSystemTables.COLUMNAR_TABLE_EVOLUTION);

    private static final String COLUMNAR_TABLE_MAPPING_TABLE = wrap(GmsSystemTables.COLUMNAR_TABLE_MAPPING);

    private static final String INSERT_COLUMNAR_PARTITION_EVOLUTION_RECORDS =
        "insert into " + COLUMNAR_PARTITION_EVOLUTION_TABLE +
            "(`partition_id`, `table_id`, `partition_name`, `status`, `version_id`, `ddl_job_id`, `partition_record`) values (?, ?, ?, ?, ?, ?, ?)";

    private static final String SELECT_ALL_COLUMNS =
        "select `id`, `partition_id`, `table_id`, `partition_name`, `status`, `version_id`, `ddl_job_id`, `partition_record`, `gmt_created`";

    private static final String FROM_TABLE = " from " + COLUMNAR_PARTITION_EVOLUTION_TABLE;

    private static final String ORDER_BY_ID = " order by id asc";

    private static final String WHERE_BY_TABLE_ID = " where `table_id`=?" + ORDER_BY_ID;

    private static final String WHERE_BY_PARTITION_ID = " where `partition_id`=?";

    private static final String WHERE_BY_VERSION_ID = " where `version_id`=?";

    private static final String WHERE_BY_TABLE_ID_VERSION_ID = " where `table_id` = ? and `version_id` = ?";
    private static final String WHERE_BY_TABLE_ID_AND_VERSION_IDS = " where `table_id` = ? and `version_id` in (%s)";

    private static final String AND_PARTITION_ID_ZERO = " and `partition_id` = 0";

    private static final String SELECT_BY_TABLE_ID_VERSION_ID_ORDER_BY_ID =
        SELECT_ALL_COLUMNS + FROM_TABLE + WHERE_BY_TABLE_ID_VERSION_ID + ORDER_BY_ID;

    private static final String SELECT_BY_TABLE_ID_AND_VERSION_IDS_ORDER_BY_ID =
        SELECT_ALL_COLUMNS + FROM_TABLE + WHERE_BY_TABLE_ID_AND_VERSION_IDS + ORDER_BY_ID;

    private static final String WHERE_BY_IDS = " where `id` in (%s)";

    private static final String SELECT_BY_TABLE_ID = SELECT_ALL_COLUMNS + FROM_TABLE + WHERE_BY_TABLE_ID + ORDER_BY_ID;

    private static final String SELECT_BY_IDS = SELECT_ALL_COLUMNS + FROM_TABLE + WHERE_BY_IDS;

    private static final String WHERE_BY_TABLE_ID_AND_STATUS = " where `table_id`=? and `status` = ?" + ORDER_BY_ID;

    private static final String WHERE_VERSION_ID_NOT_IN_FILED_ID_AND_STATUS =
        WHERE_BY_VERSION_ID + " and `table_id`=? and `partition_id` not in ( select `partition_id`" + FROM_TABLE
            + WHERE_BY_TABLE_ID_AND_STATUS + ")";

    private static final String SELECT_BY_TABLE_ID_AND_VERSION_ID_NOT_IN_STATUS =
        SELECT_ALL_COLUMNS + FROM_TABLE + WHERE_VERSION_ID_NOT_IN_FILED_ID_AND_STATUS;

    private static final String UPDATE_FIELD_ID_AS_ID_BY_TABLE_ID_VERSION_ID =
        "update " + COLUMNAR_PARTITION_EVOLUTION_TABLE + " set `partition_id`=`id`" + WHERE_BY_TABLE_ID_VERSION_ID
            + AND_PARTITION_ID_ZERO;

    private static final String DELETE_TABLE_ID = "delete " + FROM_TABLE + WHERE_BY_TABLE_ID;

    public int[] insert(List<ColumnarPartitionEvolutionRecord> records) {
        List<Map<Integer, ParameterContext>> paramsBatch = new ArrayList<>(records.size());
        for (ColumnarPartitionEvolutionRecord record : records) {
            paramsBatch.add(record.buildInsertParams());
        }
        try {
            DdlMetaLogUtil.logSql(INSERT_COLUMNAR_PARTITION_EVOLUTION_RECORDS, paramsBatch);
            return MetaDbUtil.insert(INSERT_COLUMNAR_PARTITION_EVOLUTION_RECORDS, paramsBatch, connection);
        } catch (SQLException e) {
            LOGGER.error("Failed to insert a batch of new records into " + COLUMNAR_PARTITION_EVOLUTION_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "batch insert into",
                COLUMNAR_PARTITION_EVOLUTION_TABLE,
                e.getMessage());
        }
    }

    public List<ColumnarPartitionEvolutionRecord> queryTableIdVersionIdOrderById(Long tableId, Long versionId) {
        return query(SELECT_BY_TABLE_ID_VERSION_ID_ORDER_BY_ID,
            COLUMNAR_PARTITION_EVOLUTION_TABLE,
            ColumnarPartitionEvolutionRecord.class,
            tableId,
            versionId);
    }

    public List<ColumnarPartitionEvolutionRecord> queryIds(List<Long> ids) {
        final Map<Integer, ParameterContext> params =
            MetaDbUtil.buildParameters(ParameterMethod.setLong, ids.toArray());
        return query(String.format(SELECT_BY_IDS, concatParams(ids.size())), COLUMNAR_PARTITION_EVOLUTION_TABLE,
            ColumnarPartitionEvolutionRecord.class, params);
    }

    public List<ColumnarPartitionEvolutionRecord> queryTableIdAndNotInStatus(Long tableId, Long versionId,
                                                                             long status) {
        Map<Integer, ParameterContext> params = new HashMap<>(4);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, versionId);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, tableId);
        MetaDbUtil.setParameter(3, params, ParameterMethod.setLong, tableId);
        MetaDbUtil.setParameter(4, params, ParameterMethod.setString, String.valueOf(status));
        return query(SELECT_BY_TABLE_ID_AND_VERSION_ID_NOT_IN_STATUS, COLUMNAR_PARTITION_EVOLUTION_TABLE,
            ColumnarPartitionEvolutionRecord.class, params);
    }

    /**
     * For columnar use
     *
     * @return list of ColumnarPartitionEvolutionRecord that keeps the order of partitionIdList
     */
    public List<ColumnarPartitionEvolutionRecord> queryIdsWithOrder(List<Long> ids) {
        final List<ColumnarPartitionEvolutionRecord> queryResults = queryIds(ids);

        // Reorder columns
        Map<Long, Integer> pos = new HashMap<>();
        for (int i = 0; i < ids.size(); i++) {
            pos.put(ids.get(i), i);
        }

        ColumnarPartitionEvolutionRecord[] results = new ColumnarPartitionEvolutionRecord[ids.size()];
        for (ColumnarPartitionEvolutionRecord record : queryResults) {
            results[pos.get(record.id)] = record;
        }
        return Arrays.asList(results);
    }

    public List<ColumnarPartitionEvolutionRecord> queryTableIdAndVersionIdsOrderById(Long tableId,
                                                                                     List<Long> versionIds) {
        if (CollectionUtils.isEmpty(versionIds)) {
            return new ArrayList<>();
        }

        List<Long> paramValues = new ArrayList<>();
        paramValues.add(tableId);
        paramValues.addAll(versionIds);

        final Map<Integer, ParameterContext> params =
            MetaDbUtil.buildParameters(ParameterMethod.setLong, paramValues.toArray());
        return query(String.format(SELECT_BY_TABLE_ID_AND_VERSION_IDS_ORDER_BY_ID, concatParams(versionIds.size())),
            COLUMNAR_PARTITION_EVOLUTION_TABLE,
            ColumnarPartitionEvolutionRecord.class,
            params);
    }

    public void updatePartitionIdAsId(long tableId, long versionId) {
        Map<Integer, ParameterContext> params = new HashMap<>(1);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, tableId);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, versionId);
        update(UPDATE_FIELD_ID_AS_ID_BY_TABLE_ID_VERSION_ID, COLUMNAR_PARTITION_EVOLUTION_TABLE, params);
    }

    public int deleteId(long tableId) {
        Map<Integer, ParameterContext> params = new HashMap<>(1);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, tableId);
        return delete(DELETE_TABLE_ID, COLUMNAR_PARTITION_EVOLUTION_TABLE, params);
    }
}

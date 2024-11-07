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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.GeneralUtil;
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

public class ColumnarColumnEvolutionAccessor extends AbstractAccessor {
    private static final Logger LOGGER = LoggerFactory.getLogger("oss");

    private static final String COLUMNAR_COLUMN_EVOLUTION_TABLE = wrap(GmsSystemTables.COLUMNAR_COLUMN_EVOLUTION);

    private static final String COLUMNAR_TABLE_EVOLUTION_TABLE = wrap(GmsSystemTables.COLUMNAR_TABLE_EVOLUTION);

    private static final String COLUMNAR_TABLE_MAPPING_TABLE = wrap(GmsSystemTables.COLUMNAR_TABLE_MAPPING);

    private static final String INSERT_COLUMNAR_COLUMN_EVOLUTION_RECORDS =
        "insert into " + COLUMNAR_COLUMN_EVOLUTION_TABLE +
            "(`field_id`, `table_id`, `column_name`, `version_id`, `ddl_job_id`, `columns_record`) values (?, ?, ?, ?, ?, ?)";

    private static final String SELECT_ALL_COLUMNS =
        "select `id`, `field_id`, `table_id`, `column_name`, `version_id`, `ddl_job_id`, `columns_record`, `gmt_created`";

    private static final String FROM_TABLE = " from " + COLUMNAR_COLUMN_EVOLUTION_TABLE;

    private static final String ORDER_BY_ID = " order by id asc";

    private static final String WHERE_BY_TABLE_ID = " where `table_id`=?" + ORDER_BY_ID;

    private static final String WHERE_BY_FIELD_ID = " where `field_id`=?";

    private static final String WHERE_BY_VERSION_ID = " where `version_id`=?";

    private static final String WHERE_BY_TABLE_ID_AND_STATUS =
        " where `table_id`=? and JSON_UNQUOTE(JSON_EXTRACT(columns_record, '$.status')) = ?" + ORDER_BY_ID;

    private static final String WHERE_VERSION_ID_NOT_IN_FILED_ID_AND_STATUS =
        WHERE_BY_VERSION_ID + " and `table_id`=? and `field_id` not in ( select `field_id`" + FROM_TABLE
            + WHERE_BY_TABLE_ID_AND_STATUS + ")";

    private static final String AND_FILED_ID_ZERO = " and `field_id` = 0";

    private static final String WHERE_BY_TABLE_ID_VERSION_ID = " where `table_id` = ? and `version_id` = ?";
    private static final String WHERE_BY_TABLE_ID_GT_VERSION_ID = " where `table_id` = ? and `version_id` > ?";
    private static final String WHERE_BY_TABLE_ID_AND_VERSION_IDS = " where `table_id` = ? and `version_id` in (%s)";
    private static final String WHERE_BY_SCHEMA_TABLE_INDEX =
        " where `table_id` in (select `table_id` from " + COLUMNAR_TABLE_MAPPING_TABLE
            + " where `table_schema` = ? and `table_name` = ? and `index_name` = ?)" + ORDER_BY_ID;

    private static final String WHERE_BY_SCHEMA =
        " where `table_id` in (select `table_id` from " + COLUMNAR_TABLE_MAPPING_TABLE
            + " where `table_schema`=?) order by `id`";
    private static final String WHERE_BY_IDS = " where `id` in (%s)";

    private static final String SELECT_BY_TABLE_ID = SELECT_ALL_COLUMNS + FROM_TABLE + WHERE_BY_TABLE_ID + ORDER_BY_ID;

    private static final String SELECT_BY_IDS = SELECT_ALL_COLUMNS + FROM_TABLE + WHERE_BY_IDS;

    private static final String SELECT_BY_TABLE_ID_VERSION_ID_ORDER_BY_ID =
        SELECT_ALL_COLUMNS + FROM_TABLE + WHERE_BY_TABLE_ID_VERSION_ID + ORDER_BY_ID;

    private static final String SELECT_BY_TABLE_ID_GT_VERSION_ID_ORDER_BY_ID =
        SELECT_ALL_COLUMNS + FROM_TABLE + WHERE_BY_TABLE_ID_GT_VERSION_ID + ORDER_BY_ID;

    private static final String SELECT_BY_TABLE_ID_AND_VERSION_IDS_ORDER_BY_ID =
        SELECT_ALL_COLUMNS + FROM_TABLE + WHERE_BY_TABLE_ID_AND_VERSION_IDS + ORDER_BY_ID;

    private static final String UPDATE_FIELD_ID_AS_ID_BY_TABLE_ID_VERSION_ID =
        "update " + COLUMNAR_COLUMN_EVOLUTION_TABLE + " set `field_id`=`id`" + WHERE_BY_TABLE_ID_VERSION_ID
            + AND_FILED_ID_ZERO;
    private static final String UPDATE_COLUMN_EVOLUTION = "update " + COLUMNAR_COLUMN_EVOLUTION_TABLE + " set ";

    private static final String UPDATE_FIELD_ID_AS_ID_BY_TABLE_ID =
        UPDATE_COLUMN_EVOLUTION + " `field_id`=`id`" + WHERE_BY_TABLE_ID;

    private static final String UPDATE_FIELD_ID_AS_ID_BY_VERSION_ID =
        UPDATE_COLUMN_EVOLUTION + " `field_id`=`id`" + WHERE_BY_VERSION_ID;

    private static final String DELETE_SCHEMA = "delete " + FROM_TABLE + WHERE_BY_SCHEMA;

    private static final String DELETE_SCHEMA_TABLE_INDEX = "delete " + FROM_TABLE + WHERE_BY_SCHEMA_TABLE_INDEX;

    private static final String DELETE_TABLE_ID = "delete " + FROM_TABLE + WHERE_BY_TABLE_ID;

    private static final String UPDATE_COLUMNS_RECORD =
        UPDATE_COLUMN_EVOLUTION + " `columns_record`=?" + WHERE_BY_FIELD_ID;

    private static final String DELETE_BY_ID_STATUS =
        "delete " + FROM_TABLE + WHERE_BY_TABLE_ID_AND_STATUS;

    private static final String SELECT_BY_TABLE_ID_AND_NOT_IN_STATUS = "SELECT cce.*\n"
        + FROM_TABLE + " AS cce\n"
        + "LEFT JOIN " + COLUMNAR_COLUMN_EVOLUTION_TABLE + " AS cce2\n"
        + "  ON cce.column_name = cce2.column_name\n"
        + "  AND cce.id < cce2.id\n"
        + "  AND cce2.table_id = ?\n"
        + "WHERE cce2.id IS NULL\n"
        + "  AND cce.table_id = ?\n"
        + "  AND cce.field_id NOT IN (\n"
        + "    SELECT field_id\n"
        + FROM_TABLE
        + "    WHERE table_id = ?\n"
        + "      AND JSON_UNQUOTE(JSON_EXTRACT(columns_record, '$.status')) = ?\n"
        + "  )";

    private static final String SELECT_BY_TABLE_ID_AND_VERSION_ID_NOT_IN_STATUS =
        SELECT_ALL_COLUMNS + FROM_TABLE + WHERE_VERSION_ID_NOT_IN_FILED_ID_AND_STATUS;

    public int[] insert(List<ColumnarColumnEvolutionRecord> records) {
        List<Map<Integer, ParameterContext>> paramsBatch = new ArrayList<>(records.size());
        for (ColumnarColumnEvolutionRecord record : records) {
            paramsBatch.add(record.buildInsertParams());
        }
        try {
            DdlMetaLogUtil.logSql(INSERT_COLUMNAR_COLUMN_EVOLUTION_RECORDS, paramsBatch);
            return MetaDbUtil.insert(INSERT_COLUMNAR_COLUMN_EVOLUTION_RECORDS, paramsBatch, connection);
        } catch (SQLException e) {
            LOGGER.error("Failed to insert a batch of new records into " + COLUMNAR_COLUMN_EVOLUTION_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "batch insert into",
                COLUMNAR_COLUMN_EVOLUTION_TABLE,
                e.getMessage());
        }
    }

    public Long insertAndReturnLastInsertId(List<ColumnarColumnEvolutionRecord> records) {
        List<Map<Integer, ParameterContext>> paramsBatch = new ArrayList<>(records.size());
        for (ColumnarColumnEvolutionRecord record : records) {
            paramsBatch.add(record.buildInsertParams());
        }
        try {
            DdlMetaLogUtil.logSql(INSERT_COLUMNAR_COLUMN_EVOLUTION_RECORDS, paramsBatch);
            return MetaDbUtil.executeInsertAndReturnLastInsertId(INSERT_COLUMNAR_COLUMN_EVOLUTION_RECORDS, paramsBatch,
                connection);
        } catch (SQLException e) {
            LOGGER.error("Failed to insert a batch of new records into " + COLUMNAR_TABLE_EVOLUTION_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "batch insert into",
                COLUMNAR_TABLE_EVOLUTION_TABLE,
                e.getMessage());
        }
    }

    public List<ColumnarColumnEvolutionRecord> queryIds(List<Long> ids) {
        final Map<Integer, ParameterContext> params =
            MetaDbUtil.buildParameters(ParameterMethod.setLong, ids.toArray());
        return query(String.format(SELECT_BY_IDS, concatParams(ids.size())), COLUMNAR_COLUMN_EVOLUTION_TABLE,
            ColumnarColumnEvolutionRecord.class, params);
    }

    /**
     * For columnar use
     *
     * @return list of ColumnarColumnEvolutionRecord that keeps the order of fieldIdList
     */
    public List<ColumnarColumnEvolutionRecord> queryIdsWithOrder(List<Long> ids) {
        final List<ColumnarColumnEvolutionRecord> queryResults = queryIds(ids);

        // Reorder columns
        Map<Long, Integer> pos = new HashMap<>();
        for (int i = 0; i < ids.size(); i++) {
            pos.put(ids.get(i), i);
        }

        ColumnarColumnEvolutionRecord[] results = new ColumnarColumnEvolutionRecord[ids.size()];
        for (ColumnarColumnEvolutionRecord record : queryResults) {
            results[pos.get(record.id)] = record;
        }
        return Arrays.asList(results);
    }

    public List<ColumnarColumnEvolutionRecord> queryTableId(Long tableId) {
        return query(SELECT_BY_TABLE_ID, COLUMNAR_COLUMN_EVOLUTION_TABLE, ColumnarColumnEvolutionRecord.class, tableId);
    }

    public List<ColumnarColumnEvolutionRecord> queryTableIdAndNotInStatus(Long tableId, long status) {
        Map<Integer, ParameterContext> params = new HashMap<>(4);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, tableId);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, tableId);
        MetaDbUtil.setParameter(3, params, ParameterMethod.setLong, tableId);
        MetaDbUtil.setParameter(4, params, ParameterMethod.setString, String.valueOf(status));
        return query(SELECT_BY_TABLE_ID_AND_NOT_IN_STATUS, COLUMNAR_COLUMN_EVOLUTION_TABLE,
            ColumnarColumnEvolutionRecord.class, params);
    }

    public List<ColumnarColumnEvolutionRecord> queryTableIdAndNotInStatus(Long tableId, Long versionId, long status) {
        Map<Integer, ParameterContext> params = new HashMap<>(4);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, versionId);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, tableId);
        MetaDbUtil.setParameter(3, params, ParameterMethod.setLong, tableId);
        MetaDbUtil.setParameter(4, params, ParameterMethod.setString, String.valueOf(status));
        return query(SELECT_BY_TABLE_ID_AND_VERSION_ID_NOT_IN_STATUS, COLUMNAR_COLUMN_EVOLUTION_TABLE,
            ColumnarColumnEvolutionRecord.class, params);
    }

    public List<ColumnarColumnEvolutionRecord> queryTableIdVersionIdOrderById(Long tableId, Long versionId) {
        return query(SELECT_BY_TABLE_ID_VERSION_ID_ORDER_BY_ID,
            COLUMNAR_COLUMN_EVOLUTION_TABLE,
            ColumnarColumnEvolutionRecord.class,
            tableId,
            versionId);
    }

    public List<ColumnarColumnEvolutionRecord> queryTableIdGreaterThanVersionIdOrderById(Long tableId, Long versionId) {
        return query(SELECT_BY_TABLE_ID_GT_VERSION_ID_ORDER_BY_ID,
            COLUMNAR_COLUMN_EVOLUTION_TABLE,
            ColumnarColumnEvolutionRecord.class,
            tableId,
            versionId);
    }

    public List<ColumnarColumnEvolutionRecord> queryTableIdAndVersionIdsOrderById(Long tableId, List<Long> versionIds) {
        if (CollectionUtils.isEmpty(versionIds)) {
            return new ArrayList<>();
        }
        List<Long> paramValues = new ArrayList<>();
        paramValues.add(tableId);
        paramValues.addAll(versionIds);
        final Map<Integer, ParameterContext> params =
            MetaDbUtil.buildParameters(ParameterMethod.setLong, paramValues.toArray());
        return query(String.format(SELECT_BY_TABLE_ID_AND_VERSION_IDS_ORDER_BY_ID, concatParams(versionIds.size())),
            COLUMNAR_COLUMN_EVOLUTION_TABLE,
            ColumnarColumnEvolutionRecord.class, params);
    }

    public void updateFieldIdAsId(long tableId, long versionId) {
        Map<Integer, ParameterContext> params = new HashMap<>(1);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, tableId);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, versionId);
        update(UPDATE_FIELD_ID_AS_ID_BY_TABLE_ID_VERSION_ID, COLUMNAR_COLUMN_EVOLUTION_TABLE, params);
    }

    public void updateFieldIdAsIdByVersionId(long versionId) {
        Map<Integer, ParameterContext> params = new HashMap<>(1);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, versionId);
        update(UPDATE_FIELD_ID_AS_ID_BY_VERSION_ID, COLUMNAR_COLUMN_EVOLUTION_TABLE, params);
    }

    public void updateColumnRecord(ColumnsRecord columnsRecord, long fieldId) {
        Map<Integer, ParameterContext> params = new HashMap<>(2);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString,
            ColumnarColumnEvolutionRecord.serializeToJson(columnsRecord));
        MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, fieldId);
        update(UPDATE_COLUMNS_RECORD, COLUMNAR_COLUMN_EVOLUTION_TABLE, params);
    }

    public int deleteSchema(String schemaName) {
        Map<Integer, ParameterContext> params = new HashMap<>(1);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, schemaName);
        try {
            DdlMetaLogUtil.logSql(DELETE_SCHEMA, params);
            return MetaDbUtil.delete(DELETE_SCHEMA, params, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public int deleteByTableIdAndStatus(long tableId, long status) {
        Map<Integer, ParameterContext> params = new HashMap<>(2);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, tableId);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setString, String.valueOf(status));
        try {
            DdlMetaLogUtil.logSql(DELETE_BY_ID_STATUS, params);
            return MetaDbUtil.delete(DELETE_BY_ID_STATUS, params, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public int deleteSchemaTableIndex(String schemaName, String tableName, String indexName) {
        Map<Integer, ParameterContext> params = new HashMap<>(3);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, schemaName);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setString, tableName);
        MetaDbUtil.setParameter(3, params, ParameterMethod.setString, indexName);
        return delete(DELETE_SCHEMA_TABLE_INDEX, COLUMNAR_COLUMN_EVOLUTION_TABLE, params);

    }

    public int deleteId(long tableId) {
        Map<Integer, ParameterContext> params = new HashMap<>(1);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, tableId);
        return delete(DELETE_TABLE_ID, COLUMNAR_COLUMN_EVOLUTION_TABLE, params);
    }
}

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

package com.alibaba.polardbx.gms.heatmap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

/**
 * @author ximing.yd
 * @date 2022/2/23 4:03 下午
 */
public class PartitionsHeatmapAccessor extends AbstractAccessor {

    private static final Logger logger = LoggerFactory.getLogger(PartitionsHeatmapAccessor.class);

    private static final String PARTITIONS_HEATMAP = "partitions_heatmap";

    private static final String ALL_COLUMNS =
        "`id`," +
            "`layer_num`," +
            "`timestamp`," +
            "`axis`";

    private static final String ALL_VALUES = "(null,?,?,?)";

    private static final String INSERT_PARTITIONS_HEATMAP_SQL =
        "INSERT INTO " + PARTITIONS_HEATMAP + " (" + ALL_COLUMNS + ") VALUES " + ALL_VALUES;

    private static final String QUERY_BY_LAYER_NUM_SQL
        = "SELECT " + ALL_COLUMNS + " FROM " + PARTITIONS_HEATMAP + " WHERE layer_num=? order by timestamp ASC;";

    private static final String DELETE_PARTITIONS_HEATMAP_SQL
        = "DELETE FROM " + PARTITIONS_HEATMAP + " WHERE layer_num=? AND timestamp=?;";

    private static final String QUERY_COUNT_LAYER_VISUAL_AXIS_SQL
        = "SELECT count(1) AS COUNT FROM " + PARTITIONS_HEATMAP + ";";

    /**
     * Failed to {0} the system table {1}. Caused by: {2}.
     */
    private TddlRuntimeException logAndThrow(String errMsg, String action, Exception e) {
        logger.error(errMsg, e);
        return new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
            action,
            PARTITIONS_HEATMAP,
            e.getMessage()
        );
    }

    public int insert(PartitionsHeatmapRecord record) {
        try {
            return MetaDbUtil.insert(INSERT_PARTITIONS_HEATMAP_SQL, record.buildParams(), connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to insert into " + PARTITIONS_HEATMAP, "insert into", e);
        }
    }

    public List<PartitionsHeatmapRecord> queryByLayerNum(Integer layerNum) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(1);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setInt, layerNum);
            return MetaDbUtil.query(QUERY_BY_LAYER_NUM_SQL, params, PartitionsHeatmapRecord.class, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to query " + PARTITIONS_HEATMAP, "query", e);
        }
    }

    public List<Map<String, Object>> queryHeatmapCount() {
        try {
            return MetaDbUtil.queryCount(QUERY_COUNT_LAYER_VISUAL_AXIS_SQL, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to query " + PARTITIONS_HEATMAP, "query", e);
        }
    }

    public int deleteByParam(Integer layerNum, Long timestamp) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(2);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setInt, layerNum);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, timestamp);
            return MetaDbUtil.delete(DELETE_PARTITIONS_HEATMAP_SQL, params, connection);
        } catch (Exception e) {
            throw logAndThrow(String.format("Failed to delete from %s for layerNum:%s timestamp:%s ",
                PARTITIONS_HEATMAP, layerNum, timestamp), "delete from", e);
        }
    }

}

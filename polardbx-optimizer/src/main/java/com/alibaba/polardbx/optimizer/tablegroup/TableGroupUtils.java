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

package com.alibaba.polardbx.optimizer.tablegroup;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupRecord;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class TableGroupUtils {

    public static String getPartitionDefinition(TableGroupConfig tgConfig, ExecutionContext ec) {
        if (tgConfig == null) {
            return StringUtils.EMPTY;
        }
        if (GeneralUtil.isEmpty(tgConfig.getTables()) || tgConfig.getTableGroupRecord().isSingleTableGroup()) {
            return tgConfig.getTableGroupRecord().getPartition_definition();
        } else {
            String tableName = tgConfig.getTables().get(0);
            String schemaName = tgConfig.getTableGroupRecord().getSchema();
            SchemaManager schemaManager = ec.getSchemaManager(schemaName);
            TableMeta tableMeta = schemaManager.getTable(tableName);
            PartitionInfo partInfo = tableMeta.getPartitionInfo();
            List<Integer> allLevelActualPartColCnts = partInfo.getAllLevelActualPartColCounts();
            return tableMeta.getPartitionInfo().getPartitionBy()
                .normalizePartitionByDefForShowTableGroup(tgConfig, true, allLevelActualPartColCnts);
        }
    }

    public static String getPreDefinePartitionInfo(TableGroupConfig tgConfig, ExecutionContext ec) {
        if (tgConfig == null) {
            return StringUtils.EMPTY;
        }
        if (tgConfig.isPreDefinePartitionInfo()) {
            return getPartitionDefinition(tgConfig, ec);
        } else {
            return StringUtils.EMPTY;
        }
    }
}

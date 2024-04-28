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

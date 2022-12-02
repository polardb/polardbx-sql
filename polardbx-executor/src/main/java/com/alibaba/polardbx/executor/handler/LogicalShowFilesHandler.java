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

package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.FileMeta;
import com.alibaba.polardbx.optimizer.config.table.OSSOrcFileMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.pruning.PhysicalPartitionInfo;
import com.alibaba.polardbx.optimizer.utils.ITimestampOracle;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlShowFiles;

import java.sql.Date;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class LogicalShowFilesHandler extends HandlerCommon {
    public LogicalShowFilesHandler(IRepository repo) {
        super(repo);
    }

    private static final int PART_NAME_INDEX = 3;
    private static final int FILE_NAME_INDEX = 4;
    private static final Comparator<Object[]> ROW_COMPARATOR = (l, r) -> {
        Comparable lPartName = (Comparable) l[PART_NAME_INDEX];
        Comparable rPartName = (Comparable) r[PART_NAME_INDEX];
        int partCmp;
        if ((partCmp = lPartName.compareTo(rPartName)) == 0) {
            Comparable lFileName = (Comparable) l[FILE_NAME_INDEX];
            Comparable rFileName = (Comparable) r[FILE_NAME_INDEX];
            return lFileName.compareTo(rFileName);
        }
        return partCmp;
    };

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        final LogicalShow show = (LogicalShow) logicalPlan;
        final SqlShowFiles showFiles = (SqlShowFiles) show.getNativeSqlNode();
        final SqlNode sqlTableName = showFiles.getTableName();
        String tableName = RelUtils.lastStringValue(sqlTableName);
        final OptimizerContext context = OptimizerContext.getContext(show.getSchemaName());
        //test the table existence
        TableMeta tableMeta = context.getLatestSchemaManager().getTable(tableName);
        if (tableMeta == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_TABLE_NOT_EXIST,
                MessageFormat.format("table {0} does not exists", tableName));
        } else if (!Engine.isFileStore(tableMeta.getEngine())) {
            throw GeneralUtil.nestedException(MessageFormat.format(
                "Only support show files from File-Store table. The table engine of {0} is {1}",
                tableMeta.getTableName(), tableMeta.getEngine()));
        } else if (tableMeta.getPartitionInfo() == null) {
            throw GeneralUtil.nestedException("Only support show files operation in partition mode.");
        }
        return handlePartitionedTable(tableMeta);
    }

    private Cursor handlePartitionedTable(TableMeta tableMeta) {
        ArrayResultCursor result = new ArrayResultCursor("FILES");
        result.addColumn("ID", DataTypes.IntegerType);
        result.addColumn("GROUP_NAME", DataTypes.StringType);
        result.addColumn("TABLE_NAME", DataTypes.StringType);
        result.addColumn("PARTITION_NAME", DataTypes.StringType);
        result.addColumn("FILE_NAME", DataTypes.StringType);
        result.addColumn("FILE_SIZE", DataTypes.LongType);
        result.addColumn("ROW_COUNT", DataTypes.LongType);
        result.addColumn("CREATE_TIME", DataTypes.StringType);
        result.addColumn("COMMIT_TSO_TIME", DataTypes.StringType);
        PartitionInfo partitionInfo = tableMeta.getPartitionInfo();
        Map<String, List<FileMeta>> flatFileMetas = tableMeta.getFlatFileMetas();
        result.initMeta();
        int index = 0;
        Map<String, List<PhysicalPartitionInfo>> physicalPartitionInfos =
            partitionInfo.getPhysicalPartitionTopology(new ArrayList<>());
        List<Object[]> rows = new ArrayList<>();
        for (Map.Entry<String, List<PhysicalPartitionInfo>> phyPartItem : physicalPartitionInfos.entrySet()) {
            String grpGroupKey = phyPartItem.getKey();
            List<PhysicalPartitionInfo> phyPartList = phyPartItem.getValue();
            for (int i = 0; i < phyPartList.size(); i++) {
                String partName = phyPartList.get(i).getPartName();
                String phyTable = phyPartList.get(i).getPhyTable();
                List<FileMeta> fileMetaList = flatFileMetas.get(phyTable);
                for (FileMeta fileMeta : fileMetaList) {
                    OSSOrcFileMeta ossOrcFileMeta = (OSSOrcFileMeta) fileMeta;
                    String fileName = ossOrcFileMeta.getFileName();
                    long fileSize = ossOrcFileMeta.getFileSize();
                    long rowCount = ossOrcFileMeta.getTableRows();
                    String createTime = ossOrcFileMeta.getCreateTime();
                    if (ossOrcFileMeta.getRemoveTs() == null) {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        rows.add(
                            new Object[] {
                                index++,
                                grpGroupKey,
                                phyTable,
                                partName,
                                fileName,
                                fileSize,
                                rowCount,
                                createTime,
                                ossOrcFileMeta.getCommitTs() == null ? null :
                                        sdf.format(new Date(ossOrcFileMeta.getCommitTs() >> ITimestampOracle.BITS_LOGICAL_TIME))
                            });
                    }
                }
            }
        }
        // sort
        Collections.sort(rows, ROW_COMPARATOR);
        rows.forEach(row -> result.addRow(row));
        return result;
    }
}
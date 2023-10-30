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

package com.alibaba.polardbx.executor.handler.ddl;

import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.ddl.job.builder.DdlPhyPlanBuilder;
import com.alibaba.polardbx.executor.ddl.job.builder.gsi.CreateGlobalIndexBuilder;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.factory.gsi.AlterPartitionCountJobFactory;
import com.alibaba.polardbx.executor.ddl.job.validator.ddl.RepartitionValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.TransientDdlJob;
import com.alibaba.polardbx.executor.gms.util.AlterRepartitionUtils;
import com.alibaba.polardbx.executor.gsi.GsiUtils;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTablePartitionCount;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTablePartitionsPrepareData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.CreateGlobalIndexPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import org.apache.calcite.sql.SqlAddIndex;
import org.apache.calcite.sql.SqlAddUniqueIndex;
import org.apache.calcite.sql.SqlAlterTablePartitionCount;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlIndexDefinition;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class LogicalAlterTablePartitionCountHandler extends LogicalCommonDdlHandler {
    public LogicalAlterTablePartitionCountHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        final LogicalAlterTablePartitionCount logicalAlterTablePartitionCount =
            (LogicalAlterTablePartitionCount) logicalDdlPlan;

        SqlAlterTablePartitionCount ast = (SqlAlterTablePartitionCount) logicalAlterTablePartitionCount.relDdl.sqlNode;

        // validate
        RepartitionValidator.validate(ast.getSchemaName(), ast.getPrimaryTableName());

        // validate partition count
        boolean isPartitionRuleUnchanged = RepartitionValidator.validatePartitionCount(
            ast.getSchemaName(),
            ast.getPrimaryTableName(),
            ast.getPartitionCount()
        );

        //no need to repartition
        if (isPartitionRuleUnchanged) {
            return new TransientDdlJob();
        }

        // prepare
        Map<String, String> tableNameMap = new HashMap<>();
        initNewTableDefinition(logicalAlterTablePartitionCount, tableNameMap, executionContext);
        logicalAlterTablePartitionCount.prepareData();

        List<CreateGlobalIndexPreparedData> globalIndexesPreparedData =
            logicalAlterTablePartitionCount.getCreateGlobalIndexesPreparedData();

        Map<CreateGlobalIndexPreparedData, PhysicalPlanData> globalIndexPrepareData = new HashMap<>();
        for (CreateGlobalIndexPreparedData createGsiPreparedData : globalIndexesPreparedData) {
            DdlPhyPlanBuilder builder = CreateGlobalIndexBuilder.create(
                logicalAlterTablePartitionCount.relDdl,
                createGsiPreparedData,
                executionContext).build();

            globalIndexPrepareData.put(createGsiPreparedData, builder.genPhysicalPlanData());
        }

        return new AlterPartitionCountJobFactory(
            ast.getSchemaName(),
            ast.getPrimaryTableName(),
            tableNameMap,
            globalIndexPrepareData,
            executionContext
        ).create();
    }

    private void initNewTableDefinition(BaseDdlOperation logicalDdlPlan,
                                        Map<String, String> tableNameMap,
                                        ExecutionContext executionContext) {
        Pair<String, SqlCreateTable> primaryTableInfo = genPrimaryTableInfo(logicalDdlPlan, executionContext);

        SqlAlterTablePartitionCount ast = (SqlAlterTablePartitionCount) logicalDdlPlan.relDdl.sqlNode;
        String schemaName = ast.getSchemaName();
        String primaryTableName = ast.getPrimaryTableName();
        int partitionCnt = ast.getPartitionCount();

        // logical table name --> new logical table name
        List<AlterTablePartitionsPrepareData> createGsiPrepareDataList = new ArrayList<>();

        // handle primary table
        String targetTableName =
            executionContext.getParamManager().getString(ConnectionParams.REPARTITION_FORCE_GSI_NAME);
        if (StringUtils.isEmpty(targetTableName)) {
            targetTableName = GsiUtils.generateRandomGsiName(primaryTableName);
        }
        tableNameMap.put(primaryTableName, targetTableName);
        ast.setLogicalSecondaryTableName(targetTableName);
        createGsiPrepareDataList.add(new AlterTablePartitionsPrepareData(primaryTableName, targetTableName));

        // handle gsi table
        final GsiMetaManager.GsiMetaBean gsiMetaBean =
            OptimizerContext.getContext(schemaName).getLatestSchemaManager().getGsi(primaryTableName, IndexStatus.ALL);
        GsiMetaManager.GsiTableMetaBean tableMeta = gsiMetaBean.getTableMeta().get(primaryTableName);
        if (tableMeta != null) {
            for (Map.Entry<String, GsiMetaManager.GsiIndexMetaBean> indexEntry : tableMeta.indexMap.entrySet()) {
                final String indexName = indexEntry.getKey();
                final GsiMetaManager.GsiIndexMetaBean indexDetail = indexEntry.getValue();

                if (indexDetail.indexStatus != IndexStatus.PUBLIC) {
                    throw new TddlRuntimeException(ErrorCode.ERR_REPARTITION_TABLE_WITH_GSI,
                        "can not alter table partitions when gsi table is not public");
                }

                // filter partition by key and partitions
                PartitionInfo partitionInfo =
                    OptimizerContext.getContext(schemaName).getPartitionInfoManager().getPartitionInfo(indexName);
                PartitionByDefinition partitionByDefinition = partitionInfo.getPartitionBy();

                List<List<String>> allLevelActualPartCols = partitionInfo.getAllLevelActualPartCols();
                boolean useSubPartBy = partitionInfo.getPartitionBy().getSubPartitionBy() != null;

                if (!useSubPartBy) {
                    // for auto partition table, create index like mysql, we will create gsi with partition by key(...)
                    if (!partitionByDefinition.getStrategy().isKey()
                        || partitionByDefinition.getPartitions().size() == partitionCnt
                        || allLevelActualPartCols.get(0).size() != 1) {
                        continue;
                    }
                } else {
                    throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT);
                }

                final String newIndexName = GsiUtils.generateRandomGsiName(indexName);
                tableNameMap.put(indexName, newIndexName);

                AlterTablePartitionsPrepareData prepareData =
                    new AlterTablePartitionsPrepareData(indexName, newIndexName);
                prepareData.setPartitionInfo(partitionInfo);
                prepareData.setIndexDetail(indexDetail);

                createGsiPrepareDataList.add(prepareData);
            }
        }

        List<SqlIndexDefinition> repartitionGsi = AlterRepartitionUtils.initIndexInfo(
            schemaName,
            partitionCnt,
            createGsiPrepareDataList,
            primaryTableInfo.getValue(),
            primaryTableInfo.getKey()
        );

        List<SqlAddIndex> sqlAddIndexList = repartitionGsi.stream().map(e ->
            StringUtils.equalsIgnoreCase(e.getType(), "UNIQUE") ?
                new SqlAddUniqueIndex(SqlParserPos.ZERO, e.getIndexName(), e) :
                new SqlAddIndex(SqlParserPos.ZERO, e.getIndexName(), e)
        ).collect(Collectors.toList());
        ast.getAlters().addAll(sqlAddIndexList);
    }

    @Override
    protected boolean validatePlan(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        return false;
    }
}

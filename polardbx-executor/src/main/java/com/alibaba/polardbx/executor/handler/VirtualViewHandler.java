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

import com.alibaba.polardbx.common.constants.SystemTables;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.subhandler.BaseVirtualViewSubClassHandler;
import com.alibaba.polardbx.executor.handler.subhandler.InformationSchemaCclRuleHandler;
import com.alibaba.polardbx.executor.handler.subhandler.InformationSchemaCclTriggerHandler;
import com.alibaba.polardbx.executor.handler.subhandler.InformationSchemaCollationsCharsetHandler;
import com.alibaba.polardbx.executor.handler.subhandler.InformationSchemaCollationsHandler;
import com.alibaba.polardbx.executor.handler.subhandler.InformationSchemaDdlPlanHandler;
import com.alibaba.polardbx.executor.handler.subhandler.InformationSchemaDnPerfHandler;
import com.alibaba.polardbx.executor.handler.subhandler.InformationSchemaDrdsPhysicalProcessInTrxHandler;
import com.alibaba.polardbx.executor.handler.subhandler.InformationSchemaFileStorageHandler;
import com.alibaba.polardbx.executor.handler.subhandler.InformationSchemaGlobalIndexesHandler;
import com.alibaba.polardbx.executor.handler.subhandler.InformationSchemaInformationSchemaColumnsHandler;
import com.alibaba.polardbx.executor.handler.subhandler.InformationSchemaInformationSchemaTablesHandler;
import com.alibaba.polardbx.executor.handler.subhandler.InformationSchemaInnodbBufferHandler;
import com.alibaba.polardbx.executor.handler.subhandler.InformationSchemaInnodbLockWaitsHandler;
import com.alibaba.polardbx.executor.handler.subhandler.InformationSchemaInnodbLocksHandler;
import com.alibaba.polardbx.executor.handler.subhandler.InformationSchemaInnodbPurgeFileHandler;
import com.alibaba.polardbx.executor.handler.subhandler.InformationSchemaInnodbTrxHandler;
import com.alibaba.polardbx.executor.handler.subhandler.InformationSchemaKeyColumnUsageHandler;
import com.alibaba.polardbx.executor.handler.subhandler.InformationSchemaKeywordsHandler;
import com.alibaba.polardbx.executor.handler.subhandler.InformationSchemaLocalPartitionsHandler;
import com.alibaba.polardbx.executor.handler.subhandler.InformationSchemaLocalPartitionsScheduleHandler;
import com.alibaba.polardbx.executor.handler.subhandler.InformationSchemaLocalityInfoHandler;
import com.alibaba.polardbx.executor.handler.subhandler.InformationSchemaMetadataLockHandler;
import com.alibaba.polardbx.executor.handler.subhandler.InformationSchemaMoveDatabaseHandler;
import com.alibaba.polardbx.executor.handler.subhandler.InformationSchemaFileStorageFilesMetaHandler;
import com.alibaba.polardbx.executor.handler.subhandler.InformationSchemaPhysicalProcesslistHandler;
import com.alibaba.polardbx.executor.handler.subhandler.InformationSchemaPlanCacheCapacityHandler;
import com.alibaba.polardbx.executor.handler.subhandler.InformationSchemaPlanCacheHandler;
import com.alibaba.polardbx.executor.handler.subhandler.InformationSchemaProcesslistHandler;
import com.alibaba.polardbx.executor.handler.subhandler.InformationSchemaProfilingHandler;
import com.alibaba.polardbx.executor.handler.subhandler.InformationSchemaQueryInfoHandler;
import com.alibaba.polardbx.executor.handler.subhandler.InformationSchemaReactorPerfHandler;
import com.alibaba.polardbx.executor.handler.subhandler.InformationSchemaSPMHandler;
import com.alibaba.polardbx.executor.handler.subhandler.InformationSchemaSchemataHandler;
import com.alibaba.polardbx.executor.handler.subhandler.InformationSchemaSessionPerfHandler;
import com.alibaba.polardbx.executor.handler.subhandler.InformationSchemaStatisticTaskHandler;
import com.alibaba.polardbx.executor.handler.subhandler.InformationSchemaTableDetailHandler;
import com.alibaba.polardbx.executor.handler.subhandler.InformationSchemaTableGroupHandler;
import com.alibaba.polardbx.executor.handler.subhandler.InformationSchemaTablesHandler;
import com.alibaba.polardbx.executor.handler.subhandler.InformationSchemaTcpPerfHandler;
import com.alibaba.polardbx.executor.handler.subhandler.InformationSchemaUserPrivilegesHandler;
import com.alibaba.polardbx.executor.handler.subhandler.InformationSchemaVariablesHandler;
import com.alibaba.polardbx.executor.handler.subhandler.InformationSchemaWorkloadHandler;
import com.alibaba.polardbx.executor.handler.subhandler.VirtualStatisticHandler;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.function.calc.scalar.filter.Like;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.view.VirtualView;
import com.alibaba.polardbx.rule.TableRule;
import com.alibaba.polardbx.rule.TddlRule;
import com.alibaba.polardbx.rule.model.TargetDB;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author dylan
 */
public class VirtualViewHandler extends HandlerCommon {

    private List<BaseVirtualViewSubClassHandler> subHandler;

    public VirtualViewHandler(IRepository repo) {
        super(repo);
        subHandler = new ArrayList<>();
        subHandler.add(new VirtualStatisticHandler(this));
        subHandler.add(new InformationSchemaTablesHandler(this));
        subHandler.add(new InformationSchemaSchemataHandler(this));
        subHandler.add(new InformationSchemaInformationSchemaTablesHandler(this));
        subHandler.add(new InformationSchemaInformationSchemaColumnsHandler(this));
        subHandler.add(new InformationSchemaKeywordsHandler(this));
        subHandler.add(new InformationSchemaCollationsHandler(this));
        subHandler.add(new InformationSchemaProfilingHandler(this));
        subHandler.add(new InformationSchemaDrdsPhysicalProcessInTrxHandler(this));
        subHandler.add(new InformationSchemaProcesslistHandler(this));
        subHandler.add(new InformationSchemaVariablesHandler(this));
        subHandler.add(new InformationSchemaWorkloadHandler(this));
        subHandler.add(new InformationSchemaQueryInfoHandler(this));
        subHandler.add(new InformationSchemaGlobalIndexesHandler(this));
        subHandler.add(new InformationSchemaMetadataLockHandler(this));
        subHandler.add(new InformationSchemaUserPrivilegesHandler(this));
        subHandler.add(new InformationSchemaTableGroupHandler(this));
        subHandler.add(new InformationSchemaLocalPartitionsHandler(this));
        subHandler.add(new InformationSchemaLocalPartitionsScheduleHandler(this));
        subHandler.add(new InformationSchemaTableDetailHandler(this));
        subHandler.add(new InformationSchemaLocalityInfoHandler(this));
        subHandler.add(new InformationSchemaMoveDatabaseHandler(this));
        subHandler.add(new InformationSchemaInnodbLocksHandler(this));
        subHandler.add(new InformationSchemaInnodbTrxHandler(this));
        subHandler.add(new InformationSchemaInnodbLockWaitsHandler(this));
        subHandler.add(new InformationSchemaPhysicalProcesslistHandler(this));
        subHandler.add(new InformationSchemaPlanCacheHandler(this));
        subHandler.add(new InformationSchemaInnodbBufferHandler(this));
        subHandler.add(new InformationSchemaInnodbPurgeFileHandler(this));
        subHandler.add(new InformationSchemaStatisticTaskHandler(this));
        subHandler.add(new InformationSchemaCclRuleHandler(this));
        subHandler.add(new InformationSchemaCclTriggerHandler(this));
        subHandler.add(new InformationSchemaSPMHandler(this));
        subHandler.add(new InformationSchemaPlanCacheCapacityHandler(this));
        subHandler.add(new InformationSchemaReactorPerfHandler(this));
        subHandler.add(new InformationSchemaDnPerfHandler(this));
        subHandler.add(new InformationSchemaTcpPerfHandler(this));
        subHandler.add(new InformationSchemaSessionPerfHandler(this));
        subHandler.add(new InformationSchemaFileStorageHandler(this));
        subHandler.add(new InformationSchemaCollationsCharsetHandler(this));
        subHandler.add(new InformationSchemaKeyColumnUsageHandler(this));
        subHandler.add(new InformationSchemaDdlPlanHandler(this));
        subHandler.add(new InformationSchemaFileStorageFilesMetaHandler(this));
    }

    @Override
    public Cursor handle(final RelNode logicalPlan, ExecutionContext executionContext) {
        VirtualView virtualView = (VirtualView) logicalPlan;

        ArrayResultCursor cursor = new ArrayResultCursor(virtualView.getVirtualViewType().name());

        for (RelDataTypeField field : virtualView.getRowType().getFieldList()) {
            cursor.addColumn(field.getName(), DataTypeUtil.calciteToDrdsType(field.getType()));
        }

        for (BaseVirtualViewSubClassHandler virtualViewSub : subHandler) {
            if (virtualViewSub.isSupport(virtualView)) {
                return virtualViewSub.handle(virtualView, executionContext,
                    cursor);
            }
        }
        // return no data
        return cursor;
    }

    public Map<String, Set<Pair<String, String>>> getGroupToPair(String schemaName, Set<String> indexTableNames,
                                                                 String tableLike, boolean testMode) {
        TddlRuleManager tddlRuleManager = OptimizerContext.getContext(schemaName).getRuleManager();
        PartitionInfoManager partitionInfoManager = OptimizerContext.getContext(schemaName).getPartitionInfoManager();

        // groupName -> {(logicalTableName, physicalTableName)}
        Map<String, Set<Pair<String, String>>> groupToPair = new HashMap<>();

        List<String> logicalTableNameSet = new ArrayList<>();

        TddlRule tddlRule = tddlRuleManager.getTddlRule();
        Collection<TableRule> tableRules = tddlRule.getTables();
        for (TableRule tableRule : tableRules) {
            String logicalTableName = tableRule.getVirtualTbName();
            if (SystemTables.contains(logicalTableName)) {
                continue;
            }
            logicalTableNameSet.add(logicalTableName);
        }
        for (PartitionInfo partitionInfo : partitionInfoManager.getPartitionInfos()) {
            String logicalTableName = partitionInfo.getTableName();
            if (SystemTables.contains(logicalTableName)) {
                continue;
            }
            logicalTableNameSet.add(logicalTableName);
        }

        Like likeFunc = new Like(null, null);

        for (String logicalTableName : logicalTableNameSet) {
            if (indexTableNames != null && !indexTableNames.isEmpty()) {
                if (!indexTableNames.contains(logicalTableName.toLowerCase())) {
                    continue;
                }
            }

            if (tableLike != null) {
                if (!likeFunc.like(logicalTableName, tableLike)) {
                    continue;
                }
            }

            TargetDB targetDB = tddlRuleManager.shardAny(logicalTableName);
            Set<Pair<String, String>> collection = groupToPair.get(targetDB.getDbIndex());
            if (collection == null) {
                collection = new HashSet<>();
                groupToPair.put(targetDB.getDbIndex(), collection);
            }

            String physicalTableName = targetDB.getTableNames().iterator().next();

            collection.add(Pair.of(logicalTableName, physicalTableName));
        }
        return groupToPair;
    }
}

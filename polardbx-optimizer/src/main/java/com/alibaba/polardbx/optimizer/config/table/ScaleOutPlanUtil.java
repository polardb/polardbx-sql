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

package com.alibaba.polardbx.optimizer.config.table;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.common.model.Matrix;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.gms.ha.HaSwitchParams;
import com.alibaba.polardbx.gms.ha.impl.StorageHaManager;
import com.alibaba.polardbx.gms.ha.impl.StorageInstHaContext;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineAccessor;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineRecord;
import com.alibaba.polardbx.gms.tablegroup.ComplexTaskOutlineRecord;
import com.alibaba.polardbx.gms.topology.DbGroupInfoAccessor;
import com.alibaba.polardbx.gms.topology.DbGroupInfoRecord;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoAccessor;
import com.alibaba.polardbx.gms.topology.StorageInfoRecord;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.gms.util.MetaDbLogUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.rule.TableRule;
import org.apache.calcite.sql.SqlDropDatabase;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlMoveDatabase;
import org.apache.calcite.sql.SqlNode;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author chenghui.lch
 */
public class ScaleOutPlanUtil {
    public static final EnumSet<SqlKind> MODIFY_DML =
        //never push the replace dml in delete_only status
        EnumSet.of(/*SqlKind.REPLACE, */SqlKind.DELETE, SqlKind.UPDATE);

    // default enable for polardbx codeline
    public static boolean isEnabledScaleOut(ParamManager paramManager) {
        return paramManager.getBoolean(ConnectionParams.ENABLE_SCALE_OUT_FEATURE);
    }

    public static boolean dropOldDatabaseAfterSwitchDatasource(ParamManager paramManager) {
        return paramManager.getBoolean(ConnectionParams.SCALE_OUT_DROP_DATABASE_AFTER_SWITCH_DATASOURCE);
    }

    public static List<String> getPhysicalTables(String groupName, String schemaName, String logicalTable) {
        final Map<String, Set<String>> phyTables = getPhyTables(schemaName, groupName, logicalTable);
        final List<String> tables = new ArrayList<>();

        phyTables.forEach((k, v) -> {
            if (k.equalsIgnoreCase(groupName)) {
                tables.addAll(v);
            }
        });
        return tables;
    }

    /**
     * return group and physical tables for one logical table.
     *
     * @return db: [tbs], db and tb are both sorted
     */
    public static Map<String, Set<String>> getPhyTables(String schemaName, String dbName, String logicalTableName) {
        TableRule tableRule = OptimizerContext.getContext(schemaName).getRuleManager().getTableRule(logicalTableName);
        Map<String, Set<String>> topology = new HashMap();
        Set<String> groupTopology = new HashSet(1);
        groupTopology.add(logicalTableName);
        if (tableRule != null) {
            TddlRuleManager or = OptimizerContext.getContext(schemaName).getRuleManager();
            if (or.isBroadCast(logicalTableName)) {
                assert tableRule.getActualTopology() != null;
                for (Map.Entry<String, Set<String>> entry : tableRule.getActualTopology().entrySet()) {
                    topology.put(dbName, entry.getValue());
                    break;
                }
            } else {
                topology.putAll(tableRule.getActualTopology());
            }
        } else {
            topology.put(OptimizerContext.getContext(schemaName).getRuleManager().getDefaultDbIndex(logicalTableName),
                groupTopology);
        }

        topology.entrySet().removeIf(entry -> !entry.getKey().equalsIgnoreCase(dbName));
        return topology;
    }

    // all the GSI table will be append to after all the normal table
    // in this list if the
    public static List<String> getLogicalTables(String schemaName) {
        List<String> logicalTables = new ArrayList<>();
        final OptimizerContext oc = OptimizerContext.getContext(schemaName);
        if (oc != null) {
            final Collection<TableRule> tablesRule = oc.getRuleManager().getTddlRule().getTables();

            tablesRule.forEach(tableRule -> {
                String tableName = tableRule.getVirtualTbName();
                // skip the tables in the single db
                if (tableRule.getRuleDbCount() > 1 || (tableRule.getRuleDbCount() == 1 && tableRule
                    .isBroadcast())) {
                    logicalTables.add(tableName);
                }

            });
        }
        return logicalTables;
    }

    public static List<String> getLogicalTables(String schemaName, List<String> gsilogicalTables, ExecutionContext ec) {
        List<String> normallogicalTables = new ArrayList<>();
        final OptimizerContext oc = OptimizerContext.getContext(schemaName);
        if (oc != null) {
            final Collection<TableRule> tablesRule = oc.getRuleManager().getTddlRule().getTables();

            tablesRule.forEach(tableRule -> {
                String tableName = tableRule.getVirtualTbName();
                if (GlobalIndexMeta.isGsiTable(tableName, schemaName, ec)) {
                    gsilogicalTables.add(tableName);
                } else {
                    // skip the tables in the single db
                    if (tableRule.getRuleDbCount() > 1 || (tableRule.getRuleDbCount() == 1 && tableRule
                        .isBroadcast())) {
                        normallogicalTables.add(tableName);
                    }
                }
            });
        }
        return normallogicalTables;
    }

    public static String getPhysicalTableFromParams(Map<Integer, ParameterContext> parameterContextMap) {
        if (parameterContextMap != null) {
            for (Map.Entry<Integer, ParameterContext> entry : parameterContextMap.entrySet()) {
                ParameterContext parameterContext = entry.getValue();
                if (parameterContext != null && parameterContext.getParameterMethod() == ParameterMethod.setTableName) {
                    String tableName = parameterContext.getValue().toString();
                    if (tableName != null && tableName.startsWith("`") && tableName.endsWith("`")) {
                        return tableName.substring(1, tableName.length() - 1);
                    } else {
                        return tableName;
                    }
                }
            }
        }
        return null;
    }

    public static String getPhysicalTableFromBatchParam(List<Map<Integer, ParameterContext>> batchParameter) {
        if (batchParameter != null) {
            for (Map<Integer, ParameterContext> parameterContextMap : batchParameter) {
                String tableName = getPhysicalTableFromParams(parameterContextMap);
                if (tableName != null) {
                    return tableName;
                }
            }
        }
        return null;
    }

    public static String getMetaDbStorageInstId() {
        Map<String, StorageInstHaContext> storageInstHaCtxCache =
            StorageHaManager.getInstance().refreshAndGetStorageInstHaContextCache();

        for (Map.Entry<String, StorageInstHaContext> haCtxItem : storageInstHaCtxCache.entrySet()) {
            String storageInstId = haCtxItem.getKey();
            StorageInstHaContext ctx = haCtxItem.getValue();
            int instKind = ctx.getStorageKind();
            if (instKind == StorageInfoRecord.INST_KIND_META_DB) {
                return storageInstId;
            } else {
                continue;
            }

        }
        return null;
    }

    public static boolean storageInstIsReady(String storageInstId) {
        Map<String, StorageInstHaContext> storageInstHaCtxCache =
            StorageHaManager.getInstance().refreshAndGetStorageInstHaContextCache();
        StorageInstHaContext ctx = storageInstHaCtxCache.get(storageInstId);
        if (ctx == null || !ctx.isAllReplicaReady()) {
            return false;
        } else {
            return true;
        }
    }

    public static List<String> getStorageInstReady() {
        Map<String, StorageInstHaContext> storageInstHaCtxCache =
            StorageHaManager.getInstance().refreshAndGetStorageInstHaContextCache();
        List<String> storageInstIds = new ArrayList<>();
        for (String storageInstId : storageInstHaCtxCache.keySet()) {
            StorageInstHaContext ctx = storageInstHaCtxCache.get(storageInstId);
            if (ctx == null || !ctx.isAllReplicaReady()) {
            } else if (ctx.isDNMaster()) {
                storageInstIds.add(storageInstId);
            }
        }
        return storageInstIds;
    }

    public static DbGroupInfoRecord getDbGroupInfoByGroupName(String groupName) {
        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
            GroupDetailInfoAccessor detailInfoAccessor = new GroupDetailInfoAccessor();
            detailInfoAccessor.setConnection(metaDbConn);

            DbGroupInfoAccessor dbGroupInfoAccessor = new DbGroupInfoAccessor();
            dbGroupInfoAccessor.setConnection(metaDbConn);
            return dbGroupInfoAccessor
                .getDbGroupInfoByGroupName(groupName);
        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw GeneralUtil.nestedException(ex);
        }
    }

    public static List<DdlEngineRecord> getDdlEngineRecords(String schemaName) {
        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
            DdlEngineAccessor ddlEngineAccessor = new DdlEngineAccessor();
            ddlEngineAccessor.setConnection(metaDbConn);

            return ddlEngineAccessor.query(schemaName);
        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw GeneralUtil.nestedException(ex);
        }
    }

    public static boolean checkStorageIdExistence(String storageId) {
        HaSwitchParams haSwitchParams =
            StorageHaManager.getInstance().getStorageHaSwitchParams(storageId);
        if (haSwitchParams != null) {
            return true;
        }
        return false;
    }

    /**
     * @param sqlMoveDatabase 1、check whether the group is exist
     * 2、remove the duplicate source groupName
     * 3、skip task when the source group storage instance is the same as target storage instance
     */
    public static boolean preCheckAndRemoveDuplicatedKeyForMoveDatabase(SqlMoveDatabase sqlMoveDatabase) {
        Set<String> groupKeysMap = new HashSet<>();
        List<String> sourceGroupKeys;
        Map<String, List<String>> storageGroups = new HashMap<>();
        Map<String, Map<String, List<String>>> logicalDbStorageGroups = new HashMap<>();

        boolean groupExist = false;
        final String metadbStorageInstId = ScaleOutPlanUtil.getMetaDbStorageInstId();
        for (Map.Entry<String, List<String>> entry : sqlMoveDatabase.getStorageGroups().entrySet()) {
            groupKeysMap.clear();
            sourceGroupKeys = new ArrayList<>();
            for (String sourceGroupKey : entry.getValue()) {
                DbGroupInfoRecord dbGroupInfoRecord =
                    ScaleOutPlanUtil.getDbGroupInfoByGroupName(sourceGroupKey);
                if (dbGroupInfoRecord == null) {
                    throw new TddlRuntimeException(ErrorCode.ERR_SCALEOUT_EXECUTE,
                        "the group:" + sourceGroupKey + " is not exists");
                }
                String schemaName = dbGroupInfoRecord.dbName;
                Matrix matrix = OptimizerContext.getContext(schemaName).getMatrix();
                assert matrix != null;
                final String sourceInstId = DbTopologyManager
                    .getStorageInstIdByGroupName(InstIdUtil.getInstId(), schemaName,
                        sourceGroupKey);
                final String targetGroupKey = GroupInfoUtil.buildScaleOutGroupName(sourceGroupKey);
                if (entry.getKey().equalsIgnoreCase(sourceInstId)) {
                    /*SQLRecorderLogger.scaleOutTaskLogger.info(MessageFormat.format(
                        "move database [{0}] to [{1}] is skip due to it is already in [{2}], ts={3} ", sourceGroupKey,
                        entry.getKey(), entry.getKey(),
                        String.valueOf(Instant.now().toEpochMilli())));*/
                    continue;
                }
                if (!groupKeysMap.contains(sourceGroupKey.toUpperCase())) {
                    sourceGroupKeys.add(sourceGroupKey);
                    logicalDbStorageGroups.computeIfAbsent(schemaName, o -> new HashMap<>())
                        .computeIfAbsent(entry.getKey(), o -> new ArrayList<>())
                        .add(sourceGroupKey);
                    groupKeysMap.add(sourceGroupKey.toUpperCase());
                }
                if (GroupInfoUtil.isSingleGroup(sourceGroupKey)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_SCALEOUT_EXECUTE,
                        sourceGroupKey + " is SINGLE_DB, it's not allow to move it");
                }
                if (entry.getKey().equalsIgnoreCase(metadbStorageInstId)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_SCALEOUT_EXECUTE,
                        " it's not allow to move normal database to the storage instance:" + metadbStorageInstId
                            + ", which is only for hosting the metaDb");
                }
                for (Group group : matrix.getGroups()) {
                    if (group.getName().equalsIgnoreCase(sourceGroupKey)) {
                        groupExist = true;
                    }
                }
                if (!groupExist) {
                    throw new TddlRuntimeException(ErrorCode.ERR_SCALEOUT_EXECUTE,
                        "the group:" + sourceGroupKey + " is not exists");
                }
                List<Group> groupNames = OptimizerContext.getContext(schemaName).getMatrix().getScaleOutGroups();
                if (GeneralUtil.isNotEmpty(groupNames)) {
                    for (Group groupName : groupNames) {
                        if (groupName.getName().equalsIgnoreCase(targetGroupKey)) {
                            throw new TddlRuntimeException(ErrorCode.ERR_SCALEOUT_EXECUTE,
                                "the previous move database task for " + sourceGroupKey + " is not cleanup yet");
                        }
                    }
                }
//                //check ongoing ddl in new ddl engine
//                List<DdlEngineRecord> ddlEngineRecords = getDdlEngineRecords(schemaName);
//                if (GeneralUtil.isNotEmpty(ddlEngineRecords)) {
//                    List<DdlEngineRecord> otherDdl =
//                        ddlEngineRecords.stream().filter(
//                            // is not move database or rebalance
//                            o ->
//                                !o.ddlType.equalsIgnoreCase(DdlType.MOVE_DATABASE.name())
//                                    && !o.state.equalsIgnoreCase(DdlState.COMPLETED.toString())
//                                    && !o.ddlType.equalsIgnoreCase(DdlType.REBALANCE.name())
//                        ).collect(
//                            Collectors.toList());
//                    if (GeneralUtil.isNotEmpty(otherDdl)) {
//                        throw new TddlRuntimeException(ErrorCode.ERR_SCALEOUT_EXECUTE,
//                            "it's not allow to execute the move database when other ddl is not finish in schema:"
//                                + schemaName);
//                    }
//                }
            }
            if (!storageInstIsReady(entry.getKey())) {
                throw new TddlRuntimeException(ErrorCode.ERR_SCALEOUT_EXECUTE,
                    entry.getKey() + " is not a valid storage instance id");
            }
            if (sourceGroupKeys.size() > 0) {
                storageGroups.put(entry.getKey(), sourceGroupKeys);
            }
        }
        sqlMoveDatabase.setStorageGroups(storageGroups);
        sqlMoveDatabase.setLogicalDbStorageGroups(logicalDbStorageGroups);
        return sqlMoveDatabase.getMoveGroupCount() == 0;
    }

    public static void checkDDLPermission(ExecutionPlan plan, ExecutionContext context) {
        String schemaName = context.getSchemaName();
        final SqlNode sqlNode = plan.getAst();
        boolean isNewPartDb = DbInfoManager.getInstance().isNewPartitionDb(schemaName);
        if (sqlNode != null && SqlKind.DDL.contains(sqlNode.getKind()) && !isNewPartDb) {
            if (context.getParamManager().getBoolean(ConnectionParams.ALLOW_DROP_DATABASE_IN_SCALEOUT_PHASE)
                && sqlNode instanceof SqlDropDatabase || SqlKind.MOVE_DATABASE == sqlNode.getKind()) {
                return;
            }
            List<ComplexTaskOutlineRecord> unfinishTask =
                ComplexTaskMetaManager.getAllUnFinishParentComlexTask(schemaName);
            if (GeneralUtil.isNotEmpty(unfinishTask)) {
                throw new TddlRuntimeException(ErrorCode.ERR_SCALEOUT_EXECUTE,
                    "it's not allow to run the ddl command when ScaleOut task is in progress");
            }
        }
    }

}

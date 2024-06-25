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

package com.alibaba.polardbx.optimizer.utils;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.hash.XxHash_64Hasher;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.BaseQueryOperation;
import com.alibaba.polardbx.optimizer.core.rel.BaseTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.DirectMultiDBTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.PhyQueryOperation;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.optimizer.partition.PartSpecSearcher;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.google.common.annotations.VisibleForTesting;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlSelect;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.common.jdbc.ITransactionPolicy.TransactionClass.ALLOW_GROUP_PARALLELISM_WITHOUT_SHARE_READVIEW_TRANSACTION;
import static com.alibaba.polardbx.common.jdbc.ITransactionPolicy.TransactionClass.SUPPORT_PARALLEL_GET_CONNECTION_TRANSACTION;
import static com.alibaba.polardbx.common.jdbc.ITransactionPolicy.TransactionClass.SUPPORT_SHARE_READVIEW_TRANSACTION;

public class PhyTableOperationUtil {

    public final static Logger logger = LoggerFactory.getLogger(PhyTableOperationUtil.class);
    public final static Long DEFAULT_WRITE_CONN_ID = 0L;
    private final static XxHash_64Hasher XX_HASH_64_HASHER = new XxHash_64Hasher(0);

    public static void disableIntraGroupParallelism(String schemaName, ExecutionContext ec) {
        controlIntraGroupParallelism(schemaName, ec, false);
    }

    public static void enableIntraGroupParallelism(String schemaName, ExecutionContext ec) {
        controlIntraGroupParallelism(schemaName, ec, true);
    }

    @VisibleForTesting
    protected static void controlIntraGroupParallelism(String schemaName, ExecutionContext ec,
                                                       boolean useGroupParallelism) {
        if (schemaName == null) {
            schemaName = ec.getSchemaName();
        }
        if (!DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
            return;
        }
        ITransaction trans = ec.getTransaction();
        if (trans == null) {
            return;
        }

        if (!trans.getTransactionClass().isA(SUPPORT_PARALLEL_GET_CONNECTION_TRANSACTION)) {
            return;
        }

        if (!ec.isShareReadView()) {
            boolean allowGroupParallelismWithoutShareReadView =
                ec.getParamManager().getBoolean(ConnectionParams.ALLOW_GROUP_PARALLELISM_WITHOUT_SHARE_READVIEW);
            if (allowGroupParallelismWithoutShareReadView) {
                if (trans.getTransactionClass().isA(ALLOW_GROUP_PARALLELISM_WITHOUT_SHARE_READVIEW_TRANSACTION)
                    && ec.isAutoCommit()) {
                    boolean isAutoCommitReadOnlyQuery = OptimizerUtils.isSelectQuery(ec);
                    if (!isAutoCommitReadOnlyQuery) {
                        return;
                    }
                } else {
                    return;
                }
            } else {
                return;
            }
        }

        if (!ec.getExtraCmds().containsKey(ConnectionProperties.ENABLE_GROUP_PARALLELISM)) {
            ec.getExtraCmds().put(ConnectionProperties.ENABLE_GROUP_PARALLELISM, useGroupParallelism);
        }
    }

    //========== methods for compute group conn id, use by JdbcSplit & SplitManger ===========
    public static Long computeGrpConnIdByGrpConnKey(Long connKey, boolean enableGrpParallelism, long grpParallelism) {
        if (!enableGrpParallelism) {
            return DEFAULT_WRITE_CONN_ID;
        }
        return computeGrpConnIdByGrpConnKey(connKey, grpParallelism);
    }

    public static Long computeGrpConnIdByPhyOp(BaseQueryOperation phyOp,
                                               String grpIdx,
                                               List<List<String>> phyTables,
                                               ExecutionContext ec) {
        Boolean enableGrpParallelism = ec.getParamManager().getBoolean(ConnectionParams.ENABLE_GROUP_PARALLELISM);
        if (!enableGrpParallelism) {
            return DEFAULT_WRITE_CONN_ID;
        }
        Long grpParallelism = ec.getGroupParallelism();
        if (grpParallelism == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_TRANS_FETCH_INTRA_GROUP_CONN_ID_FAIL,
                "invalid group parallelism value");
        }
        if (grpParallelism <= 1) {
            return DEFAULT_WRITE_CONN_ID;
        }
        if (!(phyOp instanceof BaseTableOperation)) {
            if (phyOp instanceof PhyQueryOperation) {
                PhyQueryOperation phyQuery = (PhyQueryOperation) phyOp;
                if (phyQuery.isUsingConnKey()) {
                    return computeGrpConnIdByGrpConnKey(phyQuery.getIntraGroupConnKey(), grpParallelism);
                }
            }
            return DEFAULT_WRITE_CONN_ID;
        }
        return computeGrpConnIdByPhyOpInner((BaseTableOperation) phyOp, grpIdx, phyTables, grpParallelism, ec);
    }

    private static Long computeGrpConnIdByGrpConnKey(Long connKey, long grpParallelism) {
        if (grpParallelism <= 1 || connKey == null) {
            return DEFAULT_WRITE_CONN_ID;
        }
        //Long connId = Math.abs(XX_HASH_64_HASHER.hashLong(connKey).asLong() % grpParallelism) + 1;
        Long connId = connKey % grpParallelism + 1;
        return connId;
    }

    private static Long computeGrpConnIdByPhyOpInner(BaseTableOperation phyOp,
                                                     String grpIdx,
                                                     List<List<String>> phyTables,
                                                     Long grpParallelism,
                                                     ExecutionContext ec) {
        if (phyOp instanceof PhyTableOperation) {
            Long connKey = fetchPhyOpIntraGroupConnKey((PhyTableOperation) phyOp, ec);
            return computeGrpConnIdByGrpConnKey(connKey, grpParallelism);
        } else if (phyOp instanceof DirectMultiDBTableOperation) {
            Long connKey = fetchMultiDBIntraGroupConnKey((DirectMultiDBTableOperation) phyOp, grpIdx, ec);
            return computeGrpConnIdByGrpConnKey(connKey, grpParallelism);
        } else {
            Long connKey = fetchNonPhyOpIntraGroupConnKey(phyOp, grpIdx, phyTables, ec);
            return computeGrpConnIdByGrpConnKey(connKey, grpParallelism);
        }
    }

    //========== methods for fetch group conn key, use by JdbcSplit & SplitManger  ===========

    public static Long fetchBaseOpIntraGroupConnKey(BaseQueryOperation phyOp, String grpIdx,
                                                    List<List<String>> phyTables, ExecutionContext ec) {

        Boolean enableGrpParallelism = ec.getParamManager().getBoolean(ConnectionParams.ENABLE_GROUP_PARALLELISM);
        if (!enableGrpParallelism) {
            return null;
        }
        Long grpParallelism = ec.getGroupParallelism();
        if (grpParallelism <= 1) {
            return null;
        }

        if (!(phyOp instanceof BaseTableOperation)) {
            if (phyOp instanceof PhyQueryOperation) {
                if (((PhyQueryOperation) phyOp).isUsingConnKey()) {
                    return fetchPhyQueryIntraGroupConnKey((PhyQueryOperation) phyOp, ec);
                }
            }
            return null;
        } else {
            if (phyOp instanceof PhyTableOperation) {
                return fetchPhyOpIntraGroupConnKey((PhyTableOperation) phyOp, ec);
            } else if (phyOp instanceof DirectMultiDBTableOperation) {
                return fetchMultiDBIntraGroupConnKey((DirectMultiDBTableOperation) phyOp, grpIdx, ec);
            } else {
                return fetchNonPhyOpIntraGroupConnKey((BaseTableOperation) phyOp, grpIdx, phyTables, ec);
            }
        }
    }

    public static Long fetchPhyOpIntraGroupConnKey(PhyTableOperation phyTbOp, ExecutionContext ec) {
        return fetchIntraGroupConnKeyInner(phyTbOp.getKind(), phyTbOp.getSchemaName(), phyTbOp.getLogicalTableNames(),
            phyTbOp.getDbIndex(), phyTbOp.getTableNames(), phyTbOp.getLockMode(), phyTbOp.isReplicateRelNode(), ec);
    }

    private static Long fetchPhyQueryIntraGroupConnKey(PhyQueryOperation phyQueryOp, ExecutionContext ec) {
        return fetchIntraGroupConnKeyInner(phyQueryOp.getKind(), phyQueryOp.getSchemaName(),
            phyQueryOp.getLogicalTables(),
            phyQueryOp.getDbIndex(), phyQueryOp.getPhysicalTables(), phyQueryOp.getLockMode(),
            phyQueryOp.isReplicateRelNode(), ec);
    }

    private static Long fetchIntraGroupConnKeyInner(SqlKind sqlKind,
                                                    String schemaName,
                                                    List<String> logTbs,
                                                    String grpIdx,
                                                    List<List<String>> phyTbs,
                                                    SqlSelect.LockMode lockMode,
                                                    boolean isReplicatedNode,
                                                    ExecutionContext ec) {
        try {
            if (schemaName == null) {
                schemaName = GroupInfoUtil.buildSchemaNameFromGroupName(grpIdx);
            }
            if (!DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
                return null;
            }
            if (!sqlKind.belongsTo(SqlKind.DML) && !sqlKind.belongsTo(SqlKind.QUERY)) {
                return null;
            }
            if (grpIdx == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_TRANS_FETCH_INTRA_GROUP_CONN_ID_FAIL,
                    "invalid group name");
            }
            List<List<String>> tablesList = phyTbs;
            if (logTbs == null || logTbs.isEmpty() || tablesList == null || tablesList.isEmpty()) {
                throw new TddlRuntimeException(ErrorCode.ERR_TRANS_FETCH_INTRA_GROUP_CONN_ID_FAIL,
                    "invalid physical table names");
            }
            PartitionInfoManager partInfoMgr =
                ec.getSchemaManager(schemaName).getTddlRuleManager().getPartitionInfoManager();
            String firstPartLogTb = null;
            Integer firstPartLogTbIdx = 0;
            for (int i = 0; i < logTbs.size(); i++) {
                String logTb = logTbs.get(i);
                if (partInfoMgr.isPartitionedTable(logTb)) {
                    firstPartLogTb = logTb;
                    firstPartLogTbIdx = i;
                    break;
                }
            }
            if (firstPartLogTb == null) {
                // PhyTableOperation has not any partitioned tbl, may is from insert bro or insert sig
                return null;
            }
            String firstPhyTbl = tablesList.get(0).get(firstPartLogTbIdx);
            PartitionInfo partInfo;
            TableMeta tbMeta = ec.getSchemaManager(schemaName).getTable(firstPartLogTb);
            if (isReplicatedNode) {
                partInfo = tbMeta.getNewPartitionInfo();
            } else {
                partInfo = tbMeta.getPartitionInfo();
            }
            Long connKey = tryFetchIntraGroupConnKeyFromPartSpec(grpIdx, firstPhyTbl, partInfo);
            logConnGrpKeyIfNeed(grpIdx, firstPhyTbl, isReplicatedNode, partInfo, tbMeta, connKey, ec);
            return connKey;
        } catch (Throwable ex) {
            logger.warn(ex);
            throw ex;
        }
    }

    // Use by DirectMultiDBTableOperation
    @VisibleForTesting
    protected static Long fetchMultiDBIntraGroupConnKey(
        DirectMultiDBTableOperation phyTbOp, String grpIdx, ExecutionContext ec) {
        try {
            String schemaName = phyTbOp.getBaseSchemaName(ec);
            if (!DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
                return null;
            }
            SqlKind sqlKind = phyTbOp.getKind();
            if (!sqlKind.belongsTo(SqlKind.DML) && !sqlKind.belongsTo(SqlKind.QUERY)) {
                return null;
            }

            ITransaction trans = ec.getTransaction();
            ITransactionPolicy.TransactionClass tranClass = trans.getTransactionClass();
            if (!tranClass.isA(SUPPORT_SHARE_READVIEW_TRANSACTION)) {
                return null;
            }

            List<String> logTbs = phyTbOp.getLogicalTables(schemaName);
            List<String> phyTables = phyTbOp.getPhysicalTables(schemaName);
            String logTb = logTbs.get(0);
            String firstPhyTbl = phyTables.get(0);
            boolean isReplicatedNode = phyTbOp.isReplicateRelNode();
            PartitionInfo partInfo;
            TableMeta tbMeta = ec.getSchemaManager(schemaName).getTable(logTb);
            if (isReplicatedNode) {
                partInfo = tbMeta.getNewPartitionInfo();
            } else {
                partInfo = tbMeta.getPartitionInfo();
            }
            Long connKey = tryFetchIntraGroupConnKeyFromPartSpec(grpIdx, firstPhyTbl, partInfo);
            logConnGrpKeyIfNeed(grpIdx, firstPhyTbl, isReplicatedNode, partInfo, tbMeta, connKey, ec);
            return connKey;
        } catch (Throwable ex) {
            logger.warn(ex);
            throw ex;
        }
    }

    // Use by SingleTableOperation/SingleTableInsert/DirectTableOperation
    @VisibleForTesting
    protected static Long fetchNonPhyOpIntraGroupConnKey(BaseTableOperation phyTbOp, String grpIdx,
                                                         List<List<String>> phyTables, ExecutionContext ec) {
        try {
            String schemaName = phyTbOp.getSchemaName();
            if (!DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
                return null;
            }

            SqlKind sqlKind = phyTbOp.getKind();
            if (!sqlKind.belongsTo(SqlKind.DML) && !sqlKind.belongsTo(SqlKind.QUERY)) {
                return null;
            }

            ITransaction trans = ec.getTransaction();
            ITransactionPolicy.TransactionClass tranClass = trans.getTransactionClass();
            if (!tranClass.isA(SUPPORT_SHARE_READVIEW_TRANSACTION)) {
                return null;
            }

            List<String> logTbs = phyTbOp.getLogicalTableNames();
            if (logTbs.isEmpty()) {
                throw new TddlRuntimeException(ErrorCode.ERR_TRANS_FETCH_INTRA_GROUP_CONN_ID_FAIL,
                    "invalid logical table names");
            }
            String logTb = logTbs.get(0);
            String firstPhyTbl = phyTables.get(0).get(0);
            boolean isReplicatedNode = phyTbOp.isReplicateRelNode();
            PartitionInfo partInfo;
            TableMeta tbMeta = ec.getSchemaManager(schemaName).getTable(logTb);
            if (isReplicatedNode) {
                partInfo = tbMeta.getNewPartitionInfo();
            } else {
                partInfo = tbMeta.getPartitionInfo();
            }
            Long connKey = tryFetchIntraGroupConnKeyFromPartSpec(grpIdx, firstPhyTbl, partInfo);
            logConnGrpKeyIfNeed(grpIdx, firstPhyTbl, isReplicatedNode, partInfo, tbMeta, connKey, ec);
            return connKey;
        } catch (Throwable ex) {
            logger.warn(ex);
            throw ex;
        }
    }

    private static void logConnGrpKeyIfNeed(String grpIdx,
                                            String phyTbl,
                                            boolean isReplicatedNode,
                                            PartitionInfo partInfo,
                                            TableMeta tbMeta,
                                            Long connKey,
                                            ExecutionContext ec) {
        try {
            Boolean enableLogGroupConnKey = ec.getParamManager().getBoolean(ConnectionParams.ENABLE_LOG_GROUP_CONN_KEY);
            if (!enableLogGroupConnKey) {
                return;
            }
            Long grpParallelism = ec.getGroupParallelism();
            StringBuilder sb = new StringBuilder();
            sb.append("grpConnKeyInfo:");
            sb.append("isReplicaOp=").append(isReplicatedNode);
            sb.append(", dbName=").append(tbMeta.getSchemaName());
            sb.append(", tbName=").append(tbMeta.getTableName());
            sb.append(", phyDbGrp=").append(grpIdx);
            sb.append(", phyTb=").append(phyTbl.toLowerCase());
            sb.append(", tbMetaDigest=").append(tbMeta.getDigest());
            PartSpecSearcher specSearcher = partInfo.getPartSpecSearcher();
            PartitionSpec spec = specSearcher.getPartSpec(grpIdx, phyTbl);
            sb.append(", partName=").append(spec.getName());
            sb.append(", grpConnKey=").append(spec.getIntraGroupConnKey());
            sb.append(", grpConnId=").append(computeGrpConnIdByGrpConnKey(connKey, grpParallelism));
            sb.append(", status=").append(tbMeta.getStatus());
            sb.append(", traceId=").append(ec.getTraceId());
            sb.append(", tbMetaAddr=").append(System.identityHashCode(tbMeta));
            sb.append(", partInfoAddr=").append(System.identityHashCode(partInfo));
            sb.append(", searcherAddr=").append(System.identityHashCode(specSearcher));
            sb.append(", partSpecsAddr=")
                .append(System.identityHashCode(partInfo.getPartitionBy().getPhysicalPartitions()));
            logger.warn(sb.toString());
        } catch (Exception ex) {
            logger.warn(ex);
        }
    }

    private static Long tryFetchIntraGroupConnKeyFromPartSpec(String grpIdx,
                                                              String phyTb,
                                                              PartitionInfo partInfo) {
        PartSpecSearcher specSearcher = partInfo.getPartSpecSearcher();
        Long connKey = specSearcher.getPartIntraGroupConnKey(grpIdx, phyTb);
        if (connKey == PartSpecSearcher.NO_FOUND_PART_SPEC) {
            throw new TddlRuntimeException(ErrorCode.ERR_TRANS_FETCH_INTRA_GROUP_CONN_ID_FAIL,
                "invalid group or physical table names");
        }
        if (connKey == PartSpecSearcher.FOUND_NON_PARTITIONED_TBL) {
            return null;
        }
        return connKey;
    }

    //========== methods for regroup targetTables by GroupConnId ===========

    public static TargetTableInfo groupTargetTablesByGroupConnId(String schemaName,
                                                                 List<String> logTblNames,
                                                                 Map<String, List<List<String>>> tmpTargetTables,
                                                                 boolean isFouUpdate,
                                                                 ExecutionContext ec) {

        /**
         * Find first partitioned tbl from logical table list
         */
        String firstPartTb = null;
        SchemaManager schemaManager = ec.getSchemaManager(schemaName);
        PartitionInfoManager partInfoMgr = schemaManager.getTddlRuleManager().getPartitionInfoManager();

        Integer firstPartTbIdx = null;
        for (int i = 0; i < logTblNames.size(); i++) {
            String tmpTbl = logTblNames.get(i);
            if (partInfoMgr.isPartitionedTable(tmpTbl)) {
                firstPartTb = tmpTbl;
                firstPartTbIdx = i;
                break;
            }
        }

        Map<GroupConnId, Map<String, List<List<String>>>> targetTablesGrpByConnId = new HashMap<>();
        Map<String, List<GroupConnId>> grpToConnSetMap = new HashMap<>();
        Long grpParallelism = ec.getGroupParallelism();

        boolean noAnyPartTbl = false;
        boolean useGrpConn = grpParallelism != null && grpParallelism > 1;
        if (firstPartTb == null) {
            /**
             * All log tables are broadcast tbl
             */
            noAnyPartTbl = true;
        }

        /**
         * Find all the Group ConnId by tmpTargetTables
         */
        for (Map.Entry<String, List<List<String>>> tmpTargetTablesItem : tmpTargetTables.entrySet()) {
            String grpKey = tmpTargetTablesItem.getKey();
            List<GroupConnId> connIdSetOfOneGrp = grpToConnSetMap.computeIfAbsent(grpKey, key -> new ArrayList<>());
            if (noAnyPartTbl || !useGrpConn) {
                GroupConnId groupConnId = new GroupConnId(grpKey, DEFAULT_WRITE_CONN_ID);
                Map<String, List<List<String>>> newTargetTables = new HashMap<>();
                newTargetTables.put(grpKey, tmpTargetTablesItem.getValue());
                targetTablesGrpByConnId.put(groupConnId, newTargetTables);
                connIdSetOfOneGrp.add(groupConnId);
            } else {
                List<List<String>> phyTblInfos = tmpTargetTablesItem.getValue();
                for (int i = 0; i < phyTblInfos.size(); i++) {
                    List<String> phyTblsOfPartGrp = phyTblInfos.get(i);
                    String phyTblOfFirstPartTblIdx = phyTblsOfPartGrp.get(firstPartTbIdx);
                    TableMeta tbMeta = schemaManager.getTable(firstPartTb);
                    PartitionInfo partInfo = tbMeta.getPartitionInfo();
                    Long grpConnKey =
                        tryFetchIntraGroupConnKeyFromPartSpec(grpKey, phyTblOfFirstPartTblIdx, partInfo);
                    logConnGrpKeyIfNeed(grpKey, phyTblOfFirstPartTblIdx, false, partInfo, tbMeta, grpConnKey, ec);
                    Long grpConnId = computeGrpConnIdByGrpConnKey(grpConnKey, grpParallelism);
                    GroupConnId groupConnId = new GroupConnId(grpKey, grpConnId);
                    Map<String, List<List<String>>> targetTablesOfSameGrp =
                        targetTablesGrpByConnId.computeIfAbsent(groupConnId, id -> new HashMap<>());
                    List<List<String>> targetTablesOfSameGrpConn =
                        targetTablesOfSameGrp.computeIfAbsent(grpKey, g -> new ArrayList<List<String>>());
                    targetTablesOfSameGrpConn.add(phyTblsOfPartGrp);
                    connIdSetOfOneGrp.add(groupConnId);
                }
            }
        }
        TargetTableInfo targetTableInfo = new TargetTableInfo();
        targetTableInfo.setGrpConnIdTargetTablesMap(targetTablesGrpByConnId);
        targetTableInfo.setGrpToConnSetMap(grpToConnSetMap);
        return targetTableInfo;
    }

    public static String buildGroConnIdStr(PhyTableOperation op, ExecutionContext ec) {
        boolean enableGrpParallelism = ec.getParamManager().getBoolean(ConnectionParams.ENABLE_GROUP_PARALLELISM);
        long grpParallelism = ec.getGroupParallelism();
        if (!enableGrpParallelism || grpParallelism <= 1) {
            return op.getDbIndex();
        }
        Long grpConnId = PhyTableOperationUtil.computeGrpConnIdByPhyOp(op, null, null, ec);
        GroupConnId grpConn = new GroupConnId(op.getDbIndex(), grpConnId);
        return grpConn.toString();
    }

    public static List<String> buildGroConnSetFromGroups(ExecutionContext ec, List<String> groupNames) {
        boolean enableGrpParallelism = ec.getParamManager().getBoolean(ConnectionParams.ENABLE_GROUP_PARALLELISM);
        long grpParallelism = ec.getGroupParallelism();
        if (!enableGrpParallelism || grpParallelism <= 1) {
            return groupNames;
        }
        List<String> allGrpConnStrSet = new ArrayList<>();
        for (int i = 0; i < groupNames.size(); i++) {
            for (int j = 0; j <= grpParallelism; j++) {
                GroupConnId grpConnId = new GroupConnId(groupNames.get(i), Long.valueOf(j));
                allGrpConnStrSet.add(grpConnId.toString());
            }
        }
        return allGrpConnStrSet;
    }

    //public static int calc
}

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

package com.alibaba.polardbx.optimizer.partition;

import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.partition.TablePartRecordInfoContext;
import com.alibaba.polardbx.gms.partition.TablePartitionAccessor;
import com.alibaba.polardbx.gms.partition.TablePartitionConfig;
import com.alibaba.polardbx.gms.partition.TablePartitionConfigUtil;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupUtils;
import com.alibaba.polardbx.gms.util.MetaDbLogUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.partition.common.PartitionLocation;
import com.alibaba.polardbx.optimizer.partition.common.PartitionTableType;
import com.alibaba.polardbx.optimizer.partition.pruning.PhysicalPartitionInfo;
import com.alibaba.polardbx.optimizer.partition.util.TableMetaFetcher;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import lombok.val;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

/**
 * Handle and manage all the changes for all partition information
 *
 * @author chenghui.lch
 */
public class PartitionInfoManager extends AbstractLifecycle {

    private final static Logger logger = LoggerFactory.getLogger(PartitionInfoManager.class);

    /**
     * the schema of parttion infos
     */
    protected String schemaName;

    /**
     * the appName of schemaName
     */
    protected String appName;

    /**
     * <pre>
     *  key: logical table name
     *  val: the partition info of logical table name
     * <pre/>
     */
    protected Map<String, PartInfoCtx> partInfoCtxCache;

    /**
     * Partition config accessor
     */
    protected TablePartitionAccessor tablePartitionAccessor;

    /**
     * The fetcher to fetch the table column meta from metadb
     */
    protected TableMetaFetcher tableMetaFetcher;

    /**
     * Label if the startup with mock mode
     */
    protected boolean isMock = false;

    /**
     * The default phy db for broadcast tbl
     */
    protected String defaultDbIndex = null;

    protected TddlRuleManager rule;

    public String getDefaultDbIndex() {
        return defaultDbIndex;
    }

    public PartitionInfoManager(String schemaName, String appName) {
        this(schemaName, appName, false);
    }

    public PartitionInfoManager(String schemaName, String appName, boolean isMock) {
        this.schemaName = schemaName;
        this.appName = appName;
        this.partInfoCtxCache = new ConcurrentSkipListMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        this.tablePartitionAccessor = new TablePartitionAccessor();
        this.isMock = isMock;
    }

    @Override
    protected void doInit() {
        super.doInit();

        if (isMock) {
            return;
        }

        loadAllPartInfoCtx();
        this.defaultDbIndex = TableInfoManager.getSchemaDefaultDbIndex(schemaName);
    }

    @Override
    protected void doDestroy() {
        super.doDestroy();
        this.partInfoCtxCache.clear();
    }

    public Set<String> getPartitionTables() {
        Set<String> tableNames = new TreeSet<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        tableNames.addAll(partInfoCtxCache.keySet());
        return tableNames;
    }

    /**
     * 注意：这里拿到的都是最新的 partition info，没有双版本。除非真的需要拿最新的，还是建议从 TableMeta 中获取 partition info
     */
    public PartitionInfo getPartitionInfo(String tbName) {
        if (!StringUtils.isEmpty(tbName)) {
            tbName = tbName.toLowerCase();
        }
        PartInfoCtx ctx = partInfoCtxCache.get(tbName);
        if (ctx == null) {
            return null;
        }
        return ctx.getPartInfo();
    }

    public List<PartitionInfo> getPartitionInfos() {
        return this.partInfoCtxCache.values().stream().map(x -> x.getPartInfo()).collect(Collectors.toList());
    }

    /**
     * New PartDb Table include partTbl/singleTbl/broadcastTbl of new part db
     */
    public boolean isNewPartDbTable(String tbName) {
        return partInfoCtxCache.containsKey(tbName);
    }

    public boolean isPartitionedTable(String tbName) {
        PartInfoCtx partInfoCtx = partInfoCtxCache.get(tbName);
        if (partInfoCtx == null) {
            return false;
        }
        return partInfoCtx.getPartInfo().getTableType() == PartitionTableType.PARTITION_TABLE
            || partInfoCtx.getPartInfo().getTableType() == PartitionTableType.GSI_TABLE;
    }

    public boolean isBroadcastTable(String tbName) {
        PartInfoCtx partInfoCtx = partInfoCtxCache.get(tbName);
        if (partInfoCtx == null) {
            return false;
        }
        return partInfoCtx.getPartInfo().getTableType() == PartitionTableType.BROADCAST_TABLE
            || partInfoCtx.getPartInfo().getTableType() == PartitionTableType.GSI_BROADCAST_TABLE;
    }

    public boolean isSingleTable(String tbName) {
        PartInfoCtx partInfoCtx = partInfoCtxCache.get(tbName);
        if (partInfoCtx == null) {
            return false;
        }
        return partInfoCtx.getPartInfo().getTableType() == PartitionTableType.SINGLE_TABLE
            || partInfoCtx.getPartInfo().getTableType() == PartitionTableType.GSI_SINGLE_TABLE;
    }

    public PhysicalPartitionInfo getFirstPhysicalPartition(String tbName) {
        PartInfoCtx partInfoCtx = partInfoCtxCache.get(tbName);
        if (partInfoCtx == null) {
            return null;
        }

        PartitionInfo partInfo = partInfoCtx.getPartInfo();
        PartitionSpec part = partInfo.getPartitionBy().getPhysicalPartitions().get(0);
        PartitionLocation location = part.getLocation();
        PhysicalPartitionInfo prunedPartInfo = new PhysicalPartitionInfo();
        prunedPartInfo.setGroupKey(location.getGroupKey());
        prunedPartInfo.setPhyTable(location.getPhyTableName());
        prunedPartInfo.setPartBitSetIdx(0);
        prunedPartInfo.setPartId(part.getId());
        prunedPartInfo.setPartLevel(part.getPartLevel());
        prunedPartInfo.setPartName(part.getName());
        return prunedPartInfo;
    }

    /**
     * Batch load all tables in a tablegroup
     * NOTE: tableNames must in the same tablegroup
     */
    public synchronized void reloadPartitionInfoTrx(String schemaName, List<String> tableNames) {

        Map<String, Pair<TablePartitionConfig, Map<Long, PartitionGroupRecord>>> tableConfigs = new HashMap<>();

        // Load all configs from metadb in a transaction
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            Map<String, TablePartitionConfig> tmpTableConfigs =
                TablePartitionConfigUtil.getPublicTablePartitionConfigs(schemaName, tableNames, false);
            for (TablePartitionConfig tableConfig : tmpTableConfigs.values()) {
                Map<Long, PartitionGroupRecord> pgMap =
                    TableGroupUtils.getPartitionGroupsMapByGroupId(conn, tableConfig.getTableConfig().getGroupId());

                tableConfigs.put(tableConfig.getTableConfig().getTableName(), Pair.of(tableConfig, pgMap));
            }
        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw GeneralUtil.nestedException(ex);
        }

        if (GeneralUtil.isEmpty(tableConfigs)) {
            for (String tableName : tableNames) {
                invalidatePartitionInfo(schemaName, tableName);
            }
            return;
        }
        for (val entry : tableConfigs.entrySet()) {
            String tableName = entry.getKey();
            TablePartitionConfig tablePartitionConfig = entry.getValue().getKey();
            Map<Long, PartitionGroupRecord> pgMap = entry.getValue().getValue();
            long tableGroupId = tablePartitionConfig.getTableConfig().getGroupId();
            PartInfoCtx partCtx = this.partInfoCtxCache.computeIfAbsent(tableName,
                (k) -> new PartInfoCtx(this, k, tableGroupId));
            partCtx.rebuild(tablePartitionConfig, pgMap);
            // don't add the invisible table to tablegroup manager
            if (partCtx.partInfo != null) {
                rule.getTableGroupInfoManager()
                    .reloadTableGroupByGroupIdAndTableName(tableGroupId, schemaName, tableName);
            }
            Long oldTableGrpId = partCtx.getTableGroupId();
            if (!Objects.equals(oldTableGrpId, tableGroupId)) {
                rule.getTableGroupInfoManager().invalidate(oldTableGrpId, tableName);
            }
        }
    }

    public synchronized void reloadPartitionInfo(String schemaName, String tbName) {
        reloadPartitionInfo(null, schemaName, tbName);
    }

    public synchronized void reloadPartitionInfo(Connection conn, String schemaName, String tbName) {

        if (!StringUtils.isEmpty(tbName)) {
            tbName = tbName.toLowerCase();
        }

        TablePartitionConfig tbPartConf =
            TablePartitionConfigUtil.getPublicTablePartitionConfig(conn, schemaName, tbName, false);
        if (tbPartConf == null) {
            invalidatePartitionInfo(schemaName, tbName);
            return;
        }

        Long tableGrpId = tbPartConf.getTableConfig().groupId;
        PartInfoCtx partCtx = this.partInfoCtxCache.computeIfAbsent(tbName,
            (name) -> new PartInfoCtx(this, name, tableGrpId));
        Long oldTableGrpId = partCtx.getTableGroupId();
        partCtx.reload(conn, false);
        // don't add the invisible table to tablegroup manager
        if (partCtx.partInfo != null) {
            rule.getTableGroupInfoManager().reloadTableGroupByGroupIdAndTableName(conn, tableGrpId, schemaName, tbName);
        }
        // remove tables in old table group
        if (!Objects.equals(oldTableGrpId, tableGrpId)) {
            rule.getTableGroupInfoManager().invalidate(oldTableGrpId, tbName);
        }
    }

    public synchronized void reloadPartitionInfo(String schemaName, String tbName,
                                                 TableMeta tableMeta,
                                                 List<TablePartitionRecord> tablePartitionRecords,
                                                 List<TablePartitionRecord> tablePartitionRecordsFromDelta) {

        if (!StringUtils.isEmpty(tbName)) {
            tbName = tbName.toLowerCase();
        }

        Optional<TablePartitionRecord> tablePartitionRecord = tablePartitionRecords.stream()
            .filter(o -> (o.partLevel == TablePartitionRecord.PARTITION_LEVEL_LOGICAL_TABLE
                && o.partStatus == TablePartitionRecord.PARTITION_STATUS_LOGICAL_TABLE_PUBLIC)).findFirst();
        if (!tablePartitionRecord.isPresent()) {
            invalidatePartitionInfo(schemaName, tbName);
            return;
        }

        Long tableGrpId = tablePartitionRecord.get().groupId;
        PartInfoCtx partCtx = this.partInfoCtxCache.computeIfAbsent(tbName,
            (name) -> new PartInfoCtx(this, name, tableGrpId));
        partCtx.reload(tableMeta, tablePartitionRecords, tablePartitionRecordsFromDelta, false);
        // don't add the invisible table to tablegroup manager
        if (partCtx.partInfo != null) {
            rule.getTableGroupInfoManager().reloadTableGroupByGroupIdAndTableName(tableGrpId, schemaName, tbName);
        }
    }

    protected void loadAllPartInfoCtx() {
        List<TablePartitionConfig> tbPartConfList =
            TablePartitionConfigUtil.getAllTablePartitionConfigs(this.schemaName);
        for (int i = 0; i < tbPartConfList.size(); i++) {
            TablePartitionConfig conf = tbPartConfList.get(i);
            if (conf.getTableConfig().partStatus != TablePartitionRecord.PARTITION_STATUS_LOGICAL_TABLE_PUBLIC) {
                continue;
            }
            String tbName = conf.getTableConfig().tableName.toLowerCase();
            PartInfoCtx partCtx = new PartInfoCtx(this, tbName, conf.getTableConfig().groupId);
            this.partInfoCtxCache.put(tbName, partCtx);
        }
    }

    protected synchronized void invalidatePartitionInfo(String schemaName, String tbName) {
        if (!StringUtils.isEmpty(tbName)) {
            tbName = tbName.toLowerCase();
        }
        PartInfoCtx partInfoCtx = this.partInfoCtxCache.get(tbName);
        if (partInfoCtx != null) {
            rule.getTableGroupInfoManager().invalidate(partInfoCtx.getTableGroupId(), tbName);
        }
        this.partInfoCtxCache.remove(tbName);
    }

    public String getSchemaName() {
        return schemaName;
    }

    public static void reload(String schemaName, String tableName) {
        OptimizerContext.getContext(schemaName).getPartitionInfoManager().reloadPartitionInfo(schemaName, tableName);
    }

    public static void invalidate(String schemaName, String tableName) {
        OptimizerContext.getContext(schemaName).getPartitionInfoManager()
            .invalidatePartitionInfo(schemaName, tableName);
    }

    public void setTableMetaFetcher(TableMetaFetcher tableMetaFetcher) {
        this.tableMetaFetcher = tableMetaFetcher;
    }

    public PartitionInfo getPartitionInfoFromDeltaTable(String tbName) {
        return getPartitionInfoFromDeltaTable(null, tbName);
    }

    public PartitionInfo getPartitionInfoFromDeltaTable(Connection conn, String tbName) {

        if (!StringUtils.isEmpty(tbName)) {
            tbName = tbName.toLowerCase();
        }

        TablePartitionConfig tbPartConf = TablePartitionConfigUtil.getTablePartitionConfig(conn, schemaName, tbName,
            true);
        if (tbPartConf == null) {
            return null;
        }

        Long tableGrpId = tbPartConf.getTableConfig().groupId;
        PartInfoCtx partCtx = new PartInfoCtx(this, tbName, tableGrpId);

        partCtx.reload(conn, true);
        return partCtx.partInfo;
    }

    public TddlRuleManager getRule() {
        return rule;
    }

    public void setRule(TddlRuleManager rule) {
        this.rule = rule;
    }

    public void putPartInfoCtx(String tableName, PartInfoCtx partInfoCtx) {
        partInfoCtxCache.put(tableName, partInfoCtx);
    }

    public void putNewPartitionInfo(String tableName, PartitionInfo partitionInfo) {
        partInfoCtxCache.put(tableName, new PartitionInfoManager.PartInfoCtx(this,
            tableName.toLowerCase(),
            partitionInfo.getTableGroupId(),
            partitionInfo));
    }

    /**
     * The partition info loading context
     */
    public static class PartInfoCtx {
        protected PartitionInfoManager manager;
        protected String tbName;
        protected volatile Long tableGroupId;
        protected volatile PartitionInfo partInfo;
        protected volatile boolean includeNonPublic;

        public PartInfoCtx(PartitionInfoManager manager, String tbName, Long tableGroupId,
                           PartitionInfo partitionInfo) {
            this.manager = manager;
            this.tbName = tbName;
            this.tableGroupId = tableGroupId;
            this.partInfo = partitionInfo;
        }

        public PartInfoCtx(PartitionInfoManager manager, String tbName, Long tableGroupId) {
            this.manager = manager;
            this.tbName = tbName;
            this.tableGroupId = tableGroupId;
        }

        public Long getTableGroupId() {
            return tableGroupId;
        }

        public PartitionInfo getPartInfo() {
            if (partInfo == null) {
                synchronized (this) {
                    if (partInfo == null) {
                        reload(false);
                    }
                }
            }
            return partInfo;
        }

        /**
         * Rebuild partition info from table config and pg config
         */
        public PartitionInfo rebuild(TablePartitionConfig tbPartConf, Map<Long, PartitionGroupRecord> pgMap) {
            loadPartInfo(tbPartConf, pgMap);
            return this.partInfo;
        }

        public PartitionInfo reload(boolean fromDeltaTable) {
            TablePartitionConfig tbPartConf =
                TablePartitionConfigUtil.getTablePartitionConfig(manager.schemaName, tbName, fromDeltaTable);
            Map<Long, PartitionGroupRecord> pgMap =
                TableGroupUtils.getPartitionGroupsMapByGroupId(tbPartConf.getTableConfig().getGroupId());
            loadPartInfo(tbPartConf, pgMap);
            return partInfo;
        }

        public PartitionInfo reload(Connection conn, boolean fromDeltaTable) {
            TablePartitionConfig tbPartConf =
                TablePartitionConfigUtil.getTablePartitionConfig(conn, manager.schemaName, tbName, fromDeltaTable);
            Map<Long, PartitionGroupRecord> pgMap =
                TableGroupUtils.getPartitionGroupsMapByGroupId(conn, tbPartConf.getTableConfig().getGroupId());

            loadPartInfo(tbPartConf, pgMap);
            return partInfo;
        }

        public PartitionInfo reload(TableMeta tableMeta, List<TablePartitionRecord> tablePartitionRecords,
                                    List<TablePartitionRecord> tablePartitionRecordsFromDelta,
                                    boolean fromDeltaTable) {
            loadPartInfo(tableMeta, tablePartitionRecords, tablePartitionRecordsFromDelta, fromDeltaTable);
            return partInfo;
        }

        public void setPartInfo(PartitionInfo partInfo) {
            this.partInfo = partInfo;
        }

        public String getTbName() {
            return tbName;
        }

        public void setTbName(String tbName) {
            this.tbName = tbName;
        }

        public boolean isIncludeNonPublic() {
            return includeNonPublic;
        }

        public void setIncludeNonPublic(boolean includeNonPublic) {
            this.includeNonPublic = includeNonPublic;
        }

        private synchronized void loadPartInfo(TablePartitionConfig tbPartConf,
                                               Map<Long, PartitionGroupRecord> pgMap) {
            try {
                if (tbPartConf.getTableConfig().partStatus != TablePartitionRecord.PARTITION_STATUS_LOGICAL_TABLE_PUBLIC
                    && !includeNonPublic) {
                    return;
                }
                List<ColumnMeta> allColumnMetas = null;
                if (manager.tableMetaFetcher != null) {
                    try {
                        TableMeta meta =
                            manager.tableMetaFetcher.getTableMeta(manager.schemaName, manager.appName, tbName);
                        if (meta != null) {
                            allColumnMetas = meta.getPhysicalColumns();
                        }
                    } catch (Throwable ex) {
                        // do log here
                        logger.error(ex);
                        throw GeneralUtil.nestedException("Failed to fetch the partition table meta from meta db", ex);
                    }
                }
                PartitionInfo partInfo =
                    PartitionInfoBuilder.buildPartitionInfoByMetaDbConfig(tbPartConf, allColumnMetas,
                        pgMap);
                this.partInfo = partInfo;
                this.tableGroupId = partInfo.getTableGroupId();
                return;
            } catch (Throwable ex) {
                String msg =
                    String.format("Failed to load partitionInfo[%s.%s] from metadb", manager.schemaName, tbName);
                logger.error(msg, ex);
                throw GeneralUtil.nestedException(msg, ex);
            }

        }

        private synchronized void loadPartInfo(TableMeta tableMeta, List<TablePartitionRecord> tablePartitionRecords,
                                               List<TablePartitionRecord> tablePartitionRecordsFromDelta,
                                               boolean fromDeltaTable) {
            try {

                TablePartitionConfig tbPartConf = null;

                // Fetch logical config
                TablePartitionRecord logTbRec = fromDeltaTable ? tablePartitionRecordsFromDelta.stream()
                    .filter(o -> o.partLevel == TablePartitionRecord.PARTITION_LEVEL_LOGICAL_TABLE).findFirst().get() :
                    tablePartitionRecords.stream()
                        .filter(o -> o.partLevel == TablePartitionRecord.PARTITION_LEVEL_LOGICAL_TABLE).findFirst()
                        .get();

                // Fetch partition configs
                List<TablePartitionRecord> partitionRecList = fromDeltaTable ? tablePartitionRecordsFromDelta.stream()
                    .filter(o -> o.partLevel == TablePartitionRecord.PARTITION_LEVEL_PARTITION)
                    .collect(Collectors.toList()) :
                    tablePartitionRecords.stream()
                        .filter(o -> o.partLevel == TablePartitionRecord.PARTITION_LEVEL_PARTITION)
                        .collect(Collectors.toList());

                // Fetch subpartition configs
                List<TablePartitionRecord> subPartitionRecList =
                    fromDeltaTable ? tablePartitionRecordsFromDelta.stream()
                        .filter(o -> o.partLevel == TablePartitionRecord.PARTITION_LEVEL_SUBPARTITION)
                        .collect(Collectors.toList()) :
                        tablePartitionRecords.stream()
                            .filter(o -> o.partLevel == TablePartitionRecord.PARTITION_LEVEL_SUBPARTITION)
                            .collect(Collectors.toList());

                TablePartRecordInfoContext confCtx = new TablePartRecordInfoContext();
                confCtx.setLogTbRec(logTbRec);
                confCtx.setPartitionRecList(partitionRecList);
                confCtx.setSubPartitionRecList(subPartitionRecList);
                tbPartConf = TablePartitionAccessor.buildPartitionConfByPartitionRecords(confCtx);

                if (tbPartConf.getTableConfig().partStatus
                    != TablePartitionRecord.PARTITION_STATUS_LOGICAL_TABLE_PUBLIC && !includeNonPublic) {
                    return;
                }
                List<ColumnMeta> allColumnMetas = tableMeta.getPhysicalColumns();
                Map<Long, PartitionGroupRecord> partitionGroupRecordsMap =
                    TableGroupUtils.getPartitionGroupsMapByGroupId(tbPartConf.getTableConfig().getGroupId());
                PartitionInfo partInfo =
                    PartitionInfoBuilder.buildPartitionInfoByMetaDbConfig(tbPartConf, allColumnMetas,
                        partitionGroupRecordsMap);
                this.partInfo = partInfo;
                this.tableGroupId = partInfo.getTableGroupId();
                return;
            } catch (Throwable ex) {
                String msg =
                    String.format("Failed to load partitionInfo[%s.%s] from metadb", manager.schemaName, tbName);
                logger.error(msg, ex);
                throw GeneralUtil.nestedException(msg, ex);
            }

        }
    }

    public String getAppName() {
        return appName;
    }

    public PartInfoCtx getPartInfoCtx(String table) {
        if (partInfoCtxCache.containsKey(table)) {
            return partInfoCtxCache.get(table);
        }
        return null;
    }

    public Map<String, PartInfoCtx> getPartInfoCtxCache() {
        return partInfoCtxCache;
    }
}

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

import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.partition.TablePartitionAccessor;
import com.alibaba.polardbx.gms.partition.TablePartitionConfig;
import com.alibaba.polardbx.gms.partition.TablePartitionConfigUtil;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.partition.pruning.PartKeyLevel;
import com.alibaba.polardbx.optimizer.partition.pruning.PhysicalPartitionInfo;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
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

    /**
     * The partition info loading context
     */
    public static class PartInfoCtx {
        protected PartitionInfoManager manager;
        protected String tbName;
        protected volatile Long partGrpId;
        protected volatile PartitionInfo partInfo;
        protected volatile boolean includeNonPublic;

        public Long getPartGrpId() {
            return partGrpId;
        }

        public PartInfoCtx(PartitionInfoManager manager, String tbName, Long partGrpId, PartitionInfo partitionInfo) {
            this.manager = manager;
            this.tbName = tbName;
            this.partGrpId = partGrpId;
            this.partInfo = partitionInfo;
        }

        public PartInfoCtx(PartitionInfoManager manager, String tbName, Long partGrpId) {
            this.manager = manager;
            this.tbName = tbName;
            this.partGrpId = partGrpId;
        }

        public PartitionInfo getPartInfo() {
            if (partInfo == null) {
                synchronized (this) {
                    if (partInfo == null) {
                        loadPartInfo(false);
                    }
                }
            }
            return partInfo;
        }

        public PartitionInfo reload(boolean fromDeltaTable) {
            loadPartInfo(fromDeltaTable);
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

        private synchronized void loadPartInfo(boolean fromDeltaTable) {
            try {
                TablePartitionConfig tbPartConf =
                    TablePartitionConfigUtil.getTablePartitionConfig(manager.schemaName, tbName, fromDeltaTable);
                if (tbPartConf.getTableConfig().partStatus
                    != TablePartitionRecord.PARTITION_STATUS_LOGICAL_TABLE_PUBLIC && !includeNonPublic) {
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
                    PartitionInfoBuilder.buildPartitionInfoByMetaDbConfig(tbPartConf, allColumnMetas);
                this.partInfo = partInfo;
                this.partGrpId = partInfo.getTableGroupId();
                return;
            } catch (Throwable ex) {
                String msg =
                    String.format("Failed to load partitionInfo[%s.%s] from metadb", manager.schemaName, tbName);
                logger.error(msg, ex);
                throw GeneralUtil.nestedException(msg, ex);
            }

        }
    }

    public PartitionInfoManager(String schemaName, String appName) {
        this(schemaName, appName, false);
    }

    public PartitionInfoManager(String schemaName, String appName, boolean isMock) {
        this.schemaName = schemaName;
        this.appName = appName;
        this.partInfoCtxCache = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
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
        return partInfoCtx.getPartInfo().getTableType() == PartitionTableType.BROADCAST_TABLE;
    }

    public boolean isSingleTable(String tbName) {
        PartInfoCtx partInfoCtx = partInfoCtxCache.get(tbName);
        if (partInfoCtx == null) {
            return false;
        }
        return partInfoCtx.getPartInfo().getTableType() == PartitionTableType.SINGLE_TABLE;
    }

    public PhysicalPartitionInfo getFirstPhysicalPartition(String tbName) {
        PartInfoCtx partInfoCtx = partInfoCtxCache.get(tbName);
        if (partInfoCtx == null) {
            return null;
        }

        PartitionInfo partInfo = partInfoCtx.getPartInfo();

        if (partInfo.getSubPartitionBy() == null) {
            PartitionSpec part = partInfo.getPartitionBy().getPartitions().get(0);
            PartitionLocation location = part.getLocation();
            PhysicalPartitionInfo prunedPartInfo = new PhysicalPartitionInfo();
            prunedPartInfo.setGroupKey(location.getGroupKey());
            prunedPartInfo.setPhyTable(location.getPhyTableName());
            prunedPartInfo.setPartBitSetIdx(0);
            prunedPartInfo.setPartId(part.getId());
            prunedPartInfo.setPartLevel(PartKeyLevel.PARTITION_KEY);
            prunedPartInfo.setPartName(part.getName());
            return prunedPartInfo;
        } else {
            throw GeneralUtil.nestedException(new NotSupportException());
        }
    }

    public synchronized void reloadPartitionInfo(String schemaName, String tbName) {

        if (!StringUtils.isEmpty(tbName)) {
            tbName = tbName.toLowerCase();
        }

        TablePartitionConfig tbPartConf =
            TablePartitionConfigUtil.getPublicTablePartitionConfig(schemaName, tbName, false);
        if (tbPartConf == null) {
            invalidatePartitionInfo(schemaName, tbName);
            return;
        }

        PartInfoCtx partCtx = this.partInfoCtxCache.get(tbName);
        Long tableGrpId = tbPartConf.getTableConfig().groupId;
        if (partCtx == null) {
            partCtx = new PartInfoCtx(this, tbName, tableGrpId);
            this.partInfoCtxCache.put(tbName, partCtx);
        }
        partCtx.reload(false);
        rule.getTableGroupInfoManager().reloadTableGroupByGroupIdAndTableName(tableGrpId, schemaName, tbName);
        return;
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
            rule.getTableGroupInfoManager().reloadTableGroupByGroupId(partInfoCtx.getPartGrpId());
        }
        this.partInfoCtxCache.remove(tbName);
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

        if (!StringUtils.isEmpty(tbName)) {
            tbName = tbName.toLowerCase();
        }

        TablePartitionConfig tbPartConf = TablePartitionConfigUtil.getTablePartitionConfig(schemaName, tbName, true);
        if (tbPartConf == null) {
            return null;
        }

        Long tableGrpId = tbPartConf.getTableConfig().groupId;
        PartInfoCtx partCtx = new PartInfoCtx(this, tbName, tableGrpId);

        partCtx.reload(true);
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

    public String getSchemaName() {
        return schemaName;
    }

    public String getAppName() {
        return appName;
    }

    public Map<String, PartInfoCtx> getPartInfoCtxCache() {
        return partInfoCtxCache;
    }
}

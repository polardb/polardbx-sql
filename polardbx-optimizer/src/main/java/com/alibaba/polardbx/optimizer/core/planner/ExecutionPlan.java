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

package com.alibaba.polardbx.optimizer.core.planner;

import com.alibaba.polardbx.common.privilege.PrivilegeVerifyItem;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.core.profiler.memory.PlanMemEstimation;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.sharding.result.PlanShardInfo;
import com.google.common.collect.Maps;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlReplace;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlShow;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.util.BitSets;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author lingce.ldm 2017-09-08 15:23
 */
public class ExecutionPlan {

    public Pair<String, String> getDbIndexAndTableName() {
        return dbIndexAndTableName;
    }

    public void setDbIndexAndTableName(
        Pair<String, String> dbIndexAndTableName) {
        this.dbIndexAndTableName = dbIndexAndTableName;
    }

    public enum DirectMode {
        // 访问的表都是在同一个库的单表的转发
        TABLE_DIRECT,
        // 根据分片键点查的转发
        SHARDING_KEY_DIRECT,
        NONE;

        public boolean isDirect() {
            return this == TABLE_DIRECT || this == SHARDING_KEY_DIRECT;
        }
    }

    private RelNode plan;
    private final CursorMeta cursorMeta;
    private final SqlNode ast;
    private boolean hitCache;
    private boolean isExplain;
    private BitSet planProperties = new BitSet();
    private List<PrivilegeVerifyItem> privilegeVerifyItems;
    private List<String> originTableNames = new ArrayList<>();
    private List<RelUtils.TableProperties> modifiedTables;
    private boolean usePostPlanner = true;
    private Set<Pair<String, String>> tableSet;
    // Collect the snapshot of table metas to print table status in scale out sql log
    /* key: 'dbName.tbName.toLowwercase', val: tableMeta */
    private Map<String, TableMeta> tableMetaSnapshots = new TreeMap<String, TableMeta>();
    private int tableSetHashCode;
    private PlanCache.CacheKey cacheKey;
    private AtomicInteger errorCount = new AtomicInteger(0);

    private AtomicInteger htapFeedCount = new AtomicInteger(0);
    private AtomicLong hitCount = new AtomicLong(0);
    /**
     * Key: Logical Dml relNodeId
     * Val: flag that label if the Logical Dml relNode can be directed do mirror write in scale out
     */
    private Map<Integer, Boolean> dmlRelScaleOutWriteFlags;
    private boolean needReBuildScaleOutPlan = false;

    /**
     * all the schema name that is referenced by plan
     */
    private Set<String> schemaNames;

    /**
     * all shard info of plan
     */
    private Map<String, PlanShardInfo> planShardInfo = Maps.newConcurrentMap();

    // the cache mem pool of current plan
    private MemoryPool planMemCachePool = null;
    // the mem estimation of plan
    private PlanMemEstimation planMemEstimation = null;

    /**
     * 是否为分片键点查
     */
    private boolean isDirectShardingKey;
    /**
     * For DirectShardingKeyTableOperation
     */
    private Pair<String, String> dbIndexAndTableName = null;

    public ExecutionPlan(SqlNode ast, RelNode plan, CursorMeta columnMeta, BitSet planProperties) {
        this(ast, plan, columnMeta);
        this.planProperties = planProperties;
    }

    public ExecutionPlan(SqlNode ast, RelNode plan, CursorMeta columnMeta) {
        this.ast = ast;
        this.plan = plan;
        this.cursorMeta = columnMeta;
    }

    public RelNode getPlan() {
        return plan;
    }

    public void setPlan(RelNode newPlan) {
        this.plan = newPlan;
    }

    public CursorMeta getCursorMeta() {
        return cursorMeta;
    }

    public SqlNode getAst() {
        return ast;
    }

    public boolean isExplain() {
        return isExplain;
    }

    public void setExplain(boolean explain) {
        isExplain = explain;
    }

    public boolean checkProperty(int prop) {
        return getPlanProperties().get(prop);
    }

    public BitSet getPlanProperties() {
        return planProperties;
    }

    public void setPlanProperties(BitSet planProperties) {
        this.planProperties = planProperties;
    }

    public ExecutionPlan copy(RelNode plan) {
        ExecutionPlan newExecutionPlan = new ExecutionPlan(ast, plan, this.cursorMeta);
        newExecutionPlan.hitCache = this.hitCache;
        newExecutionPlan.isExplain = this.isExplain;
        newExecutionPlan.getPlanProperties().or(this.planProperties);
        newExecutionPlan.originTableNames = this.originTableNames;
        newExecutionPlan.modifiedTables = this.modifiedTables;
        newExecutionPlan.usePostPlanner = this.usePostPlanner;
        newExecutionPlan.privilegeVerifyItems = this.privilegeVerifyItems;
        newExecutionPlan.dmlRelScaleOutWriteFlags = this.dmlRelScaleOutWriteFlags;
        newExecutionPlan.needReBuildScaleOutPlan = this.needReBuildScaleOutPlan;
        newExecutionPlan.tableSet = this.tableSet;
        newExecutionPlan.tableMetaSnapshots = this.tableMetaSnapshots;
        newExecutionPlan.tableSetHashCode = this.tableSetHashCode;
        newExecutionPlan.cacheKey = this.cacheKey;
        newExecutionPlan.errorCount = this.errorCount;
        newExecutionPlan.schemaNames = this.schemaNames;
        newExecutionPlan.planShardInfo = this.planShardInfo;
        newExecutionPlan.htapFeedCount = this.htapFeedCount;
        newExecutionPlan.hitCount = this.hitCount;
        newExecutionPlan.isDirectShardingKey = this.isDirectShardingKey;
        newExecutionPlan.dbIndexAndTableName = this.dbIndexAndTableName;
        return newExecutionPlan;
    }

    public List<PrivilegeVerifyItem> getPrivilegeVerifyItems() {
        return privilegeVerifyItems;
    }

    public void setPrivilegeVerifyItems(List<PrivilegeVerifyItem> privilegeVerifyItems) {
        this.privilegeVerifyItems = privilegeVerifyItems;
    }

    public List<String> getOriginTableNames() {
        return originTableNames;
    }

    public void setOriginTableNames(List<String> originTableNames) {
        this.originTableNames = originTableNames;
    }

    public List<RelUtils.TableProperties> getModifiedTables() {
        return modifiedTables;
    }

    public void setModifiedTables(List<RelUtils.TableProperties> modifiedTables) {
        this.modifiedTables = modifiedTables;
    }

    public boolean is(BitSet planProperties) {
        return getPlanProperties().intersects(planProperties);
    }

    public boolean isUsePostPlanner() {
        return usePostPlanner;
    }

    public void setUsePostPlanner(boolean usePostPlanner) {
        this.usePostPlanner = usePostPlanner;
    }

    public boolean isHitCache() {
        return hitCache;
    }

    public void setHitCache(boolean hitCache) {
        this.hitCache = hitCache;
    }

    public Set<Pair<String, String>> getTableSet() {
        return tableSet;
    }

    public int getTableSetHashCode() {
        return tableSetHashCode;
    }

    public PlanCache.CacheKey getCacheKey() {
        return cacheKey;
    }

    public void saveCacheState(
        Set<Pair<String, String>> tableSet,
        int tableSetHashCode, PlanCache.CacheKey cacheKey, Map<String, TableMeta> tableMetaSnapshots) {
        this.tableSet = tableSet;
        this.tableSetHashCode = tableSetHashCode;
        this.cacheKey = cacheKey;
        this.tableMetaSnapshots = tableMetaSnapshots;
    }

    public int incrementAndGetErrorCount() {
        return errorCount.incrementAndGet();
    }

    public int incrementAndGetHtapFeedCount() {
        return htapFeedCount.incrementAndGet();
    }

    public Map<Integer, Boolean> getDmlRelScaleOutWriteFlags() {
        return dmlRelScaleOutWriteFlags;
    }

    public void setDmlRelScaleOutWriteFlags(Map<Integer, Boolean> dmlRelScaleOutWriteFlags) {
        this.dmlRelScaleOutWriteFlags = dmlRelScaleOutWriteFlags;
    }

    public MemoryPool getPlanMemCachePool() {
        return planMemCachePool;
    }

    public void setPlanMemCachePool(MemoryPool planMemCachePool) {
        this.planMemCachePool = planMemCachePool;
    }

    public PlanMemEstimation getPlanMemEstimation() {
        return planMemEstimation;
    }

    public void setPlanMemEstimation(PlanMemEstimation planMemEstimation) {
        this.planMemEstimation = planMemEstimation;
    }

    public Map<String, TableMeta> getTableMetaSnapshots() {
        return tableMetaSnapshots;
    }

    public void setTableMetaSnapshots(
        Map<String, TableMeta> tableMetaSnapshots) {
        this.tableMetaSnapshots = tableMetaSnapshots;
    }

    public Set<String> getSchemaNames() {
        return schemaNames;
    }

    public void setSchemaNames(Set<String> schemaNames) {
        this.schemaNames = schemaNames;
    }

    public PlanShardInfo getPlanShardInfo(String key) {
        if (!planShardInfo.containsKey(key)) {

        }
        return planShardInfo.get(key);
    }

    public void setPlanShardInfo(String key, PlanShardInfo planShardInfo) {
        this.planShardInfo.put(key, planShardInfo);
    }

    public AtomicLong getHitCount() {
        return hitCount;
    }

    public boolean isDirectShardingKey() {
        return isDirectShardingKey;
    }

    public void setDirectShardingKey(boolean directShardingKey) {
        isDirectShardingKey = directShardingKey;
    }
}



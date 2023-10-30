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

import com.alibaba.polardbx.common.TddlConstants;
import com.alibaba.polardbx.common.eagleeye.EagleeyeHelper;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.schema.InformationSchema;
import com.alibaba.polardbx.optimizer.config.schema.MysqlSchema;
import com.alibaba.polardbx.optimizer.config.schema.PerformanceSchema;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.BuildFinalPlanVisitor;
import com.alibaba.polardbx.optimizer.core.rel.LogicalIndexScan;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import com.alibaba.polardbx.optimizer.exception.OptimizerException;
import com.alibaba.polardbx.optimizer.optimizeralert.OptimizerAlertUtil;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.parse.bean.SqlParameterized;
import com.alibaba.polardbx.optimizer.parse.privilege.PrivilegeContext;
import com.alibaba.polardbx.optimizer.parse.visitor.ContextParameters;
import com.alibaba.polardbx.optimizer.planmanager.PlanManagerUtil;
import com.alibaba.polardbx.optimizer.utils.ForeignKeyUtils;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.alibaba.polardbx.common.properties.ConnectionParams.PLAN_CACHE_SIZE;

/**
 * @author lingce.ldm 2017-11-22 14:38
 */
public final class PlanCache {

    private static final Logger logger = LoggerFactory.getLogger(PlanCache.class);

    private Cache<CacheKey, ExecutionPlan> cache;

    private static final int MAX_ERROR_COUNT = 16;

    private long currentCapacity;

    private static final PlanCache pc = new PlanCache();

    public static PlanCache getInstance() {
        return pc;
    }

    private PlanCache() {
        this.currentCapacity = InstConfUtil.getInt(PLAN_CACHE_SIZE);
        this.cache = buildCache(this.currentCapacity);
    }

    public PlanCache(long capacity) {
        this.currentCapacity = capacity;
        this.cache = buildCache(currentCapacity);
    }

    private Cache<CacheKey, ExecutionPlan> buildCache(long maxSize) {
        int planCacheExpireTime;
        if (ConfigDataMode.isMasterMode()) {
            planCacheExpireTime = DynamicConfig.getInstance().planCacheExpireTime(); // 12h
        } else {
            planCacheExpireTime = 300 * 1000; // 5min
        }
        return CacheBuilder.newBuilder()
            .recordStats()
            .maximumSize(maxSize)
            .expireAfterWrite(planCacheExpireTime, TimeUnit.MILLISECONDS)
            .softValues()
            .build();
    }

    /**
     * 执行过的语句可以在prepare协议下获取执行计划
     */
    public ExecutionPlan getForPrepare(String schema, final SqlParameterized sqlParameterized,
                                       final ExecutionContext ec,
                                       boolean testMode)
        throws ExecutionException {
        CacheKey cacheKey = getCacheKey(schema, sqlParameterized, ec, testMode);
        ExecutionPlan plan = null;
        try {
            plan = cache.getIfPresent(cacheKey);
            return plan;
        } catch (UncheckedExecutionException ex) {
            // unwrap and re-throw
            if (ex.getCause() instanceof RuntimeException) {
                throw (RuntimeException) ex.getCause();
            } else {
                throw ex;
            }
        }
    }

    public ExecutionPlan get(String schema, final SqlParameterized sqlParameterized,
                             final ExecutionContext ec,
                             boolean testMode)
        throws ExecutionException {
        try {
            ExecutionPlan plan =
                getFromCache(schema, sqlParameterized, sqlParameterized.getParameters(), ec, testMode);

            if (PlanManagerUtil.canOptByForcePrimary(plan, ec) && ec.isTsoTransaction()) {
                // If this plan can be optimized, disable plan cache and re-generate it.
                return null;
            }

            if (!checkPlanConstantParamsMatch(plan, ec)) {
                return null;
            }

            boolean valid = ensureValid(plan.getCacheKey(), plan);
            return valid ? plan : null;
        } catch (Throwable e) {
            logger.debug("Error: get from plan cache fail. Cause by " + e.getMessage());
            throw e;
        }
    }

    private static boolean checkPlanConstantParamsMatch(ExecutionPlan plan, ExecutionContext ec) {
        if (plan.getConstantParams() == null) {
            return true;
        }

        Parameters sqlParams = ec.getParams();
        for (Map.Entry<Integer, ParameterContext> entry : plan.getConstantParams().entrySet()) {
            int index = entry.getKey();
            Object sqlParam = sqlParams.getFirstParameter().get(index).getValue();
            Object constantParam = entry.getValue().getValue();

            if (!sqlParam.getClass().equals(constantParam.getClass())) {
                return false;
            }

            // special handling for byte[]
            if (sqlParam instanceof byte[] && !Arrays.equals((byte[]) sqlParam, (byte[]) constantParam)) {
                return false;
            }

            if (!sqlParam.equals(constantParam)) {
                return false;
            }
        }
        return true;
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    private ExecutionPlan getFromCache(String schema, SqlParameterized sqlParameterized, final List<?> params,
                                       final ExecutionContext ec,
                                       boolean testMode) throws ExecutionException {
        final AtomicBoolean beCached = new AtomicBoolean(true);
        CacheKey cacheKey = getCacheKey(schema, sqlParameterized, ec, testMode);
        final Callable<ExecutionPlan> valueLoader = () -> {
            ContextParameters contextParameters = new ContextParameters(testMode);
            SqlNodeList astList = new FastsqlParser()
                .parse(ByteString.from(sqlParameterized.getSql()), params, contextParameters, ec);
            // parameterizedSql can not be a multiStatement.
            SqlNode ast = astList.get(0);
            beCached.set(false);
            boolean isUseHint = ec.isUseHint();
            if (isUseHint || !PlanManagerUtil.cacheSqlKind(ast.getKind())) {
                // Do not cache SQL with Outline Hint.
                return PlaceHolderExecutionPlan.INSTANCE;
            } else {
                // NOTE: BuildFinalPlanVisitor will change ast, so need compute tableSet in advance
                Set<Pair<String, String>> tableSet = PlanManagerUtil.getTableSetFromAst(ast);
                int tablesVersion = PlanManagerUtil.computeTablesVersion(tableSet, schema, ec);
                PlannerContext plannerContext = PlannerContext.fromExecutionContext(ec);
                // When building plan for plan cache, do not add force index for TSO trx.
                plannerContext.setAddForcePrimary(false);
                ExecutionPlan executionPlan = Planner.getInstance().getPlan(ast, plannerContext);
                if (ec.getLoadDataContext() != null) {
                    //load data
                    LogicalInsert logicalInsert = (LogicalInsert) executionPlan.getPlan();
                    logicalInsert.getSqlTemplate();
                    logicalInsert.initLiteralColumnIndex(false);
                    logicalInsert.initAutoIncrementColumn();
                } else if (needBuildFinalPlan(executionPlan.getPlan(), plannerContext)) {
                    BuildFinalPlanVisitor visitor = new BuildFinalPlanVisitor(executionPlan.getAst(), plannerContext);
                    executionPlan = executionPlan.copy(executionPlan.getPlan().accept(visitor));
                }

                // build foreign key sub plans
                ForeignKeyUtils.buildForeignKeySubPlans(ec, executionPlan, plannerContext);

                Map<String, TableMeta> tableMetaSet =
                    PlanManagerUtil.getTableMetaSetByTableSet(tableSet, ec);
                executionPlan.saveCacheState(tableSet, tablesVersion, cacheKey, tableMetaSet);

                // set privilegeVerifyItems to logicalPlan and clear
                // privilegeVerifyItems in privilegeContext
                PrivilegeContext pc = ec.getPrivilegeContext();
                if (pc != null && pc.getPrivilegeVerifyItems() != null) {
                    executionPlan.setPrivilegeVerifyItems(pc.getPrivilegeVerifyItems());
                    pc.setPrivilegeVerifyItems(null);
                }

                // alert if plan cache is full
                OptimizerAlertUtil.plancacheAlert(ec, currentCapacity);
                return executionPlan;
            }
        };

        ExecutionPlan plan;
        try {
            plan = cache.get(cacheKey, valueLoader);
        } catch (UncheckedExecutionException ex) {
            if (ErrorCode.match(ex.getMessage())) {
                if (ex.getCause() instanceof TddlRuntimeException) {
                    // Assuming the caused TddlRuntimeException has correctly encapsulated the ErrorCode.
                    throw (TddlRuntimeException) ex.getCause();
                }
                // There may be an issue of excessive encapsulation
                throw ex;
            } else {
                throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER, ex.getMessage());
            }
        }

        if (beCached.get()) {
            plan.getHitCount().incrementAndGet();
        }
        plan.setHitCache(beCached.get());
        savePlanCachedKey(ec, plan, cacheKey);

        return plan;
    }

    private boolean ensureValid(CacheKey cacheKey, ExecutionPlan executionPlan) {
        if (executionPlan == PlaceHolderExecutionPlan.INSTANCE) {
            if (cacheKey != null) {
                cache.invalidate(cacheKey);
            }
        } else {
            if (OptimizerContext.getContext(cacheKey.getSchema()) == null
                || OptimizerContext.getContext(cacheKey.getSchema()).getLatestSchemaManager() == null) {
                cache.invalidate(cacheKey);
                return false;
            }
            for (TableMeta t : cacheKey.metas) {
                String schemaName = StringUtils.isEmpty(t.getSchemaName()) ? cacheKey.getSchema() : t.getSchemaName();TableMeta newVersionMeta =
                    OptimizerContext.getContext(schemaName).getLatestSchemaManager()
                        .getTableWithNull(t.getTableName());
                if (newVersionMeta == null || newVersionMeta.getVersion() > t.getVersion()) {
                    cache.invalidate(cacheKey);
                    return false;
                }
            }
        }
        return true;
    }

    public void invalidate() {
        for (Map.Entry<CacheKey, ExecutionPlan> entry : cache.asMap().entrySet()) {
            CacheKey cacheKey = entry.getKey();
            ExecutionPlan executionPlan = entry.getValue();
            ensureValid(cacheKey, executionPlan);
        }
    }

    public void forceInvalidateAll() {
        cache.invalidateAll();
    }

    public void invalidateByTable(String schema, String table) {
        if (StringUtils.isEmpty(schema) || StringUtils.isEmpty(table)) {
            return;
        }
        schema = schema.toLowerCase();
        table = table.toLowerCase();
        for (Map.Entry<CacheKey, ExecutionPlan> entry : cache.asMap().entrySet()) {
            CacheKey cacheKey = entry.getKey();
            List<TableMeta> metas = cacheKey.getTableMetas();
            if (metas == null || metas.isEmpty()) {
                cache.invalidate(cacheKey);
                continue;
            }

            for (TableMeta tm : metas) {
                if (tm == null) {
                    continue;
                }
                if (schema.equalsIgnoreCase(tm.getSchemaName()) && table.equalsIgnoreCase(tm.getTableName())) {
                    cache.invalidate(cacheKey);
                    break;
                }
            }
        }
    }

    public void invalidateBySchema(String schema) {
        for (Map.Entry<CacheKey, ExecutionPlan> entry : cache.asMap().entrySet()) {
            CacheKey cacheKey = entry.getKey();
            if (cacheKey.getSchema().equalsIgnoreCase(schema)) {
                cache.invalidate(cacheKey);
            }

            List<TableMeta> metas = cacheKey.getTableMetas();
            if (metas == null || metas.isEmpty()) {
                cache.invalidate(cacheKey);
                continue;
            }

            // clear plan that which were crossing schema
            for (TableMeta tm : metas) {
                if (tm == null) {
                    continue;
                }
                if (schema.equalsIgnoreCase(tm.getSchemaName())) {
                    cache.invalidate(cacheKey);
                    break;
                }
            }
        }
    }

    /**
     * this implementation of this method will affect sql filter by query.
     */
    public static CacheKey getCacheKey(String schema,
                                       SqlParameterized sqlParameterized, ExecutionContext ec,
                                       boolean testMode) {
        Set<Pair<String, String>> tableNames = sqlParameterized.getTables();
        List<TableMeta> tables = new ArrayList<>(tableNames.size());

        StringBuilder versionInfo = new StringBuilder();
        boolean first = true;
        for (Pair<String, String> t : tableNames) {
            if (InformationSchema.NAME.equalsIgnoreCase(t.getKey()) ||
                PerformanceSchema.NAME.equalsIgnoreCase(t.getKey()) ||
                MysqlSchema.NAME.equalsIgnoreCase(t.getKey())) {
                continue; // These schemas never change.
            }
            if (first) {
                first = false;
            } else {
                versionInfo.append(',');
            }

            TableMeta table = ec
                .getSchemaManager(t.getKey()).getTableWithNull(t.getValue());
            if (table != null) {
                tables.add(table);
                versionInfo.append(table.getVersion());
            }
        }

        return new CacheKey(schema, sqlParameterized, versionInfo.toString(), tables, testMode, ec.isAutoCommit(),
            ec.foreignKeyChecks());
    }

    /**
     * invalidate plan cache by table
     */
    public void invalidate(String tableName) {
        if (tableName == null || tableName.isEmpty()) {
            return;
        }
        final String tableNameLowercase = tableName.toLowerCase();
        Set<CacheKey> cacheKeys = Sets.newHashSet();
        cacheKeys.addAll(cache.asMap().keySet());

        for (CacheKey cacheKey : cacheKeys) {
            if (cacheKey.getTableMetas().stream().anyMatch(
                meta -> EagleeyeHelper.rebuildTableName(meta.getTableName(), cacheKey.testing).toLowerCase()
                    .equals(tableNameLowercase))) {
                cache.invalidate(cacheKey);
            }
        }
    }

    public static class CacheKey {
        private static final long NO_TYPE_DIGEST = Long.MIN_VALUE;

        private final String schema;
        final String parameterizedSql;
        final long typeDigest;

        final String versionInfo;

        // other context info
        final boolean testing;

        final List<TableMeta> metas;

        final boolean autoCommit;

        private final List<Object> parameters;

        private final boolean foreignKeyChecks;

        public CacheKey(String schema, String parameterizedSql, String versionInfo, List<TableMeta> metas,
                        boolean testing,
                        boolean autoCommit, boolean foreignKeyChecks) {
            this.schema = schema.toLowerCase(Locale.ROOT);
            this.parameterizedSql = parameterizedSql;
            this.foreignKeyChecks = foreignKeyChecks;
            this.typeDigest = NO_TYPE_DIGEST;
            this.versionInfo = versionInfo;
            this.testing = testing;
            this.metas = metas;
            this.autoCommit = autoCommit;
            parameters = null;
        }

        public CacheKey(String schema, SqlParameterized sqlParameterized, String versionInfo, List<TableMeta> metas,
                        boolean testing, boolean autoCommit, boolean foreignKeyChecks) {
            this.schema = schema.toLowerCase(Locale.ROOT);
            this.parameterizedSql = sqlParameterized.getSql();
            this.typeDigest = sqlParameterized.getDigest();
            this.versionInfo = versionInfo;
            this.testing = testing;
            this.metas = metas;
            this.autoCommit = autoCommit;
            // record the parameters when it is small enough
            if (sqlParameterized.getParaMemory() >= 0) {
                parameters = sqlParameterized.getParameters();
            } else {
                parameters = null;
            }
            this.foreignKeyChecks = foreignKeyChecks;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CacheKey cacheKey = (CacheKey) o;
            return testing == cacheKey.testing &&
                parameterizedSql.equals(cacheKey.parameterizedSql) &&
                (typeDigest == cacheKey.typeDigest || typeDigest == NO_TYPE_DIGEST
                    || cacheKey.typeDigest == NO_TYPE_DIGEST) &&
                versionInfo.equals(cacheKey.versionInfo) &&
                autoCommit == cacheKey.autoCommit &&
                schema.equals(cacheKey.schema) &&
                foreignKeyChecks == cacheKey.foreignKeyChecks;
        }

        @Override
        public int hashCode() {
            return Objects.hash(schema, parameterizedSql, typeDigest, versionInfo, testing, autoCommit,
                foreignKeyChecks);
        }

        public List<TableMeta> getTableMetas() {
            return this.metas;
        }

        public String getParameterizedSql() {
            return parameterizedSql;
        }

        public long getTypeDigest() {
            return typeDigest;
        }

        public List<Object> getParameters() {
            return parameters;
        }

        /**
         * only related to parameterizedSql
         */
        public String getTemplateId() {
            int hashCode = getTemplateHash();
            return TStringUtil.int2FixedLenHexStr(hashCode);
        }

        /**
         * only related to parameterizedSql
         */
        public int getTemplateHash() {
            return parameterizedSql.hashCode();
        }

        public String getSchema() {
            return schema;
        }
    }

    /**
     * Combine hash code of multiple objects
     *
     * @see java.util.Objects#hash(java.lang.Object...)
     */
    private static class HashCombiner {
        private int hashCode = 1;

        void append(Object element) {
            hashCode = 31 * hashCode + (element == null ? 0 : element.hashCode());
        }

        int result() {
            return hashCode;
        }
    }

    private static boolean isAsyncDDLJobPlan(SqlKind sqlKind) {
        switch (sqlKind) {
        case SHOW_DDL_JOBS:
        case REMOVE_DDL_JOB:
        case RECOVER_DDL_JOB:
        case CONTINUE_DDL_JOB:
        case PAUSE_DDL_JOB:
        case ROLLBACK_DDL_JOB:
        case CANCEL_DDL_JOB:
        case CHANGE_DDL_JOB:
        case INSPECT_DDL_JOB_CACHE:
        case CLEAR_DDL_JOB_CACHE:
            return true;
        default:
            return false;
        }
    }

    /**
     * Only SQL that can be pushed down to a single db should build a final
     * plan. Here we just check some simple conditions, BuildFinalPlanVisitor
     * will check again.
     */
    private static boolean needBuildFinalPlan(RelNode plan, PlannerContext plannerContext) {
        if (plannerContext.isExplain()) {
            return false;
        }

        // GSI can not build final plan
        if (plan instanceof LogicalIndexScan || plan instanceof OSSTableScan) {
            return false;
        }

        if (plan instanceof LogicalView) {
            return true;
        } else if (plan instanceof LogicalInsert) {
            LogicalInsert logicalInsert = (LogicalInsert) plan;
            return logicalInsert.isReplace() || logicalInsert.isInsert();
        }
        return false;
    }

    public void feedBack(ExecutionPlan executionPlan, Throwable ex) {
        if (ex == null) {
            return;
        }

        CacheKey cacheKey = executionPlan.getCacheKey();
        if (cacheKey == null) {
            return;
        }

        int errorCount = executionPlan.incrementAndGetErrorCount();
        if (errorCount > MAX_ERROR_COUNT) {
            cache.invalidate(cacheKey);
        }
    }

    public static void savePlanCachedKey(ExecutionContext ec, ExecutionPlan plan, CacheKey cacheKey) {
        plan.saveCacheState(plan.getTableSet(), plan.getTableSetHashCode(), cacheKey, plan.getTableMetaSnapshots());
        if (ec != null) {
            ec.setSqlTemplateId(cacheKey.getTemplateId());
        }
    }

    public void putCachePlan(CacheKey cacheKey, ExecutionPlan plan) {
        try {
            if (cacheKey != null && plan != null) {
                cache.put(cacheKey, plan);
            }
        } catch (Throwable ex) {
            logger.warn("update cache plan failed!!!", ex);
        }
    }

    /**
     * 调整cache的容量
     * 创建一个新的cache并将原来的KV拷贝过来
     * Key的过期时间需要重新计时
     */
    public Pair<CapacityInfo, CapacityInfo> resize(int newSize) {
        if (newSize < TddlConstants.MIN_OPTIMIZER_CACHE_SIZE) {
            throw new OptimizerException(
                "Cannot set plan cache size to less than " + TddlConstants.MIN_OPTIMIZER_CACHE_SIZE);
        }
        if (newSize > TddlConstants.MAX_OPTIMIZER_CACHE_SIZE) {
            throw new OptimizerException(
                "Cannot set plan cache size to greater than " + TddlConstants.MAX_OPTIMIZER_CACHE_SIZE);
        }
        CapacityInfo oldInfo = getCurrentCapacityInfo(), newInfo;
        synchronized (this) {
            if (newSize == this.currentCapacity) {
                return new Pair<>(oldInfo, oldInfo);
            }
            Cache<CacheKey, ExecutionPlan> newCache = buildCache(newSize);
            if (newSize > this.currentCapacity) {
                // 扩容时把原先cache拷贝过来 缩容则只初始化空的cache
                Map<CacheKey, ExecutionPlan> oldKVs = this.cache.asMap();
                for (Map.Entry<CacheKey, ExecutionPlan> entry : oldKVs.entrySet()) {
                    newCache.put(entry.getKey(), entry.getValue());
                }
            }
            Cache<CacheKey, ExecutionPlan> oldCache = this.cache;
            this.cache = newCache;
            this.currentCapacity = newSize;
            oldCache.invalidateAll();
            newInfo = getCurrentCapacityInfo();
        }
        return new Pair<>(oldInfo, newInfo);
    }

    public CapacityInfo getCurrentCapacityInfo() {
        return new CapacityInfo(getCacheKeyCount(), getCurrentCapacity());
    }

    public static class CapacityInfo {
        long keyCount;
        long capacity;

        public long getKeyCount() {
            return keyCount;
        }

        public long getCapacity() {
            return capacity;
        }

        public CapacityInfo(long keyCount, long capacity) {
            this.keyCount = keyCount;
            this.capacity = capacity;
        }
    }

    public Cache<CacheKey, ExecutionPlan> getCache() {
        return cache;
    }

    public long getCacheKeyCount() {
        return this.cache.size();
    }

    public Map<String, AtomicInteger> getCacheKeyCountGroupBySchema() {
        Map<String, AtomicInteger> rs = Maps.newHashMap();
        Set<CacheKey> cacheKeys = Sets.newHashSet();
        cacheKeys.addAll(cache.asMap().keySet());

        for (CacheKey cacheKey : cacheKeys) {
            if (rs.get(cacheKey.getSchema()) == null) {
                rs.put(cacheKey.getSchema(), new AtomicInteger(1));
            } else {
                rs.get(cacheKey.getSchema()).incrementAndGet();
            }
        }
        return rs;
    }

    public long getCurrentCapacity() {
        return currentCapacity;
    }
}

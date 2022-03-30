package com.alibaba.polardbx.optimizer.ccl.service.impl;

import com.alibaba.polardbx.optimizer.ccl.common.CclAction;
import com.alibaba.polardbx.optimizer.ccl.common.CclContext;
import com.alibaba.polardbx.optimizer.ccl.common.CclMetric;
import com.alibaba.polardbx.optimizer.ccl.common.CclPlanCacheKey;
import com.alibaba.polardbx.optimizer.ccl.common.CclRuleInfo;
import com.alibaba.polardbx.optimizer.ccl.common.RescheduleTask;
import com.alibaba.polardbx.optimizer.ccl.exception.CclRescheduleException;
import com.alibaba.polardbx.optimizer.ccl.service.ICclConfigService;
import com.alibaba.polardbx.optimizer.ccl.service.ICclService;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.PlanCache;
import com.google.common.base.Objects;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.alibaba.polardbx.common.TddlConstants;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.taobao.tddl.common.privilege.PrivilegePoint;
import com.alibaba.polardbx.common.privilege.PrivilegeVerifyItem;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.ccl.CclRuleRecord;
import com.alibaba.polardbx.optimizer.parse.privilege.PrivilegeContext;
import com.alibaba.polardbx.optimizer.utils.CclUtils;
import com.alibaba.polardbx.optimizer.utils.SqlKeywordMatchUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * @author busu
 * date: 2020/10/29 1:33 下午
 */
public class CclService implements ICclService {

    private static final Logger LOGGER = LoggerFactory.getLogger(CclService.class);

    private static final Integer NOT_MATCH_PLAN_NULL = 0;
    private static final Integer NOT_MATCH_PLAN_NEED_CHECK_PARAM = 1;

    private static final CclRuleInfo NULL_CCL_RULE_INFO =
        new CclRuleInfo(null, null, 0, false, false, false, false, false, null, null, null, null, null, false);

    private final Cache<Long, Boolean> notMatchConnIdsCache = CacheBuilder.newBuilder()
        .maximumSize(TddlConstants.DEFAULT_CCL_CACHE_NOT_MATCH_CONN_SIZE)
        .expireAfterAccess(10, TimeUnit.MINUTES)
        .softValues()
        .build();

    private final Cache<CclPlanCacheKey, Integer> notMatchPlansCache = CacheBuilder.newBuilder()
        .maximumSize(TddlConstants.DEFAULT_CCL_CACHE_NOT_MATCH_PLAN_SIZE)
        .expireAfterAccess(10, TimeUnit.MINUTES)
        .softValues()
        .build();

    private final Cache<CclPlanCacheKey, CclRuleInfo> matchCclRuleInfoCache = CacheBuilder.newBuilder()
        .maximumSize(TddlConstants.DEFAULT_CCL_MATCH_RULE_SIZE)
        .expireAfterAccess(10, TimeUnit.MINUTES)
        .softValues()
        .build();

    private final Callable<Boolean> notMatchConnIdValueLoader = new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
            return Boolean.FALSE;
        }
    };

    private final Callable<Integer> notMatchPlanValueLoader = new Callable<Integer>() {
        @Override
        public Integer call() throws Exception {
            return NOT_MATCH_PLAN_NULL;
        }
    };

    private final Callable<CclRuleInfo> matchCclRuleInfoValueLoader = new Callable<CclRuleInfo>() {
        @Override
        public CclRuleInfo call() throws Exception {
            return NULL_CCL_RULE_INFO;
        }
    };

    private final ICclConfigService cclConfigService;

    /**
     * stat the effects of the caches
     */
    private final AtomicLong notMatchConnCacheHitCount = new AtomicLong(0);
    private final AtomicLong notMatchPlanParamCacheHitCount = new AtomicLong(0);
    private final AtomicLong notMatchPlanInvalidHitCount = new AtomicLong(0);

    public CclService(ICclConfigService cclConfigService) {
        this.cclConfigService = cclConfigService;
    }

    @Override
    public boolean begin(final ExecutionContext executionContext) {
        if (!isActive() || executionContext == null || executionContext.isRescheduled()) {
            return false;
        }
        if (executionContext.getCclContext() != null) {
            executionContext.setCclContext(null);
        }
        if (executionContext.getExplain() != null) {
            return false;
        }

        CclAction action = getAction(executionContext);
        switch (action) {
        case RUN:
            doRun(executionContext);
            return true;
        case KILL:
            doKill(executionContext);
            return false;
        case WAIT:
            doWait(executionContext);
            return true;
        case RESCHEDULE:
            doReschedule(executionContext);
            return true;
        }
        doNone(executionContext);

        return false;
    }

    protected void doWait(ExecutionContext executionContext) {

        CclContext cclContext = executionContext.getCclContext();
        CclRuleInfo cclRuleInfo = cclContext.getCclRule();

        //add ccl rule info to the queue
        cclRuleInfo.getWaitQueue().offer(cclContext);

        //try to poll once before waiting
        CclUtils.tryPollWaitQueue(cclRuleInfo);

        //park himself
        long startTime = System.currentTimeMillis();

        int waitTimeout = cclRuleInfo.getCclRuleRecord().waitTimeout * 1000 + CclUtils.generateNoise();
        long deadline = (startTime + waitTimeout);
        while (true) {
            LockSupport.parkUntil(cclRuleInfo, deadline);
            long currentTime = System.currentTimeMillis();
            long waitTime = (currentTime - startTime);
            if (cclContext.isReady()) {
                CclMetric cclMetric = new CclMetric(CclMetric.WAIT, waitTime,
                    cclRuleInfo.getCclRuleRecord().id, cclContext.isHitCache());
                cclContext.setMetric(cclMetric);
                return;
            }
            if (currentTime > deadline || Thread.interrupted()) {
                CclMetric cclMetric = new CclMetric(CclMetric.WAIT_K, waitTime,
                    cclRuleInfo.getCclRuleRecord().id, cclContext.isHitCache());
                cclContext.setMetric(cclMetric);
                cclContext.getCclRule().getCclRuntimeStat().killedCount.incrementAndGet();
                throw new TddlRuntimeException(ErrorCode.ERR_CCL, String
                    .format(CCL_WAIT_TIMEOUT_MESSAGE_FORMAT,
                        cclContext.getCclRule().getCclRuleRecord().parallelism,
                        cclContext.getCclRule().getCclRuleRecord().id, waitTime),
                    null);
            }
        }

    }

    protected void doReschedule(ExecutionContext executionContext) {
        CclContext cclContext = executionContext.getCclContext();
        CclRuleInfo<RescheduleTask> cclRuleInfo = cclContext.getCclRule();
        throw new CclRescheduleException(
            String.format("The query should be rescheduled. ccl name = %s", cclRuleInfo.getCclRuleRecord().id),
            (reschedulable) -> {
                cclContext.setReschedule(true);
                RescheduleTask rescheduleTask = new RescheduleTask();
                rescheduleTask.setReschedulable(reschedulable);
                rescheduleTask.setCclRuleInfo(cclRuleInfo);
                rescheduleTask.setHitCache(cclContext.isHitCache());
                rescheduleTask.setWaitStartTs(System.currentTimeMillis());
                rescheduleTask.setActivation(new AtomicBoolean(false));
                reschedulable.setRescheduled(true, rescheduleTask);
                cclRuleInfo.getWaitQueue().offer(rescheduleTask);
                CclUtils.tryPollWaitQueue(cclRuleInfo);
            });
    }

    protected void doRun(ExecutionContext executionContext) {
        CclContext cclContext = executionContext.getCclContext();
        cclContext.setReady();
        CclMetric cclMetric =
            new CclMetric(CclMetric.RUN, cclContext.getCclRule().getCclRuleRecord().id, cclContext.isHitCache());
        cclContext.setMetric(cclMetric);
    }

    protected void doKill(ExecutionContext executionContext) {
        CclContext cclContext = executionContext.getCclContext();
        cclContext.setValid(false);
        cclContext.getCclRule().getCclRuntimeStat().killedCount.incrementAndGet();
        CclMetric cclMetric =
            new CclMetric(CclMetric.KILLED, cclContext.getCclRule().getCclRuleRecord().id, cclContext.isHitCache());
        cclContext.setMetric(cclMetric);

        throw new TddlRuntimeException(ErrorCode.ERR_CCL, String
            .format("Exceeding the max concurrency %d per node of ccl rule %s",
                cclContext.getCclRule().getCclRuleRecord().parallelism, cclContext.getCclRule().getCclRuleRecord().id),
            null);
    }

    protected void doNone(ExecutionContext executionContext) {
        CclContext cclContext = executionContext.getCclContext();
        if (cclContext != null) {
            cclContext.setValid(false);
        }
    }

    @Override
    public void end(ExecutionContext executionContext) {
        if (executionContext == null) {
            return;
        }
        CclContext cclContext = executionContext.getCclContext();
        if (cclContext == null || !cclContext.isValid()) {
            //not matched executionContext
            return;
        }
        CclRuleInfo cclRuleInfo = cclContext.getCclRule();

        if (cclContext.setFinish()) {
            boolean deliverResult = CclUtils.deliverRunningCount(cclRuleInfo);
            if (!deliverResult) {
                CclUtils.exitRunning(cclRuleInfo);
            }
        }
        CclUtils.exitStaying(cclRuleInfo);
        cclContext.setValid(false);

    }

    private CclAction getAction(final ExecutionContext executionContext) {

        CclRuleInfo matchRule = null;
        try {
            matchRule = matchRules(executionContext);
        } catch (Throwable e) {
            LOGGER.error("Failed to match rules", e);
        }
        if (matchRule == null) {
            return CclAction.NONE;
        }
        matchRule.getCclRuntimeStat().totalMatchCclRuleCount.incrementAndGet();
        CclContext cclContext = executionContext.getCclContext();
        if (cclContext != null) {
            cclContext.setCclRule(matchRule);
            cclContext.setThread(Thread.currentThread());
            cclContext.setValid(true);
        } else {
            cclContext = new CclContext(matchRule, Thread.currentThread());
        }
        executionContext.setCclContext(cclContext);

        if (!CclUtils.enterStaying(matchRule)) {
            return CclAction.KILL;
        }

        if (CclUtils.enterRunning(matchRule)) {
            return CclAction.RUN;
        }

        if (matchRule.isReschedule()) {
            return CclAction.RESCHEDULE;
        }

        return CclAction.WAIT;

    }

    @Override
    public void invalidateCclRule(CclRuleInfo cclRuleInfo) {

        Queue queue = cclRuleInfo.getWaitQueue();
        if (!cclRuleInfo.isEnabled()) {
            Object obj = queue.poll();
            while (obj != null) {
                if (obj instanceof CclContext) {
                    CclUtils.wakenUp((CclContext) obj);
                } else {
                    CclUtils.wakenUp((RescheduleTask) obj);
                }
                obj = queue.poll();
            }
        }

    }

    private CclRuleInfo matchRules(final ExecutionContext executionContext) throws ExecutionException {
        PrivilegeContext privilegeContext = executionContext.getPrivilegeContext();
        ExecutionPlan plan = executionContext.getFinalPlan();
        if (plan == null) {
            return null;
        }

        Long connId = executionContext.getConnId();
        if (notMatchConnIdsCache.get(connId, notMatchConnIdValueLoader)) {
            notMatchConnCacheHitCount.incrementAndGet();
            return null;
        }

        PlanCache.CacheKey cacheKey = plan.getCacheKey();
        if (cacheKey == null) {
            return null;
        }

        String currentUser = privilegeContext.getUser();
        String currentHost = privilegeContext.getHost();

        CclPlanCacheKey matchPlanKey = getCclPlanKey(currentUser, currentHost, executionContext.getSchemaName(),
            cacheKey.getTemplateHash());
        Integer notMatchPlanCacheValue = notMatchPlansCache.get(matchPlanKey, notMatchPlanValueLoader);
        if (notMatchPlanCacheValue > NOT_MATCH_PLAN_NULL) {
            Parameters params = executionContext.getParams();
            boolean mightContain = mightParamsMatchKeywords(params);
            if (!mightContain) {
                notMatchPlanParamCacheHitCount.incrementAndGet();
                return null;
            }
            notMatchPlanInvalidHitCount.incrementAndGet();
        }

        CclRuleInfo cachedCclRuleInfo = matchCclRuleInfoCache.get(matchPlanKey, matchCclRuleInfoValueLoader);
        if (cachedCclRuleInfo != NULL_CCL_RULE_INFO) {
            cachedCclRuleInfo.getCclRuntimeStat().matchCclRuleHitCount.incrementAndGet();
            CclContext cclContext = new CclContext(true);
            executionContext.setCclContext(cclContext);
            return cachedCclRuleInfo;
        }

        int currentTemplateId = cacheKey.getTemplateHash();
        boolean notMatchUser = true;
        for (CclRuleInfo cclRuleInfo : this.cclConfigService.getCclRuleInfos()) {

            CclRuleRecord cclRuleRecord = cclRuleInfo.getCclRuleRecord();
            //match user
            if (cclRuleInfo.isNeedMatchUser() && !StringUtils.equals(currentUser, cclRuleRecord.userName)) {
                continue;
            }

            if (cclRuleInfo.isNeedMatchHost()) {
                String clientIp = cclRuleRecord.clientIp;
                if (cclRuleInfo.isNormalHost()) {
                    if (!StringUtils.equals(currentHost, clientIp)) {
                        continue;
                    }
                } else if (cclRuleInfo.getHostCommPrefixLen() >= 0) {
                    if (!currentHost
                        .startsWith(clientIp.substring(0, cclRuleInfo.getHostCommPrefixLen()))) {
                        continue;
                    }
                } else if (cclRuleInfo.getHostCommSuffixLen() >= 0) {

                    if (!currentHost.endsWith(clientIp
                        .substring(clientIp.length() - cclRuleInfo.getHostCommSuffixLen()))) {
                        continue;
                    }

                } else if (!cclRuleInfo.getHost().matches(currentHost)) {
                    //regex match host
                    continue;
                }

            }

            if (notMatchUser) {
                notMatchUser = false;
            }

            boolean privilegeMatched = false;
            List<PrivilegeVerifyItem> privilegeVerifyItems = plan.getPrivilegeVerifyItems();
            if (CollectionUtils.isEmpty(privilegeVerifyItems)) {
                //match schema and table
                String schemaName = executionContext.getSchemaName();
                if (cclRuleInfo.isNeedMatchDb()) {
                    if (!StringUtils.equals(cclRuleRecord.dbName, schemaName) && !StringUtils.equals("*", schemaName)) {
                        continue;
                    }
                }
                //match table
                if (cclRuleInfo.isNeedMatchTable()) {
                    continue;
                }
                privilegeMatched = true;
            }

            //match sql type, db, table from privilege items
            if (!privilegeMatched) {
                for (PrivilegeVerifyItem privilegeVerifyItem : privilegeVerifyItems) {

                    //match sql type
                    if (privilegeVerifyItem.getPrivilegePoint() != cclRuleInfo.getSqlType()
                        && cclRuleInfo.getSqlType() != PrivilegePoint.ALL) {
                        continue;
                    }

                    //match db
                    if (cclRuleInfo.isNeedMatchDb()) {
                        String db = privilegeVerifyItem.getDb();
                        if (StringUtils.isEmpty(db)) {
                            db = privilegeContext.getSchema();
                        }
                        if (!StringUtils.equals(cclRuleRecord.dbName, db) && !StringUtils.equals("*", db)) {
                            continue;
                        }
                    }

                    //match table
                    if (cclRuleInfo.isNeedMatchTable()) {
                        String table = privilegeVerifyItem.getTable();
                        if (!StringUtils.equals(cclRuleRecord.tableName, table) && !StringUtils.equals("*", table)) {
                            continue;
                        }
                    }
                    privilegeMatched = true;
                    break;
                }
            }

            if (!privilegeMatched) {
                continue;
            }

            //match templateId
            Set<Integer> templateIdSet = cclRuleInfo.getTemplateIdSet();
            if (CollectionUtils.isNotEmpty(templateIdSet)) {
                if (!templateIdSet.contains(currentTemplateId)) {
                    continue;
                }
            }

            //match SQL param
            Map<Integer, Object> posParamMap = cclRuleInfo.getPosParamMap();
            if (posParamMap != null) {
                boolean matchQueryKeyword = matchQueryKeyword(posParamMap, executionContext.getParams());
                if (!matchQueryKeyword) {
                    continue;
                }
            }

            SqlKeywordMatchUtils.MatchResult keywordMatchResult = SqlKeywordMatchUtils.DEFAULT_MATCH_RESULT;
            //match keywords. user MySQlLexer in fast sql
            List<String> keywords = cclRuleInfo.getKeywords();
            if (CollectionUtils.isNotEmpty(keywords)) {
                keywordMatchResult = SqlKeywordMatchUtils
                    .matchKeywords(executionContext.getOriginSql(), executionContext.getParams(), keywords,
                        cclRuleInfo.getTotalKeywordLen());
                if (!keywordMatchResult.matched) {
                    continue;
                }
            }

            if (CollectionUtils.isNotEmpty(keywords) && !keywordMatchResult.matchParam) {
                keywordMatchResult.matchParam = mightParamsMatchKeywords(executionContext.getParams());
            }

            if (keywordMatchResult.matched && !keywordMatchResult.matchParam
                && cclRuleInfo.getCclRuleRecord().fastMatch > 0 && !cclRuleInfo.isQueryMatch()) {
                matchCclRuleInfoCache.put(matchPlanKey, cclRuleInfo);
            }

            return cclRuleInfo;
        }

        if (notMatchUser) {
            notMatchConnIdsCache.put(connId, Boolean.TRUE);
        } else if (cclConfigService.isAllFastMatch()) {
            //判断是否需要参数
            notMatchPlansCache.put(matchPlanKey, NOT_MATCH_PLAN_NEED_CHECK_PARAM);
        }

        return null;
    }

    private CclPlanCacheKey getCclPlanKey(String user, String host, String schema, int templateId) {
        return new CclPlanCacheKey(templateId, schema, user, host);
    }

    private boolean mightParamsMatchKeywords(Parameters params) {
        for (Map<Integer, ParameterContext> parameterContextMap : params.getBatchParameters()) {
            for (ParameterContext parameterContext : parameterContextMap.values()) {
                ParameterMethod parameterMethod = parameterContext.getParameterMethod();
                switch (parameterMethod) {
                case setArray:
                case setAsciiStream:
                case setBinaryStream:
                case setBytes:
                case setByte:
                case setCharacterStream:
                case setRef:
                case setUnicodeStream:
                case setTableName:
                    continue;
                }
                Object value = parameterContext.getValue();
                if (value == null) {
                    value = "null";
                }
                if (cclConfigService.mightContainKeyword(value.toString())) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean matchQueryKeyword(Map<Integer, Object> posValueMap, Parameters params) {
        for (Map.Entry<Integer, Object> entry : posValueMap.entrySet()) {
            Integer pos = entry.getKey();
            Object value = entry.getValue();
            for (Map<Integer, ParameterContext> parameterContextMap : params.getBatchParameters()) {
                ParameterContext parameterContext = parameterContextMap.get(pos);
                if (parameterContext == null) {
                    return false;
                }
                ParameterMethod parameterMethod = parameterContext.getParameterMethod();
                switch (parameterMethod) {
                case setArray:
                case setAsciiStream:
                case setBinaryStream:
                case setBytes:
                case setByte:
                case setCharacterStream:
                case setRef:
                case setUnicodeStream:
                case setTableName:
                    return false;
                }
                if (!Objects.equal(value, parameterContext.getValue())) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public void clearCache() {
        notMatchConnIdsCache.invalidateAll();
        notMatchPlansCache.invalidateAll();
        matchCclRuleInfoCache.invalidateAll();
    }

    @Override
    public List<Long> getCacheStats() {
        long matchCclRuleHitCount = 0;
        long totalMatchCclRuleCount = 0;
        for (CclRuleInfo cclRuleInfo : cclConfigService.getCclRuleInfos()) {
            matchCclRuleHitCount += cclRuleInfo.getCclRuntimeStat().matchCclRuleHitCount.get();
            totalMatchCclRuleCount += cclRuleInfo.getCclRuntimeStat().totalMatchCclRuleCount.get();
        }

        return Lists.newArrayList(notMatchConnCacheHitCount.get(), notMatchPlanParamCacheHitCount.get(),
            notMatchPlanInvalidHitCount.get(), matchCclRuleHitCount,
            totalMatchCclRuleCount);
    }

    private boolean isActive() {
        return !cclConfigService.getCclRuleInfos().isEmpty();
    }

}

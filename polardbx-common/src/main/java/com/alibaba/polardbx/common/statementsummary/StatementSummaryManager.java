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

package com.alibaba.polardbx.common.statementsummary;

import com.alibaba.polardbx.common.statementsummary.model.ExecInfo;
import com.alibaba.polardbx.common.statementsummary.model.StatementSummaryByDigest;
import com.alibaba.polardbx.common.statementsummary.model.StatementSummaryByDigestEntry;
import com.alibaba.polardbx.common.statementsummary.model.StatementSummaryElementByDigest;
import com.alibaba.polardbx.common.statementsummary.model.StatementSummaryKeyByDigest;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.Lists;
import lombok.Data;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import java.lang.ref.SoftReference;
import java.security.InvalidParameterException;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * @author busu
 * date: 2021/11/4 11:27 上午
 * manage data for statementsummary, a singleton
 */
public class StatementSummaryManager {

    private static StatementSummaryManager INSTANCE = new StatementSummaryManager();

    private StatementSummaryConfig config = new StatementSummaryConfig();

    private Cache<StatementSummaryKeyByDigest, StatementSummaryByDigest> stmtCache;

    private final static StatementSummaryByDigest NULL_OBJECT = new StatementSummaryByDigest();

    private StatementSummaryByDigest other;

    public static StatementSummaryManager getInstance() {
        return INSTANCE;
    }

    private StatementSummaryManager() {
    }

    /**
     * get the cached statement summarys
     */
    public Collection<StatementSummaryByDigest> getCachedStatementSummaryByDigests() {
        final Cache<StatementSummaryKeyByDigest, StatementSummaryByDigest> cache = stmtCache;
        if (cache != null) {
            return cache.asMap().values();
        }
        return CollectionUtils.EMPTY_COLLECTION;
    }

    /**
     * get the other object which can not be cached in the StmtCache
     */
    public StatementSummaryByDigest getOtherStatementSummaryByDigest() {
        return this.other;
    }

    /**
     * summary the exec info
     */
    public void summaryStmt(ExecInfo execInfo) throws ExecutionException {
        final Cache<StatementSummaryKeyByDigest, StatementSummaryByDigest> stmtCache = this.stmtCache;
        //check if the the statement summary is enabled
        if (stmtCache == null || config.getEnableStmtSummary() <= 0) {
            return;
        }

        int percent = config.getStmtSummaryPercent();
        //0: only slow; > 0 use percent
        if (percent == 0) {
            if (!execInfo.isSlow()) {
                return;
            }
        } else if (percent <= ThreadLocalRandom.current().nextInt(100)) {
            return;
        }

        //build cache key
        StatementSummaryKeyByDigest summaryKey = StatementSummaryKeyByDigest
            .create(execInfo.getSchema(), execInfo.getTemplateHash(), execInfo.getPrevTemplateHash(),
                execInfo.getPlanHash());
        final Callable<StatementSummaryByDigest> valueLoader = () -> NULL_OBJECT;
        StatementSummaryByDigest statementSummaryByDigest = stmtCache.get(summaryKey, valueLoader);
        //check if the value is stored before, if not a new one will be created.
        if (statementSummaryByDigest == NULL_OBJECT) {
            String sampleSql = execInfo.getSampleSql();
            if (sampleSql != null && sampleSql.length() > this.config.getStmtSummaryMaxSqlLength()) {
                sampleSql = StringUtils.substring(sampleSql, 0, this.config.getStmtSummaryMaxSqlLength());
                //not use the returnObj of substring, so that the origin sql could be released.
                sampleSql = new StringBuilder().append(sampleSql).toString();
                execInfo.setSampleSql(sampleSql);
            }
            statementSummaryByDigest =
                new StatementSummaryByDigest(execInfo.getSchema(), execInfo.getTemplateHash(), execInfo.getPlanHash(),
                    execInfo.getSampleSql(),
                    execInfo.getTemplateText(), execInfo.getSqlType(), execInfo.getPrevTemplateHash(),
                    execInfo.getPrevTemplateText(), execInfo.getSampleTraceId(), execInfo.getWorkloadType(),
                    execInfo.getExecuteMode());
            stmtCache.put(summaryKey, statementSummaryByDigest);
        }
        statementSummaryByDigest.add(execInfo, config.stmtSummaryHistorySize, config.stmtSummaryRefreshInterval);
    }

    public StatementSummaryConfig getConfig() {
        return config;
    }

    /**
     * store statement summary config values
     */
    @Data
    public class StatementSummaryConfig {

        public static final int USE_DEFAULT_VALUE = -1;
        public static final int USE_NO_CHANGE_VALUE = -2;

        public static final int DEFAULT_VALUE_ENABLE_STMT_SUMMARY = 1;
        public static final long DEFAULT_VALUE_STMT_SUMMARY_REFRESH_INTERVAL = 1800000;
        public static final int DEFAULT_VALUE_STMT_SUMMARY_HISTORY_SIZE = 24;
        public static final int DEFAULT_VALUE_STMT_SUMMARY_MAX_STMT_COUNT = 1000;
        public static final int DEFAULT_VALUE_RECORD_INTERNAL_STATEMENT = 0;
        public static final int DEFAULT_VALUE_STMT_SUMMARY_MAX_SQL_LENGTH = 1024;
        public static final int DEFAULT_VALUE_PERCENT = 1;
        //0: close, 1: open
        /**
         * >0 : open
         * <=0: close
         */
        private int enableStmtSummary;
        /**
         * the interval of refresh current summary stage, unit: milliseconds
         */
        private long stmtSummaryRefreshInterval;
        /**
         * the max stored stages
         */
        private int stmtSummaryHistorySize;
        /**
         * max count statement in the cache
         */
        private int stmtSummaryMaxStmtCount;

        /**
         * if summary the internal statement
         */
        private int recordIntervalStatement;

        /**
         * the max len of sample sql
         */
        private int stmtSummaryMaxSqlLength;

        /**
         * 0: only summary slow sql; >0 the percent
         */
        private int stmtSummaryPercent = DEFAULT_VALUE_PERCENT;

        private StatementSummaryConfig() {
            setConfig(USE_DEFAULT_VALUE, USE_DEFAULT_VALUE, USE_DEFAULT_VALUE, USE_DEFAULT_VALUE, USE_DEFAULT_VALUE,
                USE_DEFAULT_VALUE);
        }

        public synchronized void setConfig(int enableStmtSummary, long stmtSummaryRefreshInterval,
                                           int stmtSummaryHistorySize,
                                           int stmtSummaryMaxStmtCount, int recordIntervalStatement,
                                           int stmtSummaryMaxSqlLength) {
            enableStmtSummary =
                getConfigValue(enableStmtSummary, 0, DEFAULT_VALUE_ENABLE_STMT_SUMMARY, this.enableStmtSummary);
            stmtSummaryRefreshInterval =
                getConfigValue(stmtSummaryRefreshInterval, DEFAULT_VALUE_STMT_SUMMARY_REFRESH_INTERVAL,
                    this.stmtSummaryRefreshInterval);
            if (stmtSummaryRefreshInterval <= 0) {
                throw new InvalidParameterException(
                    "[StatementSummaryConfig] Invalid stmtSummaryRefreshInterval of " + stmtSummaryRefreshInterval);
            }
            stmtSummaryHistorySize =
                getConfigValue(stmtSummaryHistorySize, DEFAULT_VALUE_STMT_SUMMARY_HISTORY_SIZE,
                    this.stmtSummaryHistorySize);
            if (stmtSummaryHistorySize <= 0) {
                throw new InvalidParameterException(
                    "[StatementSummaryConfig] Invalid stmtSummaryHistorySize of " + stmtSummaryHistorySize);
            }
            stmtSummaryMaxStmtCount =
                getConfigValue(stmtSummaryMaxStmtCount, DEFAULT_VALUE_STMT_SUMMARY_MAX_STMT_COUNT,
                    this.stmtSummaryMaxStmtCount);
            if (stmtSummaryMaxStmtCount <= 0) {
                throw new InvalidParameterException(
                    "[StatementSummaryConfig] Invalid stmtSummaryMaxStmtCount  of " + stmtSummaryMaxStmtCount);
            }
            recordIntervalStatement =
                getConfigValue(recordIntervalStatement, DEFAULT_VALUE_RECORD_INTERNAL_STATEMENT,
                    this.recordIntervalStatement);
            if (recordIntervalStatement < 0) {
                throw new InvalidParameterException(
                    "[StatementSummaryConfig] Invalid recordIntervalStatement  of " + recordIntervalStatement);
            }
            this.recordIntervalStatement = recordIntervalStatement;
            stmtSummaryMaxSqlLength =
                getConfigValue(stmtSummaryMaxSqlLength, DEFAULT_VALUE_STMT_SUMMARY_MAX_SQL_LENGTH,
                    this.stmtSummaryMaxSqlLength);
            if (stmtSummaryMaxSqlLength < 0) {
                throw new InvalidParameterException(
                    "[StatementSummaryConfig] Invalid stmtSummaryMaxSqlLength of " + stmtSummaryMaxSqlLength);
            }
            this.stmtSummaryMaxSqlLength = stmtSummaryMaxSqlLength;
            if (enableStmtSummary != this.enableStmtSummary
                || stmtSummaryRefreshInterval != this.stmtSummaryRefreshInterval
                || stmtSummaryHistorySize != this.stmtSummaryHistorySize
                || stmtSummaryMaxStmtCount != this.stmtSummaryMaxStmtCount) {
                //rebuild the cache
                Cache<StatementSummaryKeyByDigest, StatementSummaryByDigest> newStmtCache = null;
                StatementSummaryByDigest newOther = null;
                if (enableStmtSummary > 0 && (stmtSummaryMaxStmtCount != this.stmtSummaryMaxStmtCount
                    || stmtCache == null)) {
                    HoldOtherRemovalListener holdOtherRemovalListener = new HoldOtherRemovalListener();
                    newStmtCache = CacheBuilder.newBuilder().maximumSize(stmtSummaryMaxStmtCount)
                        .expireAfterAccess((stmtSummaryHistorySize + 1) * stmtSummaryRefreshInterval / 1000,
                            TimeUnit.SECONDS).removalListener(holdOtherRemovalListener).build();
                    newOther = new StatementSummaryByDigest();
                    holdOtherRemovalListener.setStatementSummaryByDigestSoftReference(newOther);
                    if (stmtCache != null) {
                        newStmtCache.putAll(stmtCache.asMap());
                    }
                    other = newOther;
                    stmtCache = newStmtCache;
                }
                this.enableStmtSummary = enableStmtSummary;
                this.stmtSummaryRefreshInterval = stmtSummaryRefreshInterval;
                this.stmtSummaryHistorySize = stmtSummaryHistorySize;
                this.stmtSummaryMaxStmtCount = stmtSummaryMaxStmtCount;
                if (enableStmtSummary <= 0) {
                    other = null;
                    stmtCache = null;
                }
            }
        }

        public void setEnableStmtSummary(int enableStmtSummary) {
            setConfig(enableStmtSummary, USE_NO_CHANGE_VALUE, USE_NO_CHANGE_VALUE, USE_NO_CHANGE_VALUE,
                USE_NO_CHANGE_VALUE, USE_NO_CHANGE_VALUE);
        }

        public void setStmtSummaryRefreshInterval(long stmtSummaryRefreshInterval) {
            setConfig(USE_NO_CHANGE_VALUE, stmtSummaryRefreshInterval, USE_NO_CHANGE_VALUE, USE_NO_CHANGE_VALUE,
                USE_NO_CHANGE_VALUE, USE_NO_CHANGE_VALUE);
        }

        public void setStmtSummaryHistorySize(int stmtSummaryHistorySize) {
            setConfig(USE_NO_CHANGE_VALUE, USE_NO_CHANGE_VALUE, stmtSummaryHistorySize, USE_NO_CHANGE_VALUE,
                USE_NO_CHANGE_VALUE, USE_NO_CHANGE_VALUE);
        }

        public void setStmtSummaryMaxStmtCount(int stmtSummaryMaxStmtCount) {
            setConfig(USE_NO_CHANGE_VALUE, USE_NO_CHANGE_VALUE, USE_NO_CHANGE_VALUE, stmtSummaryMaxStmtCount,
                USE_NO_CHANGE_VALUE, USE_NO_CHANGE_VALUE);
        }

        public void setRecordIntervalStatement(int recordIntervalStatement) {
            setConfig(USE_NO_CHANGE_VALUE, USE_NO_CHANGE_VALUE, USE_NO_CHANGE_VALUE, USE_NO_CHANGE_VALUE,
                recordIntervalStatement, USE_NO_CHANGE_VALUE);
        }

        public void setStmtSummaryMaxSqlLength(int stmtSummaryMaxSqlLength) {
            setConfig(USE_NO_CHANGE_VALUE, USE_NO_CHANGE_VALUE, USE_NO_CHANGE_VALUE, USE_NO_CHANGE_VALUE,
                USE_NO_CHANGE_VALUE, stmtSummaryMaxSqlLength);
        }

        public synchronized void setStmtSummaryPercent(int stmtSummaryPercent) {
            this.stmtSummaryPercent = stmtSummaryPercent;
        }

        private int getConfigValue(int value, int defaultValue, int oldValue) {
            if (value == USE_DEFAULT_VALUE) {
                return defaultValue;
            } else if (value == USE_NO_CHANGE_VALUE) {
                return oldValue;
            }
            return value;
        }

        private long getConfigValue(long value, long defaultValue, long oldValue) {
            if (value == USE_DEFAULT_VALUE) {
                return defaultValue;
            } else if (value == USE_NO_CHANGE_VALUE) {
                return oldValue;
            }
            return value;
        }

        private int getConfigValue(int value, int drdsDefaultValue, int polarxDefaultValue, int oldValue) {
            if (value == USE_DEFAULT_VALUE) {
                return polarxDefaultValue;
            } else if (value == USE_NO_CHANGE_VALUE) {
                return oldValue;
            }
            return value;
        }

        class HoldOtherRemovalListener
            implements RemovalListener<StatementSummaryKeyByDigest, StatementSummaryByDigest> {

            private SoftReference<StatementSummaryByDigest> statementSummaryByDigestSoftReference;

            public HoldOtherRemovalListener() {
            }

            public void setStatementSummaryByDigestSoftReference(StatementSummaryByDigest other) {
                statementSummaryByDigestSoftReference = new SoftReference<>(other);
            }

            @Override
            public void onRemoval(
                RemovalNotification<StatementSummaryKeyByDigest, StatementSummaryByDigest> notification) {
                if (notification.getCause() == RemovalCause.SIZE && notification.getValue() != null
                    && statementSummaryByDigestSoftReference != null) {
                    StatementSummaryByDigest other = statementSummaryByDigestSoftReference.get();
                    if (other != null) {
                        other.merge(notification.getValue(), stmtSummaryHistorySize);
                    }
                }
            }
        }

    }

    public Iterator<StatementSummaryByDigestEntry> getCurrentStmtSummaries(long nowBeginTime) {
        return getStmtSummaries(nowBeginTime, (e1, e2) -> {
            return (int) (e1.longValue() - e2.longValue());
        });
    }

    public Iterator<StatementSummaryByDigestEntry> getStmtHistorySummaries(long nowBeginTime) {
        return getStmtSummaries(nowBeginTime, (e1, e2) -> {
            return (int) (e1.longValue() - e2.longValue()) == 0 ? 1 : 0;
        });
    }

    private Iterator<StatementSummaryByDigestEntry> getStmtSummaries(long nowBeginTime, Comparator<Long> comparator) {
        nowBeginTime = nowBeginTime / config.getStmtSummaryRefreshInterval() * config.getStmtSummaryRefreshInterval();
        List<StatementSummaryByDigestEntry> statementSummaryByDigestEntries = Lists.newArrayList();
        final Cache<StatementSummaryKeyByDigest, StatementSummaryByDigest> stmtCache = this.stmtCache;
        final StatementSummaryByDigest other = this.other;
        if (stmtCache != null) {
            for (Map.Entry<StatementSummaryKeyByDigest, StatementSummaryByDigest> entry : stmtCache.asMap()
                .entrySet()) {
                synchronized (entry.getValue()) {
                    for (StatementSummaryElementByDigest statementSummaryElementByDigest : entry.getValue()) {
                        if (comparator.compare(statementSummaryElementByDigest.getBeginTime(), nowBeginTime) == 0) {
                            statementSummaryByDigestEntries.add(
                                new StatementSummaryByDigestEntry(entry.getValue(), statementSummaryElementByDigest));
                        }
                    }
                }
            }
        }

        if (other != null) {
            synchronized (other) {
                for (StatementSummaryElementByDigest statementSummaryElementByDigest : other) {
                    if (comparator.compare(statementSummaryElementByDigest.getBeginTime(), nowBeginTime) == 0) {
                        statementSummaryByDigestEntries.add(
                            new StatementSummaryByDigestEntry(other,
                                statementSummaryElementByDigest));
                    }
                }
            }
        }
        return statementSummaryByDigestEntries.iterator();
    }

}


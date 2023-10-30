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

package com.alibaba.polardbx.executor.common;

import com.alibaba.polardbx.common.IdGenerator;
import com.alibaba.polardbx.common.constants.SequenceAttribute;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.executor.sync.InspectGroupSeqMinValueSyncAction;
import com.alibaba.polardbx.executor.sync.SequenceSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.gms.util.SeqTypeUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.sequence.Sequence;
import com.alibaba.polardbx.sequence.exception.SequenceException;
import com.alibaba.polardbx.sequence.impl.BaseSequence;
import com.alibaba.polardbx.sequence.impl.CustomUnitGroupSequence;
import com.alibaba.polardbx.sequence.impl.CustomUnitGroupSequenceDao;
import com.alibaba.polardbx.sequence.impl.GroupSequence;
import com.alibaba.polardbx.sequence.impl.GroupSequenceDao;
import com.alibaba.polardbx.sequence.impl.NewSequence;
import com.alibaba.polardbx.sequence.impl.NewSequenceDao;
import com.alibaba.polardbx.sequence.impl.NewSequenceScheduler;
import com.alibaba.polardbx.sequence.impl.NewSequenceWithCache;
import com.alibaba.polardbx.sequence.impl.SimpleSequence;
import com.alibaba.polardbx.sequence.impl.SimpleSequenceDao;
import com.alibaba.polardbx.sequence.impl.TimeBasedSequence;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_INNER_STEP;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_UNIT_COUNT;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_UNIT_INDEX;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.GROUP_SEQ_MIN_VALUE;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.NEW_SEQ;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.STR_NA;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.TIME_BASED;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.Type;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.UPPER_LIMIT_UNIT_COUNT;
import static com.alibaba.polardbx.gms.metadb.GmsSystemTables.SEQUENCE;
import static com.alibaba.polardbx.gms.metadb.GmsSystemTables.SEQUENCE_OPT;

/**
 * @author mengshi.sunmengshi 2014/05/07 14:08:15
 * @since 5.1.0
 */
public class SequenceLoadFromDBManager extends AbstractSequenceManager {

    private final static Logger logger = LoggerFactory.getLogger(SequenceLoadFromDBManager.class);
    private LoadingCache<StringIgnoreCase, Sequence> cache = null;

    private ParamManager paramManager = null;

    private String schemaName;
    private Sequence NULL_OBJ = ExecUtils.mockSeq("NULL_OBJ");

    private NewSequenceDao newSeqDao;
    private GroupSequenceDao groupSeqDao;
    private SimpleSequenceDao simpleSeqDao;

    private NewSequenceScheduler newSeqScheduler;

    private volatile boolean currentNewSeqCacheEnabledOnCN = false;
    private volatile int currentNewSeqCacheSizeOnCN = (int) SequenceAttribute.NEW_SEQ_CACHE_SIZE;
    private volatile boolean newSeqCacheChangedOnCN = false;

    private int unitCount;
    private int unitIndex;
    private int innerStep;
    private int[] customUnitArgs;

    /**
     * Reference to cached sequences used to catch up with explicit insert value
     */
    private Collection<Sequence> cachedSequences = null;
    private ScheduledExecutorService groupSeqCatchers;

    public SequenceLoadFromDBManager(String schemaName, Map<String, Object> connectionProperties) {
        this.schemaName = schemaName;

        this.paramManager = new ParamManager(connectionProperties);

        this.innerStep = this.paramManager.getInt(ConnectionParams.SEQUENCE_STEP);
        if (this.innerStep < 1) {
            this.innerStep = DEFAULT_INNER_STEP;
        }

        this.unitCount = this.paramManager.getInt(ConnectionParams.SEQUENCE_UNIT_COUNT);
        if (this.unitCount < DEFAULT_UNIT_COUNT || this.unitCount > UPPER_LIMIT_UNIT_COUNT) {
            this.unitCount = DEFAULT_UNIT_COUNT;
        }

        this.unitIndex = this.paramManager.getInt(ConnectionParams.SEQUENCE_UNIT_INDEX);
        if (this.unitIndex < DEFAULT_UNIT_INDEX || this.unitIndex >= this.unitCount) {
            this.unitIndex = DEFAULT_UNIT_INDEX;
        }

        customUnitArgs = new int[] {unitCount, unitIndex, innerStep};
    }

    @Override
    public void doInit() {
        // Avoid init sequence from db in mock mode
        if (ConfigDataMode.isFastMock()) {
            return;
        }

        cache = CacheBuilder.newBuilder().build(new CacheLoader<StringIgnoreCase, Sequence>() {
            @Override
            public Sequence load(StringIgnoreCase seqName) {
                return getSequenceInternal(seqName.value);
            }
        });

        if (!ConfigDataMode.needInitMasterModeResource()) {
            return;
        }

        try {
            if (SeqTypeUtil.isNewSeqSupported(schemaName)) {
                NewSequenceDao newSeqDao = new NewSequenceDao();
                newSeqDao.setSchemaName(schemaName);
                newSeqDao.init();
                this.newSeqDao = newSeqDao;

                NewSequenceScheduler.setGroupingTimeout(getNewSeqGroupingTimeout());
                NewSequenceScheduler newSeqScheduler = new NewSequenceScheduler(this.newSeqDao);
                newSeqScheduler.setTaskQueueNum(getNewSeqTaskQueueNum());
                newSeqScheduler.setRequestMergingEnabled(isNewSeqRequestMergingEnabled());
                newSeqScheduler.setValueHandlerKeepAliveTime(getNewSeqValueHandlerKeepAliveTime());
                newSeqScheduler.init();
                this.newSeqScheduler = newSeqScheduler;
            }

            GroupSequenceDao groupSeqDao = new CustomUnitGroupSequenceDao();
            groupSeqDao.setSchemaName(schemaName);
            groupSeqDao.setStep(innerStep);
            groupSeqDao.init();
            this.groupSeqDao = groupSeqDao;

            SimpleSequenceDao simpleSeqDao = new SimpleSequenceDao();
            simpleSeqDao.setSchemaName(schemaName);
            simpleSeqDao.init();
            this.simpleSeqDao = simpleSeqDao;
        } catch (Exception ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_INIT_SEQUENCE_FROM_DB, ex, ex.getMessage());
        }

        if (!isGroupSeqCatcherEnabled()) {
            return;
        }

        long checkInterval = getGroupSeqCheckInterval();
        groupSeqCatchers = DdlHelper.createSingleThreadScheduledPool("GroupSeqCatcher");

        // cachedSequences changes with the change of cache
        cachedSequences = cache.asMap().values();

        // Start timed tasks to check explicit insert values that group sequences have
        // to catch up with.
        groupSeqCatchers.scheduleWithFixedDelay(() -> {
            for (Sequence seq : cachedSequences) {
                if (seq instanceof GroupSequence) {
                    updateGroupSeqValue((GroupSequence) seq);
                }
            }
        }, checkInterval, checkInterval, TimeUnit.SECONDS);
    }

    @Override
    public Sequence getSequence(String schemaName, String name) {
        // Mock sequence in mock mode
        if (ConfigDataMode.isFastMock()) {
            return ExecUtils.mockSeq(name);
        }

        // Cast to uppercase
        StringIgnoreCase seqName = new StringIgnoreCase(name);

        try {
            Sequence seq = cache.get(seqName);

            // The cached sequence may be unexpected.
            if (!isSequenceExpected(seq, name)) {
                // Invalidate cache and try again.
                invalidate(schemaName, name);
                seq = cache.get(seqName);
            }

            if (seq == NULL_OBJ) {
                invalidate(schemaName, name);
            }

            return seq == NULL_OBJ ? null : seq;
        } catch (Throwable e) {
            invalidate(schemaName, name);
            throw GeneralUtil.nestedException(e.getCause());
        }
    }

    @Override
    public void invalidate(String schemaName, String name) {
        StringIgnoreCase seqName = new StringIgnoreCase(name);

        Sequence seq = cache.getIfPresent(seqName);
        invalidate(seq);

        // Invalidate cached sequence object.
        cache.invalidate(seqName);
    }

    @Override
    public int invalidateAll(String schemaName) {
        if (cache == null) {
            return 0;
        }
        int size = (int) cache.size();

        Collection<Sequence> seqs = cache.asMap().values();
        for (Sequence seq : seqs) {
            invalidate(seq);
        }

        // Invalidate all cached sequence objects.
        cache.invalidateAll();

        return size;
    }

    private void invalidate(Sequence seq) {
        if (seq != null) {
            if (seq instanceof TimeBasedSequence) {
                // Remove registered IdGenerator to avoid leak.
                IdGenerator.remove(((TimeBasedSequence) seq).getIdGenerator());
            }
        }
    }

    @Override
    public Type checkIfExists(String schemaName, String seqName) {
        Sequence seq = getSequence(schemaName, seqName);
        if (seq == null || seq == NULL_OBJ) {
            return Type.NA;
        } else {
            return ((BaseSequence) seq).getType();
        }
    }

    @Override
    public boolean isUsingSequence(String schemaName, String tableName) {
        TddlRuleManager rule = OptimizerContext.getContext(schemaName).getRuleManager();
        if (rule != null && (!rule.isTableInSingleDb(tableName) || rule.isBroadCast(tableName))) {
            return true;
        } else {
            String seqName = AUTO_SEQ_PREFIX + tableName;
            return checkIfExists(schemaName, seqName) != Type.NA;
        }
    }

    @Override
    public String getCurrentSeqRange(String schemaName, String seqName) {
        String currentRange = STR_NA;

        Sequence seq = getSequence(schemaName, seqName);

        if (seq != null && seq != NULL_OBJ) {
            long[] currentAndMax;

            if (seq instanceof GroupSequence) {
                currentAndMax = ((GroupSequence) seq).getCurrentAndMax();
            } else if (seq instanceof NewSequenceWithCache) {
                currentAndMax = ((NewSequenceWithCache) seq).getCurrentAndMax();
            } else {
                currentAndMax = new long[] {-1, -1};
            }

            currentRange = "[ " + currentAndMax[0] + ", " + currentAndMax[1] + " ]";
        }

        return currentRange;
    }

    @Override
    public long getMinValueFromCurrentSeqRange(String schemaName, String seqName) {
        Sequence seq = getSequence(schemaName, seqName);

        if (seq != null && seq != NULL_OBJ && seq instanceof GroupSequence) {
            long[] currentAndMax = ((GroupSequence) seq).getCurrentAndMax();
            if (currentAndMax != null && currentAndMax[0] > 0L) {
                return currentAndMax[0];
            }
        }

        return DEFAULT_INNER_STEP;
    }

    @Override
    public int[] getCustomUnitArgsForGroupSeq(String schemaName) {
        return customUnitArgs;
    }

    private boolean isSequenceExpected(Sequence cachedSeq, String seqName) {
        if (cachedSeq == null) {
            return false;
        } else if (cachedSeq == NULL_OBJ) {
            return true;
        } else {
            String cachedSeqName = ((BaseSequence) cachedSeq).getName();
            return TStringUtil.equalsIgnoreCase(cachedSeqName, seqName);
        }
    }

    private Sequence getSequenceInternal(String seqName) {
        // Firstly, attempt to get a group sequence for compatibility.
        Sequence seq = getGroupSequence(seqName);
        if (seq == null || seq == NULL_OBJ) {
            // Then attempt to get other sequences.
            seq = getVariousSequences(seqName);
        }
        if (seq == null) {
            // CacheLoader doesn't support returning a null value.
            seq = NULL_OBJ;
        }
        return seq;
    }

    private Sequence getGroupSequence(String seqName) {
        String sql = "select name from " + SEQUENCE + " where name = ? and schema_name = ?";
        try (Connection metaDbConn = MetaDbUtil.getConnection();
            PreparedStatement ps = metaDbConn.prepareStatement(sql)) {

            ps.setString(1, seqName);
            ps.setString(2, schemaName);

            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return buildGroupSequence(seqName);
                }
                return null;
            }
        } catch (Exception e) {
            boolean ignoreException = false;
            // We should fail and throw exception in most cases because
            // it's possible that there is already an existing group
            // sequence, but the SELECT statement failed to execute or build
            // failed for some reason. In such case, if we proceed with
            // simple sequence, then newly created simple sequence may
            // generate lots of unexpected duplicate value.
            if (e instanceof SQLException) {
                SQLException ex = (SQLException) e;
                // MySQL Error = 1146 and MySQL SQLState = 42S02 indicate
                // that the target table doesn't exist. For the case, we
                // should proceed with SimpleSequence instead of failure.
                if (ex.getErrorCode() == 1146 && ex.getSQLState().equals("42S02")) {
                    ignoreException = true;
                }
            }
            String errMsg = "Failed to build GroupSequence '" + seqName + "'.";
            logger.error(errMsg, e);
            if (!ignoreException) {
                throw new SequenceException(e, errMsg);
            }
            return null;
        }
    }

    private Sequence getVariousSequences(String seqName) {
        String sql = "select cycle from " + SEQUENCE_OPT + " where name = ? and schema_name = ?";
        try (Connection metaDbConn = MetaDbUtil.getConnection();
            PreparedStatement ps = metaDbConn.prepareStatement(sql)) {

            ps.setString(1, seqName);
            ps.setString(2, schemaName);

            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    int flag = rs.getInt(1);

                    if ((flag & NEW_SEQ) == NEW_SEQ) {
                        return buildNewSequence(seqName);
                    } else if ((flag & TIME_BASED) == TIME_BASED) {
                        return buildTimeBasedSequence(seqName);
                    } else {
                        return buildSimpleSequence(seqName);
                    }
                }
                return null;
            }
        } catch (Exception e) {
            boolean ignoreException = false;
            // We should fail and throw exception in most cases because
            // it's possible that there is already an existing simple
            // sequence, but the SELECT statement failed to execute or build
            // failed for some reason. In such case, if we proceed with
            // time-based sequence, then newly created time-based sequence may
            // generate lots of unexpected values.
            if (e instanceof SQLException) {
                SQLException ex = (SQLException) e;
                // MySQL Error = 1146 and MySQL SQLState = 42S02 indicate
                // that the target table doesn't exist. For the case, we
                // should proceed with SimpleSequence instead of failure.
                if (ex.getErrorCode() == 1146 && ex.getSQLState().equals("42S02")) {
                    ignoreException = true;
                }
            }
            String errMsg = "Failed to build SequenceOpt '" + seqName + "'.";
            logger.error(errMsg, e);
            if (!ignoreException) {
                throw new SequenceException(e, errMsg);
            }
            return null;
        }
    }

    private Sequence buildNewSequence(String name) {
        if (!SeqTypeUtil.isNewSeqSupported(schemaName)) {
            return null;
        }

        NewSequence seq = currentNewSeqCacheEnabledOnCN ?
            new NewSequenceWithCache(name, currentNewSeqCacheSizeOnCN, newSeqDao, newSeqScheduler) :
            new NewSequence(name, newSeqDao, newSeqScheduler);

        try {
            seq.setGroupingEnabled(isNewSeqGroupingEnabled());
            if (!ConfigDataMode.needInitMasterModeResource()) {
                // DO NOT initialize to avoid write operations
                // in Read-Only instance.
                seq.setType(Type.NEW);
            } else {
                seq.init();
            }
            logger.info("NewSequence Init: " + seq);
            return seq;
        } catch (TddlRuntimeException e) {
            String errorMsg = e.getMessage();
            if (errorMsg != null && errorMsg.contains("Not found")) {
                return NULL_OBJ;
            }
            throw e;
        }
    }

    private Sequence buildGroupSequence(String name) throws Exception {
        GroupSequence seq = new CustomUnitGroupSequence();
        try {
            seq.setName(name);
            seq.setGroupSequenceDao(groupSeqDao);
            if (!ConfigDataMode.needInitMasterModeResource()) {
                // DO NOT initialize to avoid write operations
                // in Read-Only instance.
                seq.setType(Type.GROUP);
            } else {
                seq.init();
            }
            logger.info("GroupSequence Init: " + seq);
            return seq;
        } catch (TddlRuntimeException e) {
            throw e;
        }
    }

    private Sequence buildSimpleSequence(String name) {
        SimpleSequence seq = new SimpleSequence();
        try {
            seq.setName(name);
            seq.setSimpleSequenceDao(simpleSeqDao);
            if (!ConfigDataMode.needInitMasterModeResource()) {
                // DO NOT initialize to avoid write operations
                // in Read-Only instance.
                seq.setType(Type.SIMPLE);
            } else {
                seq.init();
            }
            logger.info("SimpleSequence Init: " + seq);
            return seq;
        } catch (TddlRuntimeException e) {
            String errorMsg = e.getMessage();
            if (errorMsg != null && errorMsg.contains("Not found")) {
                return NULL_OBJ;
            }
            throw e;
        }
    }

    private Sequence buildTimeBasedSequence(String name) {
        return new TimeBasedSequence(name);
    }

    @Override
    public boolean areAllSequencesSameType(String schemaName, Type[] seqTypes) {
        if (ConfigDataMode.isFastMock()) {
            return true;
        }

        Collection<Sequence> cachedSequences = cache.asMap().values();

        int countNew = 0, countGroup = 0, countSimple = 0, countTime = 0;
        boolean allNew = false, allGroup = false, allSimple = false, allTime = false;

        for (Sequence seq : cachedSequences) {
            if (seq instanceof NewSequence) {
                countNew++;
            } else if (seq instanceof GroupSequence) {
                countGroup++;
            } else if (seq instanceof SimpleSequence) {
                countSimple++;
            } else if (seq instanceof TimeBasedSequence) {
                countTime++;
            }
        }

        if (countNew == 0 && countGroup == 0 && countSimple == 0 && countTime == 0) {
            return true;
        }

        if (countNew > 0 && countGroup == 0 && countSimple == 0 && countTime == 0) {
            allNew = true;
        } else if (countNew == 0 && countGroup > 0 && countSimple == 0 && countTime == 0) {
            allGroup = true;
        } else if (countNew == 0 && countGroup == 0 && countSimple > 0 && countTime == 0) {
            allSimple = true;
        } else if (countNew == 0 && countGroup == 0 && countSimple == 0 && countTime > 0) {
            allTime = true;
        }

        boolean matchedAtLeast = false;

        for (Type seqType : seqTypes) {
            switch (seqType) {
            case NEW:
                matchedAtLeast |= allNew;
            case GROUP:
                matchedAtLeast |= allGroup;
            case SIMPLE:
                matchedAtLeast |= allSimple;
            case TIME:
                matchedAtLeast |= allTime;
            }
        }

        return matchedAtLeast;
    }

    private void updateGroupSeqValue(GroupSequence groupSeq) {
        String seqName = groupSeq.getName();
        try {
            if (groupSeq.needToUpdateValue()) {
                long minValueInAllRanges = getMinValueFromAllRanges(seqName);
                boolean needSync = groupSeq.updateValueRegularly(minValueInAllRanges);
                if (needSync) {
                    SyncManagerHelper.sync(new SequenceSyncAction(schemaName, seqName), schemaName);
                }
            }
        } catch (Throwable t) {
            logger.error("Failed to update explicit value for sequence '" + seqName + "' in " + schemaName, t);
        }
    }

    private long getMinValueFromAllRanges(String seqName) {
        long minValue = DEFAULT_INNER_STEP;
        try {
            List<List<Map<String, Object>>> resultSets =
                SyncManagerHelper.sync(new InspectGroupSeqMinValueSyncAction(schemaName, seqName), schemaName);
            if (resultSets != null && resultSets.size() > 0) {
                for (List<Map<String, Object>> resultSet : resultSets) {
                    if (resultSet != null && resultSet.size() > 0) {
                        for (Map<String, Object> row : resultSet) {
                            long minValueInRange = (long) row.get(GROUP_SEQ_MIN_VALUE);
                            if (minValue <= DEFAULT_INNER_STEP) {
                                minValue = minValueInRange;
                            } else if (minValueInRange < minValue) {
                                minValue = minValueInRange;
                            }
                        }
                    }
                }
                if (minValue < DEFAULT_INNER_STEP) {
                    minValue = DEFAULT_INNER_STEP;
                }
            }
        } catch (Throwable t) {
            logger.error("Failed to get min value from all ranges. Caused by: " + t.getMessage(), t);
        }
        return minValue;
    }

    @Override
    public synchronized void reloadConnProps(String schemaName, Map<String, Object> connProps) {
        this.paramManager = new ParamManager(connProps);
        NewSequenceScheduler.resetGroupingTimeout(getNewSeqGroupingTimeout());
        if (newSeqScheduler != null) {
            newSeqScheduler.resetTaskQueueNum(getNewSeqTaskQueueNum());
            newSeqScheduler.resetRequestMergingEnabled(isNewSeqRequestMergingEnabled());
            newSeqScheduler.resetValueHandlerKeepAliveTime(getNewSeqValueHandlerKeepAliveTime());
        }
        resetNewSeqCacheOnCN();
    }

    @Override
    public void resetNewSeqResources(String schemaName) {
        if (newSeqScheduler != null) {
            newSeqScheduler.resetQueuesAndHandlers(false);
        }
    }

    private boolean isNewSeqGroupingEnabled() {
        return this.paramManager.getBoolean(ConnectionParams.ENABLE_NEW_SEQ_GROUPING);
    }

    private long getNewSeqGroupingTimeout() {
        return this.paramManager.getLong(ConnectionParams.NEW_SEQ_GROUPING_TIMEOUT);
    }

    private int getNewSeqTaskQueueNum() {
        return this.paramManager.getInt(ConnectionParams.NEW_SEQ_TASK_QUEUE_NUM_PER_DB);
    }

    private boolean isNewSeqRequestMergingEnabled() {
        return this.paramManager.getBoolean(ConnectionParams.ENABLE_NEW_SEQ_REQUEST_MERGING);
    }

    private long getNewSeqValueHandlerKeepAliveTime() {
        return this.paramManager.getLong(ConnectionParams.NEW_SEQ_VALUE_HANDLER_KEEP_ALIVE_TIME);
    }

    private boolean isGroupSeqCatcherEnabled() {
        return this.paramManager.getBoolean(ConnectionParams.ENABLE_GROUP_SEQ_CATCHER);
    }

    private long getGroupSeqCheckInterval() {
        return this.paramManager.getLong(ConnectionParams.GROUP_SEQ_CHECK_INTERVAL);
    }

    private void resetNewSeqCacheOnCN() {
        resetNewSeqCacheEnabledOnCN();
        resetNewSeqCacheSizeOnCN();
        if (this.newSeqCacheChangedOnCN) {
            invalidateNewSeqObjects();
            this.newSeqCacheChangedOnCN = false;
        }
    }

    private void resetNewSeqCacheEnabledOnCN() {
        boolean newSeqCacheEnabledOnCN = this.paramManager.getBoolean(ConnectionParams.ENABLE_NEW_SEQ_CACHE_ON_CN);
        if (this.currentNewSeqCacheEnabledOnCN != newSeqCacheEnabledOnCN) {
            this.currentNewSeqCacheEnabledOnCN = newSeqCacheEnabledOnCN;
            this.newSeqCacheChangedOnCN = true;
            logger.warn(String.format("Reset New Seq Cache Enabled On CN from %s to %s",
                this.currentNewSeqCacheEnabledOnCN, newSeqCacheEnabledOnCN));
        }
    }

    private void resetNewSeqCacheSizeOnCN() {
        int newSeqCacheSizeOnCN = (int) this.paramManager.getLong(ConnectionParams.NEW_SEQ_CACHE_SIZE_ON_CN);
        if (newSeqCacheSizeOnCN > 0 && this.currentNewSeqCacheSizeOnCN != newSeqCacheSizeOnCN) {
            this.currentNewSeqCacheSizeOnCN = newSeqCacheSizeOnCN;
            this.newSeqCacheChangedOnCN = true;
            logger.warn(String.format("Reset New Seq Cache Size On CN from %s to %s", this.currentNewSeqCacheSizeOnCN,
                newSeqCacheSizeOnCN));
        }
    }

    private void invalidateNewSeqObjects() {
        Collection<Sequence> cachedSequences = cache.asMap().values();
        for (Sequence seq : cachedSequences) {
            if (seq instanceof NewSequence) {
                StringIgnoreCase seqName = new StringIgnoreCase(((NewSequence) seq).getName());
                cache.invalidate(seqName);
            }
        }
    }

    @Override
    protected void doDestroy() {
        invalidateAll(schemaName);

        try {
            if (newSeqDao != null) {
                newSeqDao.destroy();
            }
        } catch (Throwable e) {
            logger.error(e);
        }

        try {
            if (newSeqScheduler != null) {
                newSeqScheduler.destroy();
            }
        } catch (Throwable e) {
            logger.error(e);
        }

        try {
            if (groupSeqDao != null) {
                groupSeqDao.destroy();
            }
        } catch (Throwable e) {
            logger.error(e);
        }

        if (!ConfigDataMode.isFastMock()) {
            try {
                simpleSeqDao.destroy();
            } catch (Throwable e) {
                logger.error(e);
            }
        }

        if (cache != null) {
            cache.cleanUp();
        }

        if (groupSeqCatchers != null) {
            try {
                groupSeqCatchers.shutdown();
                groupSeqCatchers = null;
            } catch (Throwable e) {
                logger.error(e);
            }
        }
    }

    private static class StringIgnoreCase {

        private String value;

        public StringIgnoreCase(String value) {
            this.value = value;
        }

        @Override
        public int hashCode() {
            // Always compare with uppercase
            String value = StringUtils.upperCase(this.value);
            final int prime = 31;
            int result = 1;
            result = prime * result + ((value == null) ? 0 : value.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            StringIgnoreCase other = (StringIgnoreCase) obj;
            if (value == null) {
                if (other.value != null) {
                    return false;
                }
            } else if (!StringUtils.equalsIgnoreCase(value, other.value)) {
                return false;
            }
            return true;
        }
    }

}

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
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.sync.InspectGroupSeqMinValueSyncAction;
import com.alibaba.polardbx.executor.sync.SequenceSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.sequence.ISequenceManager;
import com.alibaba.polardbx.sequence.Sequence;
import com.alibaba.polardbx.sequence.SequenceDao;
import com.alibaba.polardbx.sequence.exception.SequenceException;
import com.alibaba.polardbx.sequence.impl.BaseSequence;
import com.alibaba.polardbx.sequence.impl.CustomUnitGroupSequence;
import com.alibaba.polardbx.sequence.impl.CustomUnitGroupSequenceDao;
import com.alibaba.polardbx.sequence.impl.DefaultSequence;
import com.alibaba.polardbx.sequence.impl.GroupSequence;
import com.alibaba.polardbx.sequence.impl.GroupSequenceDao;
import com.alibaba.polardbx.sequence.impl.SimpleSequence;
import com.alibaba.polardbx.sequence.impl.SimpleSequenceDao;
import com.alibaba.polardbx.sequence.impl.TimeBasedSequence;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.commons.lang.StringUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.alibaba.polardbx.common.constants.SequenceAttribute.CACHE_ENABLED;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_GROUP_TABLE_NAME;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_INNER_STEP;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_TABLE_NAME;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_UNIT_COUNT;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_UNIT_INDEX;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.GROUP_SEQ_MIN_VALUE;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.GROUP_SEQ_UPDATE_INTERVAL;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.STR_NA;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.TIME_BASED;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.Type;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.UPPER_LIMIT_UNIT_COUNT;

/**
 * 直接读取sequence表，生成sequence配置
 *
 * @author mengshi.sunmengshi 2014年5月7日 下午2:08:15
 * @since 5.1.0
 */
public class SequenceLoadFromDBManager extends AbstractSequenceManager {

    private final static Logger logger = LoggerFactory
        .getLogger(SequenceLoadFromDBManager.class);
    private LoadingCache<StringIgnoreCase, Sequence> cache = null;
    private String appName = null;
    private String schemaName = null;
    private String unitName = null;
    private TddlRuleManager rule;
    private Sequence NULL_OBJ = new DefaultSequence();

    private DataSource dsForGroupCheck = null;
    private DataSource dsForSimpleCheck = null;

    /**
     * For Group Sequence
     */
    private String groupSeqGroupKey;
    private String groupSeqTable;
    private SequenceDao groupSeqDao;
    private int unitCount;
    private int unitIndex;
    private int step;
    private int[] customUnitArgs;

    /**
     * For Simple Sequence
     */
    private String simpleSeqGroupKey;
    private String simpleSeqTable;
    private SequenceDao simpleSeqDao;

    private boolean customUnitGroupSeqSupported = false;

    /**
     * Reference to cached sequences used to catch up with explicit insert value
     * regularly (5 minutes by default).
     */
    private Collection<Sequence> cachedSequences = null;

    private ScheduledExecutorService groupSeqCatchers;

    private long checkInterval = GROUP_SEQ_UPDATE_INTERVAL;

    public SequenceLoadFromDBManager(String appName, String schemaName, String unitName, TddlRuleManager rule,
                                     Map<String, Object> connectionProperties) {
        this.appName = appName;
        this.schemaName = schemaName;
        this.unitName = unitName;
        this.rule = rule;

        this.step = (int) GeneralUtil
            .getPropertyLong(connectionProperties, ConnectionProperties.SEQUENCE_STEP, DEFAULT_INNER_STEP);
        if (this.step < 1) {
            this.step = DEFAULT_INNER_STEP;
        }

        if (ConfigDataMode.isMasterMode()) {
            checkInterval = GeneralUtil.getPropertyLong(connectionProperties,
                ConnectionProperties.GROUP_SEQ_CHECK_INTERVAL,
                GROUP_SEQ_UPDATE_INTERVAL);

            this.customUnitGroupSeqSupported = true;
        }

        if (customUnitGroupSeqSupported) {
            this.unitCount = (int) GeneralUtil
                .getPropertyLong(connectionProperties, ConnectionProperties.SEQUENCE_UNIT_COUNT, DEFAULT_UNIT_COUNT);
            if (this.unitCount < DEFAULT_UNIT_COUNT || this.unitCount > UPPER_LIMIT_UNIT_COUNT) {
                this.unitCount = DEFAULT_UNIT_COUNT;
            }

            this.unitIndex = (int) GeneralUtil
                .getPropertyLong(connectionProperties, ConnectionProperties.SEQUENCE_UNIT_INDEX, DEFAULT_UNIT_INDEX);
            if (this.unitIndex < DEFAULT_UNIT_INDEX || this.unitIndex >= this.unitCount) {
                this.unitIndex = DEFAULT_UNIT_INDEX;
            }

            customUnitArgs = new int[] {unitCount, unitIndex, step};
        }
    }

    @Override
    public void doInit() {
        // Avoid init sequence from db in mock mode
        if (ConfigDataMode.isFastMock()) {
            return;
        }
        cache = CacheBuilder.newBuilder().build(new CacheLoader<StringIgnoreCase, Sequence>() {
            @Override
            public Sequence load(StringIgnoreCase seqName) throws Exception {
                return getSequenceInternal(seqName.value);
            }
        });

        // We only use one group to generate sequence values in DRDS mode.
        groupSeqTable = DEFAULT_GROUP_TABLE_NAME;
        simpleSeqTable = DEFAULT_TABLE_NAME;

        this.groupSeqGroupKey = SequenceAttribute.GMS_META_DB_KEY;
        this.simpleSeqGroupKey = SequenceAttribute.GMS_META_DB_KEY;

        try {
            dsForGroupCheck = MetaDbDataSource.getInstance().getDataSource();
            dsForSimpleCheck = MetaDbDataSource.getInstance().getDataSource();

            GroupSequenceDao groupSeqDao;
            if (customUnitGroupSeqSupported) {
                groupSeqDao = new CustomUnitGroupSequenceDao();
            } else {
                groupSeqDao = new GroupSequenceDao();
            }

            List<String> groupKeys = new ArrayList<>();
            groupKeys.add(groupSeqGroupKey);
            groupSeqDao.setDbGroupKeys(groupKeys);
            groupSeqDao.setSchemaName(schemaName);
            groupSeqDao.setAppName(this.appName);
            groupSeqDao.setUnitName(this.unitName);
            groupSeqDao.setTableName(groupSeqTable);
            groupSeqDao.setAdjust(true);
            groupSeqDao.setDscount(1);
            groupSeqDao.setInnerStep(step);
            groupSeqDao.init();
            this.groupSeqDao = groupSeqDao;

            SimpleSequenceDao simpleSeqDao = new SimpleSequenceDao();
            simpleSeqDao.setDbGroupKey(simpleSeqGroupKey);
            simpleSeqDao.setAppName(appName);
            simpleSeqDao.setSchemaName(schemaName);
            simpleSeqDao.setUnitName(unitName);
            simpleSeqDao.setTableName(simpleSeqTable);
            simpleSeqDao.init();
            this.simpleSeqDao = simpleSeqDao;
        } catch (Exception ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_INIT_SEQUENCE_FROM_DB, ex, ex.getMessage());
        }

        groupSeqCatchers =
            Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("GroupSeqCatcher", true));

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
        return getSequence(schemaName, name, true);
    }

    public Sequence getSequence(String schemaName, String name, boolean buildSeqIfNotExists) {
        // mock sequence in mock mode
        if (ConfigDataMode.isFastMock()) {
            return ExecUtils.mockSeq(name);
        }
        // 强制转为大写
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
                if (buildSeqIfNotExists && name.startsWith(ISequenceManager.AUTO_SEQ_PREFIX)) {
                    // If there is no any sequence found, then try to build a
                    // GROUP sequence when it's an AUTO_SEQ_xxx sequence.
                    // NOTE that if a table is created via DRDS console that
                    // doesn't connect to DRDS to execute DDLs, then the
                    // corresponding sequence isn't created automatically, so
                    // that we have to rely on the way here to create a GROUP
                    // sequence by default.
                    seq = buildGroupSequence(name);
                    cache.put(seqName, seq);
                }
            }

            if (seq == NULL_OBJ) {
                return null;
            } else {
                return seq;
            }
        } catch (Throwable e) {
            invalidate(schemaName, name);
            throw GeneralUtil.nestedException(e.getCause());
        }
    }

    @Override
    public void invalidate(String schemaName, String name) {
        StringIgnoreCase seqName = new StringIgnoreCase(name);
        Sequence seq = cache.getIfPresent(seqName);
        if (seq != null && seq instanceof TimeBasedSequence) {
            // Remove registered IdGenerator to avoid leak.
            IdGenerator.remove(((TimeBasedSequence) seq).getIdGenerator());
        }
        // Invalidate cached sequence object.
        cache.invalidate(seqName);
        // Invalidate cached sequence attributes.
        ((SimpleSequenceDao) simpleSeqDao).invalidate(name);
    }

    @Override
    public int invalidateAll(String schemaName) {
        int size = (int) cache.size();
        Collection<Sequence> seqs = cache.asMap().values();
        for (Sequence seq : seqs) {
            if (seq instanceof TimeBasedSequence) {
                // Remove registered IdGenerator to avoid leak.
                IdGenerator.remove(((TimeBasedSequence) seq).getIdGenerator());
            }
        }
        // Invalidate all cached sequence objects.
        cache.invalidateAll();
        // Invalidate cached sequence attributes for all sequence objects.
        ((SimpleSequenceDao) simpleSeqDao).invalidateAll();
        return size;
    }

    @Override
    public void validateDependence(String schemaName) {
    }

    @Override
    public Type checkIfExists(String schemaName, String seqName) {
        Sequence seq = getSequence(schemaName, seqName, false);
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

        Sequence seq = getSequence(schemaName, seqName, false);

        if (seq != null && seq != NULL_OBJ && seq instanceof GroupSequence) {
            long[] currentAndMax = ((GroupSequence) seq).getCurrentAndMax();
            currentRange = "[ " + currentAndMax[0] + ", " + currentAndMax[1] + " ]";
        }

        return currentRange;
    }

    @Override
    public long getMinValueFromCurrentSeqRange(String schemaName, String seqName) {
        Sequence seq = getSequence(schemaName, seqName, false);
        if (seq != null && seq != NULL_OBJ && seq instanceof GroupSequence) {
            long[] currentAndMax = ((GroupSequence) seq).getCurrentAndMax();
            if (currentAndMax != null && currentAndMax[0] > 0L) {
                return currentAndMax[0];
            }
        }
        return DEFAULT_INNER_STEP;
    }

    @Override
    public boolean isCustomUnitGroupSeqSupported(String schemaName) {
        return customUnitGroupSeqSupported;
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
            return (cachedSeq instanceof SimpleSequence
                && TStringUtil.equalsIgnoreCase(((SimpleSequence) cachedSeq).getName(), seqName))
                || (cachedSeq instanceof GroupSequence
                && TStringUtil.equalsIgnoreCase(((GroupSequence) cachedSeq).getName(), seqName))
                || (cachedSeq instanceof TimeBasedSequence
                && TStringUtil.equalsIgnoreCase(((TimeBasedSequence) cachedSeq).getName(), seqName));
        }
    }

    private Sequence getSequenceInternal(String seqName) {
        // Firstly, attempt to get a group sequence for compatibility.
        Sequence seq = getGroupSequence(seqName, false);
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

    /**
     * Attempt to get a group sequence object
     *
     * @param isLegacy True - means Group Sequence only, False - means that Simple
     * Sequence or Time-based Sequence is enabled and supports Group Sequence
     * for compatibility.
     */
    private Sequence getGroupSequence(String seqName, boolean isLegacy) {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            String sql = "select name from " + groupSeqTable + " where name = ?";
            sql += " and schema_name = ?";

            conn = dsForGroupCheck.getConnection();
            stmt = conn.prepareStatement(sql);

            stmt.setString(1, seqName);
            stmt.setString(2, schemaName);

            rs = stmt.executeQuery();

            if (rs.next()) {
                return buildGroupSequence(seqName);
            }

            if (isLegacy) {
                if (seqName.startsWith(ISequenceManager.AUTO_SEQ_PREFIX)) {
                    // 如果是数据库自增id,自动创建一个sequence
                    return buildGroupSequence(seqName);
                }
                return NULL_OBJ;
            }

            return null;
        } catch (Exception e) {
            if (isLegacy) {
                // not exists sequence table on default db index
                if (e.getMessage() != null && e.getMessage().contains("doesn't exist")) {
                    if (ConfigDataMode.isSlaveMode()) {
                        return NULL_OBJ;
                    }
                    throw new TddlRuntimeException(ErrorCode.ERR_MISS_SEQUENCE_TABLE_ON_DEFAULT_DB);
                } else if (e.getMessage() != null && e.getMessage().contains("Unknown column")) {
                    throw new TddlRuntimeException(ErrorCode.ERR_SEQUENCE_TABLE_META);
                }
                throw new TddlRuntimeException(ErrorCode.ERR_OTHER_WHEN_BUILD_SEQUENCE, e, e.getMessage());
            } else {
                boolean ignoreException = false;
                // We should fail and throw exception in most of cases because
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
        } finally {
            close(rs);
            close(stmt);
            close(conn);
        }
    }

    /**
     *
     */
    private Sequence getVariousSequences(String seqName) {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {

            String sql = "select cycle from " + simpleSeqTable + " where name = ?";
            sql += " and schema_name = ?";

            conn = dsForSimpleCheck.getConnection();
            stmt = conn.prepareStatement(sql);

            stmt.setString(1, seqName);
            stmt.setString(2, schemaName);

            rs = stmt.executeQuery();
            if (rs.next()) {
                int flag = rs.getInt(1);
                boolean isTimeBased = (flag & TIME_BASED) == TIME_BASED;
                boolean isCached = (flag & CACHE_ENABLED) == CACHE_ENABLED;
                if (isTimeBased) {
                    return new TimeBasedSequence(seqName);
                } else {
                    return buildSimpleSequence(seqName, isCached);
                }
            }
            return null;
        } catch (Exception e) {
            boolean ignoreException = false;
            // We should fail and throw exception in most of cases because
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
            String errMsg = "Failed to build SimpleSequence '" + seqName + "'.";
            logger.error(errMsg, e);
            if (!ignoreException) {
                throw new SequenceException(e, errMsg);
            }
            return null;
        } finally {
            close(rs);
            close(stmt);
            close(conn);
        }
    }

    private Sequence buildGroupSequence(String name) throws Exception {
        GroupSequence seq;
        if (customUnitGroupSeqSupported) {
            seq = new CustomUnitGroupSequence();
        } else {
            seq = new GroupSequence();
        }
        try {
            seq.setName(name);
            seq.setSequenceDao(groupSeqDao);
            if (ConfigDataMode.isSlaveMode()) {
                // DO NOT initialize to avoid write operations
                // in Read-Only instance.
                seq.setType(Type.GROUP);
            } else {
                seq.init();
            }
            logger.info("GroupSequence Init: " + seq.toString());
            return seq;
        } catch (TddlRuntimeException e) {
            throw e;
        }
    }

    private Sequence buildSimpleSequence(String name, boolean isCached) throws Exception {
        SimpleSequence seq = new SimpleSequence();
        try {
            seq.setName(name);
            seq.setSequenceDao(simpleSeqDao);
            if (ConfigDataMode.isSlaveMode()) {
                // DO NOT initialize to avoid write operations
                // in Read-Only instance.
                seq.setType(Type.SIMPLE);
            } else {
                seq.init();
            }
            logger.info("SimpleSequence Init: " + seq.toString());
            return seq;
        } catch (TddlRuntimeException e) {
            String errorMsg = e.getMessage();
            if (errorMsg != null && errorMsg.contains("Not found")) {
                return NULL_OBJ;
            }
            throw e;
        }
    }

    @Override
    public boolean areAllSequencesSameType(String schemaName, Type seqType) {
        if (ConfigDataMode.isFastMock()) {
            return true;
        }

        int countGroup = 0, countSimple = 0, countTime = 0;

        // Check for group sequences.
        if (dsForGroupCheck != null) {
            try (Connection conn = dsForGroupCheck.getConnection(); Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("select count(*) from " + groupSeqTable)) {
                if (rs.next()) {
                    countGroup = rs.getInt(1);
                }
            } catch (SQLException e) {
                throw new SequenceException(e, "Failed to query '" + groupSeqTable + "'. Caused by: " + e.getMessage());
            }
        }

        // Check for simple and time-based sequences.
        if (dsForSimpleCheck != null) {
            try (Connection conn = dsForSimpleCheck.getConnection(); Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("select cycle from " + simpleSeqTable)) {
                while (rs.next()) {
                    int flag = rs.getInt(1);
                    if ((flag & TIME_BASED) == TIME_BASED) {
                        countTime++;
                    } else {
                        countSimple++;
                    }
                }
            } catch (SQLException e) {
                throw new SequenceException(e, "Failed to query '" + groupSeqTable + "'. Caused by: " + e.getMessage());
            }
        }

        if (countGroup == 0 && countSimple == 0 && countTime == 0) {
            // No sequence.
            return true;
        }

        switch (seqType) {
        case GROUP:
            return countGroup > 0 && countSimple == 0 && countTime == 0;
        case SIMPLE:
            return countGroup == 0 && countSimple > 0 && countTime == 0;
        case TIME:
            return countGroup == 0 && countSimple == 0 && countTime > 0;
        default:
            return false;
        }
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
    protected void doDestroy() {
        if (dsForGroupCheck instanceof TGroupDataSource) {
            try {
                ((TGroupDataSource) dsForGroupCheck).destroy();
            } catch (Throwable e) {
                logger.error(e);
            }
        }

        try {
            if (groupSeqDao != null) {
                groupSeqDao.destroy();
            }
        } catch (Throwable e) {
            logger.error(e);
        }

        if (!ConfigDataMode.isFastMock()) {
            if (dsForSimpleCheck instanceof TGroupDataSource) {
                try {
                    ((TGroupDataSource) dsForSimpleCheck).destroy();
                } catch (Throwable e) {
                    logger.error(e);
                }
            }

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

    private static void close(ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                logger.debug("Could not close JDBC ResultSet.", e);
            } catch (Throwable e) {
                logger.debug("Unexpected exception on closing JDBC ResultSet.", e);
            }
        }
    }

    private static void close(Statement stmt) {
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) {
                logger.debug("Could not close JDBC Statement.", e);
            } catch (Throwable e) {
                logger.debug("Unexpected exception on closing JDBC Statement.", e);
            }
        }
    }

    private static void close(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                logger.debug("Could not close JDBC Connection.", e);
            } catch (Throwable e) {
                logger.debug("Unexpected exception on closing JDBC Connection.", e);
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
            // 全部按照大写来做对比
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
                // 忽略大小写
                return false;
            }
            return true;
        }
    }

}

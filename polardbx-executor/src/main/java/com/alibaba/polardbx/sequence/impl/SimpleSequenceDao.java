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

package com.alibaba.polardbx.sequence.impl;

import com.alibaba.polardbx.common.constants.SequenceAttribute;
import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.logger.LoggerInit;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.sequence.SequenceDao;
import com.alibaba.polardbx.sequence.SequenceRange;
import com.alibaba.polardbx.sequence.exception.SequenceException;
import com.alibaba.polardbx.sequence.util.SequenceHelper;
import org.apache.commons.lang.StringUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A simple sequence DAO to maintain sequence value.
 *
 * @author chensr 2016年4月12日 上午11:15:46
 * @since 5.0.0
 */
public class SimpleSequenceDao extends AbstractLifecycle implements SequenceDao {

    private static final Logger logger = LoggerFactory.getLogger(SimpleSequenceDao.class);
    protected int retryTimes = SequenceAttribute.DEFAULT_RETRY_TIMES;
    protected boolean check = SequenceAttribute.DEFAULT_CHECK;

    protected String tableName = SequenceAttribute.DEFAULT_TABLE_NAME;
    protected String nameColumn = SequenceAttribute.DEFAULT_NAME_COLUMN;
    protected String valueColumn = SequenceAttribute.DEFAULT_VALUE_COLUMN;
    protected String incrementByColumn = SequenceAttribute.DEFAULT_INCREMENT_BY_COLUMN;
    protected String startWithColumn = SequenceAttribute.DEFAULT_START_WITH_COLUMN;
    protected String maxValueColumn = SequenceAttribute.DEFAULT_MAX_VALUE_COLUMN;
    protected String cycleColumn = SequenceAttribute.DEFAULT_CYCLE_COLUMN;
    protected String gmtCreatedColumn = SequenceAttribute.DEFAULT_GMT_CREATED_COLUMN;
    protected String gmtModifiedColumn = SequenceAttribute.DEFAULT_GMT_MODIFIED_COLUMN;

    protected int incrementBy = SequenceAttribute.DEFAULT_INCREMENT_BY;
    protected long startWith = SequenceAttribute.DEFAULT_START_WITH;
    protected long maxValue = SequenceAttribute.DEFAULT_MAX_VALUE;
    protected int cycle = SequenceAttribute.NOCYCLE;
    protected int cache = SequenceAttribute.CACHE_DISABLED;

    protected ConcurrentMap<String, SeqAttr> seqAttrCaches = new ConcurrentHashMap<String, SeqAttr>();

    protected DataSource dataSource;

    protected String dbGroupKey;
    protected String appName;
    protected String schemaName;
    protected String unitName;

    protected String configStr;

    public SimpleSequenceDao() {
    }

    @Override
    public void doInit() {
        if (StringUtils.isEmpty(appName)) {
            SequenceException e = new SequenceException("appName is null or empty!");
            logger.error("appName is not set!", e);
            throw e;
        }

        if (StringUtils.isEmpty(dbGroupKey)) {
            SequenceException e = new SequenceException("dbGroupKey is null or empty!");
            logger.error("dbGroupKey is not set!", e);
            throw e;
        }

        dataSource = MetaDbDataSource.getInstance().getDataSource();

        LoggerInit.TDDL_SEQUENCE_LOG.info(getConfigStr());
    }

    @Override
    public void doDestroy() {
        if (dataSource instanceof TGroupDataSource) {
            ((TGroupDataSource) dataSource).destroy();
        }
    }

    public void validate(String seqName) {
        // Always validate sequence entry with the input seqName.
        // If there is no corresponding row in the sequence table,
        // then we should insert new one with initial value.
        validateEntry(seqName);
    }

    public void invalidate(String seqName) {
        if (seqName != null) {
            seqAttrCaches.remove(seqName);
        }
    }

    public void invalidateAll() {
        seqAttrCaches.clear();
    }

    public long nextValue(String seqName, int size) {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        String errPrefix = "Failed to get next value for sequence '" + seqName + "' - ";
        try {
            long newValue = 0L;
            conn = SequenceHelper.getConnection(dataSource);
            // Do some protection first.
            size = size <= 0 ? 1 : size;
            stmt = conn.prepareStatement(getNextValueSql(seqName, tableName, size), Statement.RETURN_GENERATED_KEYS);

            stmt.setString(1, seqName);
            stmt.setString(2, schemaName);

            int updateCount = stmt.executeUpdate();
            if (updateCount == 1) {
                rs = stmt.getGeneratedKeys();
                if (rs.next()) {
                    newValue = rs.getLong(1);
                    if (newValue == 0) {
                        String errMsg = errPrefix + "unexpected generated key '0'.";
                        logger.error(errMsg);
                        throw new SequenceException(errMsg);
                    }
                } else {
                    SeqAttr seqAttr = getCachedSeqAttr(seqName);
                    String errMsg = errPrefix + "requested sequence value(s) with size " + size
                        + " exceeds maximum value allowed " + seqAttr.maxValue + ".";
                    logger.error(errMsg);
                    throw new SequenceException(errMsg);
                }
            } else {
                String errMsg = errPrefix + "nothing updated.";
                logger.error(errMsg);
                throw new SequenceException(errMsg);
            }
            return newValue;
        } catch (SQLException e) {
            String errMsg = errPrefix + e.getMessage();
            logger.error(errMsg, e);
            throw new SequenceException(e, errMsg);
        } finally {
            SequenceHelper.closeDbResources(rs, stmt, conn);
        }
    }

    public void updateValue(String seqName, long value) {
        Connection conn = null;
        try {
            conn = SequenceHelper.getConnection(dataSource);
            updateValueInternal(conn, seqName, value);
        } catch (SQLException e) {
            String errMsg = "Failed to get connection when trying to update sequence value " + value + ".";
            logger.error(errMsg, e);
            throw new SequenceException(e, errMsg);
        } finally {
            SequenceHelper.closeDbResources(null, null, conn);
        }

    }

    protected void updateValueInternal(Connection conn, String seqName, long value) {
        SeqAttr seqAttr = getCachedSeqAttr(seqName);
        if (value > seqAttr.maxValue) {
            String errMsg = "Attempting to update sequence value " + value + " that exceeds maximum value allowed "
                + seqAttr.maxValue + ".";
            logger.error(errMsg);
            throw new SequenceException(errMsg);
        }
        PreparedStatement stmt = null;
        try {
            stmt = conn.prepareStatement(getUpdateValueSql(seqName));

            stmt.setLong(1, value);
            stmt.setString(2, seqName);
            stmt.setLong(3, value);
            stmt.setString(4, schemaName);

            // Update the sequence value only when new value > current value.
            // Don't care about return code.
            stmt.executeUpdate();
        } catch (SQLException e) {
            String errMsg = "Failed to update sequence value " + value + ".";
            logger.error(errMsg, e);
            throw new SequenceException(e, errMsg);
        } finally {
            SequenceHelper.closeDbResources(null, stmt, null);
        }
    }

    public void validateTable() {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = SequenceHelper.getConnection(dataSource);
            stmt = conn.prepareStatement(getTableCheckSql());
            stmt.setString(1, tableName);

            rs = stmt.executeQuery();

            if (rs.next()) {
                if (rs.getInt(1) == 0) {
                    // The table doesn't exist, then create it.
                    createNewTable(conn);
                }
            } else {
                // This should never occur.
                String errMsg = "Unexpected: Failed to get COUNT info from system table for sequence table.";
                logger.error(errMsg);
                throw new SequenceException(errMsg);
            }
        } catch (SQLException e) {
            String errMsg = "Failed to validate sequence table '" + tableName + "'.";
            logger.error(errMsg, e);
            throw new SequenceException(e, errMsg);
        } finally {
            SequenceHelper.closeDbResources(rs, stmt, conn);
        }
    }

    private void createNewTable(Connection conn) {
        PreparedStatement stmt = null;
        try {
            stmt = conn.prepareStatement(getTableCreateSql());
            stmt.executeUpdate();
        } catch (SQLException e) {
            String errMsg = "Failed to create sequence table '" + tableName + "'.";
            logger.error(errMsg, e);
            throw new SequenceException(e, errMsg);
        } finally {
            SequenceHelper.closeDbResources(null, stmt, null);
        }
    }

    private void validateEntry(String seqName) {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = SequenceHelper.getConnection(dataSource);
            stmt = conn.prepareStatement(getEntryCheckSql());

            stmt.setString(1, seqName);
            stmt.setString(2, schemaName);

            rs = stmt.executeQuery();

            if (rs.next()) {
                populateAttributes(seqName, rs);
            } else if (seqName.toUpperCase().startsWith(SequenceAttribute.AUTO_SEQ_PREFIX)) {
                // Try to insert new one if AUTO_SEQ_xxx.
                insertNewEntry(conn, seqName);
            } else {
                throw new SequenceException(
                    "Sequence '" + seqName + "' doesn't exist. Please double check and try again.");
            }
        } catch (SQLException e) {
            logger.error("Failed to validate sequence '" + seqName + "'.", e);

            String errMsg = e.getMessage();
            if (errMsg != null && errMsg.contains("doesn't exist")) {
                throw new TddlRuntimeException(ErrorCode.ERR_MISS_SEQUENCE_TABLE_ON_DEFAULT_DB);
            } else if (errMsg != null && errMsg.contains("Unknown column")) {
                throw new TddlRuntimeException(ErrorCode.ERR_SEQUENCE_TABLE_META);
            }

            throw new TddlRuntimeException(ErrorCode.ERR_OTHER_WHEN_BUILD_SEQUENCE, e, errMsg);
        } finally {
            SequenceHelper.closeDbResources(rs, stmt, conn);
        }
    }

    private void populateAttributes(String seqName, ResultSet rs) throws SQLException {
        SeqAttr seqAttr = seqAttrCaches.get(seqName);
        if (seqAttr == null) {
            seqAttr = new SeqAttr();
            seqAttrCaches.put(seqName, seqAttr);
        }
        seqAttr.incrementBy = rs.getInt(1);
        seqAttr.startWith = rs.getLong(2);
        seqAttr.maxValue = rs.getLong(3);
        int flag = rs.getInt(4);
        seqAttr.cycle = flag & SequenceAttribute.CYCLE;
        seqAttr.cache = flag & SequenceAttribute.CACHE_ENABLED;
    }

    private void insertNewEntry(Connection conn, String seqName) {
        PreparedStatement stmt = null;
        try {
            stmt = conn.prepareStatement(getEntryInsertSql(tableName));

            int index = 0;
            stmt.setString(++index, schemaName);
            stmt.setString(++index, seqName);
            // We use general default values when inserting a new entry.
            stmt.setLong(++index, this.startWith);
            stmt.setInt(++index, this.incrementBy);
            stmt.setLong(++index, this.startWith);
            stmt.setLong(++index, this.maxValue);
            stmt.setInt(++index, this.cycle);

            int insertCount = stmt.executeUpdate();
            if (insertCount == 0) {
                String errMsg = "Sequence '" + seqName + "' wasn't inserted.";
                logger.error(errMsg);
                throw new SequenceException(errMsg);
            }

            // Initialize cached sequence attributes.
            SeqAttr seqAttr = seqAttrCaches.get(seqName);
            if (seqAttr == null) {
                seqAttr = new SeqAttr();
                seqAttrCaches.put(seqName, seqAttr);
            }
            seqAttr.incrementBy = this.incrementBy;
            seqAttr.startWith = this.startWith;
            seqAttr.maxValue = this.maxValue;
            int flag = this.cycle;
            seqAttr.cycle = flag & SequenceAttribute.CYCLE;
            seqAttr.cache = flag & SequenceAttribute.CACHE_ENABLED;

        } catch (SQLException e) {
            String errMsg = "Failed to insert sequence '" + seqName + "'.";
            logger.error(errMsg, e);
            throw new SequenceException(e, errMsg);
        } finally {
            SequenceHelper.closeDbResources(null, stmt, null);
        }
    }

    protected String getNextValueSql(String seqName, String tableName, int size) {
        StringBuilder sb = new StringBuilder();
        String incrementValue = incrementByColumn + " * (" + size + " - 1)";
        sb.append("UPDATE ").append(tableName);
        // Check if next value will exceed maximum value allowed.
        sb.append(" SET ").append(valueColumn).append(" = IF(");
        sb.append(valueColumn).append(" + ").append(incrementValue).append(" > ").append(maxValueColumn).append(",");
        // If it exceeds maximum value, then check if cycle is enabled.
        sb.append(" IF(").append(cycleColumn).append(" & ").append(SequenceAttribute.CYCLE);
        sb.append(" = ").append(SequenceAttribute.NOCYCLE).append(",");
        // If no cycle, then no generated key and keep database value as is.
        sb.append(" LAST_INSERT_ID(0) + ").append(valueColumn).append(",");
        // If cycle, then check if we have enough values.
        sb.append(" IF(").append(startWithColumn).append(" + ").append(incrementValue);
        sb.append(" > ").append(maxValueColumn).append(",");
        sb.append(" LAST_INSERT_ID(0) + ").append(valueColumn).append(",");
        // If enough, then return next value after recycled.
        sb.append(" LAST_INSERT_ID(").append(startWithColumn).append(" + ").append(incrementValue).append(")");
        sb.append(" + ").append(incrementByColumn).append(")),");
        // If next value won't exceed maxinum value, then return normal value.
        sb.append(" LAST_INSERT_ID(").append(valueColumn).append(" + ").append(incrementValue).append(")");
        sb.append(" + ").append(incrementByColumn).append("), ");
        sb.append(gmtModifiedColumn).append(" = NOW()");
        sb.append(" WHERE ").append(nameColumn).append(" = ?");
        sb.append(" AND schema_name = ?");
        return sb.toString();
    }

    protected String getUpdateValueSql(String seqName) {
        return getUpdateValueBaseSql(tableName);
    }

    protected String getUpdateValueBaseSql(String tableName) {
        StringBuilder sb = new StringBuilder();
        sb.append("UPDATE ").append(tableName);
        sb.append(" SET ").append(valueColumn).append(" = ? + ").append(incrementByColumn).append(", ");
        sb.append(gmtModifiedColumn).append(" = NOW()");
        sb.append(" WHERE ").append(nameColumn).append(" = ? AND ");
        sb.append(valueColumn).append(" <= ?");
        sb.append(" AND schema_name = ?");
        return sb.toString();
    }

    protected String getTableCheckSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT COUNT(*) ");
        sb.append("FROM INFORMATION_SCHEMA.TABLES ");
        sb.append("WHERE TABLE_SCHEMA = (SELECT DATABASE()) AND ");
        sb.append("TABLE_NAME = ?");
        return sb.toString();
    }

    private String getTableCreateSql() {
        return getTableCreateSqlBase(tableName) + " ENGINE=InnoDB";
    }

    protected String getTableCreateSqlBase(String tableName) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE ").append(tableName).append(" ( ");
        sb.append("id BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT, ");
        sb.append(nameColumn).append(" VARCHAR(128) NOT NULL, ");
        sb.append(valueColumn).append(" BIGINT(20) UNSIGNED NOT NULL, ");
        sb.append(incrementByColumn).append(" INT(10) UNSIGNED NOT NULL DEFAULT 1, ");
        sb.append(startWithColumn).append(" BIGINT(20) UNSIGNED NOT NULL DEFAULT 1, ");
        sb.append(maxValueColumn).append(" BIGINT(20) UNSIGNED NOT NULL DEFAULT 18446744073709551615, ");
        sb.append(cycleColumn).append(" TINYINT(5) UNSIGNED NOT NULL DEFAULT 0, ");
        sb.append(gmtCreatedColumn).append(" TIMESTAMP DEFAULT CURRENT_TIMESTAMP, ");
        sb.append(gmtModifiedColumn).append(" TIMESTAMP NOT NULL DEFAULT '1970-02-01 00:00:00', ");
        sb.append("PRIMARY KEY (id), ");
        sb.append("UNIQUE KEY unique_name (name) ");
        sb.append(") DEFAULT CHARSET=UTF8 ");
        return sb.toString();
    }

    private String getEntryCheckSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ");
        sb.append(incrementByColumn).append(", ");
        sb.append(startWithColumn).append(", ");
        sb.append(maxValueColumn).append(", ");
        sb.append(cycleColumn);
        sb.append(" FROM ").append(tableName);
        sb.append(" WHERE ").append(nameColumn).append(" = ?");
        sb.append(" AND schema_name = ?");
        return sb.toString();
    }

    private String getEntryInsertSql(String tableName) {
        return getEntryInsertSqlBase(tableName) + " VALUES(?, ?, ?, ?, ?, ?, ?, NOW())";
    }

    protected String getEntryInsertSqlBase(String tableName) {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ").append(tableName).append("(");
        sb.append(getColumnListSql()).append(") ");
        return sb.toString();
    }

    protected String getColumnListSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("schema_name, ");
        sb.append(nameColumn).append(", ");
        sb.append(valueColumn).append(", ");
        sb.append(incrementByColumn).append(", ");
        sb.append(startWithColumn).append(", ");
        sb.append(maxValueColumn).append(", ");
        sb.append(cycleColumn).append(", ");
        sb.append(gmtModifiedColumn);
        return sb.toString();
    }

    protected String getEntryCurrentValueSql(String tableName) {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ").append(valueColumn).append(", ").append(maxValueColumn);
        sb.append(" FROM ").append(tableName);
        sb.append(" WHERE ").append(nameColumn).append(" = ?");
        sb.append(" AND schema_name = ?");
        return sb.toString();
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public String getDbGroupKey() {
        return dbGroupKey;
    }

    public void setDbGroupKey(String dbGroupKey) {
        this.dbGroupKey = dbGroupKey;
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public String getUnitName() {
        return unitName;
    }

    public void setUnitName(String unitName) {
        this.unitName = unitName;
    }

    @Override
    public int getRetryTimes() {
        return retryTimes;
    }

    public void setRetryTimes(int retryTimes) {
        this.retryTimes = retryTimes;
    }

    public boolean isCheck() {
        return check;
    }

    public void setCheck(boolean check) {
        this.check = check;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getNameColumnName() {
        return nameColumn;
    }

    public void setNameColumnName(String nameColumnName) {
        this.nameColumn = nameColumnName;
    }

    public String getValueColumnName() {
        return valueColumn;
    }

    public void setValueColumnName(String valueColumnName) {
        this.valueColumn = valueColumnName;
    }

    public String getIncrementByColumnName() {
        return incrementByColumn;
    }

    public void setIncrementByColumnName(String incrementByColumnName) {
        this.incrementByColumn = incrementByColumnName;
    }

    public String getStartWithColumnName() {
        return startWithColumn;
    }

    public void setStartWithColumnName(String startWithColumnName) {
        this.startWithColumn = startWithColumnName;
    }

    public String getMaxValueColumnName() {
        return maxValueColumn;
    }

    public void setMaxValueColumnName(String maxValueColumnName) {
        this.maxValueColumn = maxValueColumnName;
    }

    public String getCycleColumnName() {
        return cycleColumn;
    }

    public void setCycleColumnName(String cycleColumnName) {
        this.cycleColumn = cycleColumnName;
    }

    public String getGmtCreatedColumnName() {
        return gmtCreatedColumn;
    }

    public void setGmtCreatedColumnName(String gmtCreatedColumnName) {
        this.gmtCreatedColumn = gmtCreatedColumnName;
    }

    public String getGmtModifiedColumnName() {
        return gmtModifiedColumn;
    }

    public void setGmtModifiedColumnName(String gmtModifiedColumnName) {
        this.gmtModifiedColumn = gmtModifiedColumnName;
    }

    public int getIncrementBy() {
        return incrementBy;
    }

    public int getIncrementBy(String seqName) {
        SeqAttr seqAttr = seqAttrCaches.get(seqName);
        return seqAttr != null ? seqAttr.incrementBy : incrementBy;
    }

    public void setIncrementBy(int incrementBy) {
        if (incrementBy <= 0 || incrementBy > Integer.MAX_VALUE) {
            incrementBy = 1;
        }
        this.incrementBy = incrementBy;
    }

    public long getStartWith() {
        return startWith;
    }

    public void setStartWith(long startWith) {
        if (startWith <= 0 || startWith > Long.MAX_VALUE) {
            startWith = 1;
        }
        this.startWith = startWith;
    }

    public long getMaxValue() {
        return maxValue;
    }

    public void setMaxValue(long maxValue) {
        if (maxValue <= 0 || maxValue > Long.MAX_VALUE) {
            maxValue = Long.MAX_VALUE;
        }
        this.maxValue = maxValue;
    }

    public boolean isCycle() {
        return cycle == SequenceAttribute.CYCLE;
    }

    public void setCycle(boolean cycle) {
        this.cycle = cycle ? SequenceAttribute.CYCLE : SequenceAttribute.NOCYCLE;
    }

    public boolean isCache() {
        return cache == SequenceAttribute.CACHE_ENABLED;
    }

    public void setCache(boolean cache) {
        this.cache = cache ? SequenceAttribute.CACHE_ENABLED : SequenceAttribute.CACHE_DISABLED;
    }

    public String getConfigStr() {
        if (StringUtils.isEmpty(configStr)) {
            String format =
                "[type:simple] [appName:{0}] [dbGroupKey:{1}] [retryTimes:{2}] [check:{3}] [tableInfo:{4}({5},{6},{7},{8},{9},{10},{11},{12})]";
            configStr = MessageFormat.format(format,
                appName,
                dbGroupKey,
                String.valueOf(retryTimes),
                String.valueOf(check),
                tableName,
                nameColumn,
                valueColumn,
                incrementByColumn,
                startWithColumn,
                maxValueColumn,
                cycleColumn,
                gmtCreatedColumn,
                gmtModifiedColumn);
        }
        return configStr;
    }

    @Override
    public int getStep() {
        return getIncrementBy();
    }

    @Override
    public SequenceRange nextRange(String name) {
        throw new NotSupportException();
    }

    protected SeqAttr getCachedSeqAttr(String seqName) {
        SeqAttr seqAttr = seqAttrCaches.get(seqName);
        if (seqAttr == null) {
            String errMsg = "The cache of sequence attributes is missing.";
            logger.error(errMsg);
            throw new SequenceException(errMsg);
        }
        return seqAttr;
    }

    protected static class SeqAttr {

        public int incrementBy = SequenceAttribute.DEFAULT_INCREMENT_BY;
        public long startWith = SequenceAttribute.DEFAULT_START_WITH;
        public long maxValue = SequenceAttribute.DEFAULT_MAX_VALUE;
        public int cycle = SequenceAttribute.NOCYCLE;
        public int cache = SequenceAttribute.CACHE_DISABLED;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

}

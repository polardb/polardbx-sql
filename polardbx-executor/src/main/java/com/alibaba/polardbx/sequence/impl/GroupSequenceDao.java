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
import com.alibaba.polardbx.common.logger.LoggerInit;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.sequence.SequenceDao;
import com.alibaba.polardbx.sequence.SequenceRange;
import com.alibaba.polardbx.sequence.exception.SequenceException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.alibaba.polardbx.gms.metadb.GmsSystemTables.SEQUENCE;

public class GroupSequenceDao extends AbstractLifecycle implements SequenceDao {

    protected static final Logger logger = LoggerFactory.getLogger(GroupSequenceDao.class);

    protected static final String INSERT_GROUP_SEQ =
        "insert into " + SEQUENCE + "(schema_name, name, value) values(?,?,?)";

    protected static final String SELECT_GROUP_SEQ_VALUE =
        "select value from " + SEQUENCE + " where name = ? and schema_name = ?";

    protected static final String UPDATE_GROUP_SEQ_VALUE =
        "update " + SEQUENCE + " set value = ? where name = ? and value = ? and schema_name = ?";

    protected static final String DIRECT_GROUP_SEQ_UPDATE =
        "UPDATE " + SEQUENCE + " SET value = ? WHERE name = ? AND value < ? AND schema_name = ?";

    protected static final long DELTA = 100000000L;

    protected String schemaName;

    protected int retryTimes = SequenceAttribute.DEFAULT_RETRY_TIMES;
    protected int step = SequenceAttribute.DEFAULT_INNER_STEP;

    protected Lock configLock = new ReentrantLock();

    public GroupSequenceDao() {
    }

    @Override
    public void doInit() {
        outputInitResult();
    }

    public void adjust(String name) throws SequenceException, SQLException {
        String errorMessage = "Failed to check/adjust value for %s. Caused by: %s";

        try (Connection metaDbConn = MetaDbUtil.getConnection();
            PreparedStatement ps = metaDbConn.prepareStatement(SELECT_GROUP_SEQ_VALUE)) {

            ps.setString(1, name);
            ps.setString(2, schemaName);

            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    long val = rs.getLong(1);
                    if (!check(val)) {
                        adjustUpdate(val, name);
                    }
                } else {
                    adjustInsert(name);
                }
            }
        } catch (SQLException e) {
            String errMsg = String.format(errorMessage, name, e.getMessage());
            logger.error(errMsg, e);
            throw new SequenceException(e, errMsg);
        }
    }

    private boolean check(long value) {
        return (value % step) == 0;
    }

    private void adjustUpdate(long value, String name) throws SequenceException {
        long newValue = (value - value % step) + step;
        adjustUpdate(value, newValue, name);
    }

    protected void adjustUpdate(long value, long newValue, String name) throws SequenceException {
        String errorMessage = "Failed to adjust value for %s from %s to %s. Caused by: %s";

        try (Connection metaDbConn = MetaDbUtil.getConnection();
            PreparedStatement ps = metaDbConn.prepareStatement(UPDATE_GROUP_SEQ_VALUE)) {

            ps.setLong(1, newValue);
            ps.setString(2, name);
            ps.setLong(3, value);
            ps.setString(4, schemaName);

            int affectedRows = ps.executeUpdate();

            if (affectedRows == 0) {
                throw new SequenceException(String.format(errorMessage, name, value, newValue, "0 rows affected"));
            }

            logger.info(String.format("Adjusted value for %s from %s to %s", name, value, newValue));
        } catch (SQLException e) {
            String errMsg = String.format(errorMessage, name, value, newValue, e.getMessage());
            logger.error(errMsg, e);
            throw new SequenceException(e, errMsg);
        }
    }

    protected void adjustInsert(String name) throws SequenceException, SQLException {
        String errorMessage = "Failed to insert initial value from adjust for %s. Caused by: %s";

        try (Connection metaDbConn = MetaDbUtil.getConnection();
            PreparedStatement ps = metaDbConn.prepareStatement(INSERT_GROUP_SEQ)) {

            ps.setString(1, schemaName);
            ps.setString(2, name);
            ps.setLong(3, 0L);

            int affectedRows = ps.executeUpdate();

            if (affectedRows == 0) {
                throw new SequenceException(String.format(errorMessage, name, "0 rows affected"));
            }

            logger.info("Inserted initial value from adjust for " + name);
        } catch (SQLException e) {
            String errMsg = String.format(errorMessage, name, e.getMessage());
            logger.error(errMsg, e);
            throw new SequenceException(e, errMsg);
        }
    }

    @Override
    public SequenceRange nextRange(String name) throws SequenceException {
        String errorMessage = "Failed to get next range for %s. Caused by: %s";

        Throwable ex = null;
        configLock.lock();
        try {
            for (int i = 0; i < retryTimes; i++) {
                long newValue;
                try {
                    newValue = fetchNextValue(name);
                } catch (SQLException e) {
                    ex = e;
                    logger.warn(String.format(errorMessage, name, e.getMessage()), e);
                    continue;
                }

                SequenceRange range = new SequenceRange(newValue + 1, newValue + step);

                String infoMsg = String.format("Got a new range for Group Sequence %s. Range Info: %s", name, range);
                LoggerInit.TDDL_SEQUENCE_LOG.info(infoMsg);

                return range;
            }

            errorMessage = "Failed to get next range for %s after all %s retries";
            if (ex == null) {
                String errMsg = String.format(errorMessage, name, retryTimes);
                logger.error(errMsg);
                throw new SequenceException(errMsg);
            } else {
                errorMessage += ". Caused by: %s";
                String errMsg = String.format(errorMessage, name, retryTimes, ex.getMessage());
                logger.error(errMsg, ex);
                throw new SequenceException(ex, errMsg);
            }
        } finally {
            configLock.unlock();
        }
    }

    protected long fetchNextValue(String name) throws SQLException {
        long newValue;
        try (Connection metaDbConn = MetaDbUtil.getConnection();
            PreparedStatement ps = metaDbConn.prepareStatement(getFetchSql(), Statement.RETURN_GENERATED_KEYS)) {

            ps.setString(1, name);
            ps.setString(2, schemaName);

            int updateCount = ps.executeUpdate();

            if (updateCount == 1) {
                try (ResultSet rs = ps.getGeneratedKeys()) {
                    if (rs.next()) {
                        newValue = rs.getLong(1);
                    } else {
                        String errMsg = String.format("Failed to fetch generated new value for %s", name);
                        logger.error(errMsg);
                        throw new SequenceException(errMsg);
                    }
                }
            } else if (updateCount > 1) {
                String errMsg = String.format("Unexpected: found multiple sequences with the same name %", name);
                logger.warn(errMsg);
                throw new SequenceException(errMsg);
            } else {
                String errMsg = String.format("Sequence %s doesn't exist", name);
                logger.warn(errMsg);
                throw new SequenceException(errMsg);
            }

            return newValue;
        }
    }

    public boolean updateValue(final String name, final long value) throws SequenceException {
        if (value <= 0L) {
            return false;
        }
        try {
            long newValue = value - value % step;
            try (Connection metaDbConn = MetaDbUtil.getConnection();
                PreparedStatement ps = metaDbConn.prepareStatement(DIRECT_GROUP_SEQ_UPDATE)) {

                ps.setLong(1, newValue);
                ps.setString(2, name);
                ps.setLong(3, newValue);
                ps.setString(4, schemaName);

                int updateCount = ps.executeUpdate();

                return updateCount >= 1;
            }
        } catch (SQLException e) {
            String errMsg = String.format("Failed to update value %s directly for %s. Caused by: %s",
                name, value, e.getMessage());
            logger.error(errMsg, e);
            throw new SequenceException(e, errMsg);
        }
    }

    protected String getFetchSql() {
        StringBuilder sql = new StringBuilder();
        sql.append("UPDATE ").append(SEQUENCE);
        sql.append(" SET value = IF((value + ").append(step).append(") % ").append(step);
        sql.append(" != 0, LAST_INSERT_ID(value + ").append(step).append(" - (");
        sql.append("value + ").append(step).append(") % ").append(step).append(" + ").append(step);
        sql.append("), LAST_INSERT_ID(value + ").append(step).append("))");
        sql.append(" WHERE name = ? AND schema_name = ?");
        return sql.toString();
    }

    protected void outputInitResult() {
        String message =
            String.format("GroupSequenceDao has been initialized: step - %s, retryTimes - %s", step, retryTimes);
        logger.info(message);
        LoggerInit.TDDL_SEQUENCE_LOG.info(message);
    }

    @Override
    public int getStep() {
        return step;
    }

    public void setStep(int step) {
        this.step = step;
    }

    @Override
    public int getRetryTimes() {
        return retryTimes;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    @Override
    protected void doDestroy() {
        super.doDestroy();
    }

}

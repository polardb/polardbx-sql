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

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.sequence.exception.SequenceException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_INNER_STEP;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_UNIT_COUNT;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_UNIT_INDEX;
import static com.alibaba.polardbx.gms.metadb.GmsSystemTables.SEQUENCE;

public class CustomUnitGroupSequenceDao extends GroupSequenceDao {

    private static final Logger logger = LoggerFactory.getLogger(CustomUnitGroupSequenceDao.class);

    protected static final String SELECT_CUSTOM_UNIT_GROUP_SEQ =
        "SELECT value, unit_count, unit_index, inner_step FROM " + SEQUENCE
            + " WHERE name = ? and schema_name = ?";

    public CustomUnitGroupSequenceDao() {
    }

    @Override
    public void doInit() {
        super.doInit();
    }

    public int[] adjustPlus(String name) throws SequenceException, SQLException {
        try (Connection metaDbConn = MetaDbUtil.getConnection();
            PreparedStatement ps = metaDbConn.prepareStatement(SELECT_CUSTOM_UNIT_GROUP_SEQ)) {
            ps.setString(1, name);
            ps.setString(2, schemaName);
            try (ResultSet rs = ps.executeQuery()) {
                int unitCount, unitIndex, innerStep;
                if (rs.next()) {
                    long value = rs.getLong(1);
                    unitCount = rs.getInt(2);
                    unitIndex = rs.getInt(3);
                    innerStep = rs.getInt(4);

                    if (!check(unitCount, unitIndex, innerStep, value)) {
                        this.adjustUpdate(unitCount, unitIndex, innerStep, value, name);
                    }
                } else {
                    // Insert initial one since the sequence doesn't exist.
                    this.adjustInsert(name);

                    unitCount = DEFAULT_UNIT_COUNT;
                    unitIndex = DEFAULT_UNIT_INDEX;
                    innerStep = DEFAULT_INNER_STEP;
                }
                return new int[] {unitCount, unitIndex, innerStep};
            }
        } catch (SQLException e) {
            logger.error("Failed to check and adjust initial value for sequence '" + name + "'.", e);
            throw e;
        }
    }

    protected boolean check(int unitCount, int unitIndex, int innerStep, long value) {
        return value % (innerStep * unitCount) == unitIndex * innerStep;
    }

    protected void adjustUpdate(int unitCount, int unitIndex, int innerStep, long value, String name)
        throws SequenceException, SQLException {
        long localOutStep = innerStep * unitCount;
        long newValue = (value - value % localOutStep) + localOutStep + unitIndex * innerStep;
        adjustUpdate(value, newValue, name);
    }

    public long nextRangeStart(String name) throws SequenceException {
        String errorMessage = "Failed to get next range start for %s. Caused by: %s";

        Throwable ex = null;
        configLock.lock();
        try {
            for (int i = 0; i < retryTimes; i++) {
                try {
                    return fetchNextValue(name);
                } catch (SQLException e) {
                    ex = e;
                    logger.warn(String.format(errorMessage, name, e.getMessage()), e);
                    continue;
                }
            }

            errorMessage = "Failed to get next range start for %s after all %s retries";
            if (ex != null) {
                errorMessage += ". Caused by: %s";
                String errMsg = String.format(errorMessage, name, retryTimes, ex.getMessage());
                logger.error(errMsg, ex);
                throw new SequenceException(ex, errMsg);
            } else {
                String errMsg = String.format(errorMessage, name, retryTimes);
                logger.error(errMsg);
                throw new SequenceException(errMsg);
            }
        } finally {
            configLock.unlock();
        }
    }

    @Override
    protected String getFetchSql() {
        final String outStep = "(inner_step * unit_count)";
        final String offset = "(unit_index * inner_step)";
        final String newValue = "(value + " + outStep + ")";

        StringBuilder sb = new StringBuilder();
        sb.append("UPDATE ").append(SEQUENCE);
        sb.append(" SET value = IF(").append(newValue).append(" % ").append(outStep).append(" != ");
        sb.append(offset).append(", ").append("LAST_INSERT_ID(");
        sb.append(newValue).append(" - ").append(newValue).append(" % ");
        sb.append(outStep).append(" + ").append(outStep).append(" + ").append(offset);
        sb.append("), LAST_INSERT_ID(").append(newValue).append("))");
        sb.append(" WHERE name = ? AND schema_name = ?");

        return sb.toString();
    }

}

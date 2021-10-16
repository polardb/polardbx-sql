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
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.sequence.exception.SequenceException;
import com.alibaba.polardbx.sequence.util.SequenceHelper;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;

import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_UNIT_COUNT;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_UNIT_INDEX;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.EXT_UNIT_INDEX_COLUMN;

public class CustomUnitGroupSequenceDao extends GroupSequenceDao {

    private static final Logger logger = LoggerFactory.getLogger(CustomUnitGroupSequenceDao.class);

    private String unitCountColumn = SequenceAttribute.EXT_UNIT_COUNT_COLUMN;
    private String unitIndexColumn = EXT_UNIT_INDEX_COLUMN;
    private String innerStepColumn = SequenceAttribute.EXT_INNER_STEP_COLUMN;

    public CustomUnitGroupSequenceDao() {
    }

    @Override
    public void doInit() {
        super.doInit();
        // Validate key arguments
        if (dscount != 1 || innerStep != outStep) {
            String errMsg = "Unexpected dsCount, innerStep or outStep: " + dscount + ", " + innerStep + ", " + outStep;
            logger.error(errMsg);
            throw new SequenceException(errMsg);
        }
    }

    public int[] adjustPlus(String name) throws SequenceException, SQLException {
        String dbGroupKey = checkAndGetDbGroupKey(name);

        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            DataSource dataSource = dataSourceMap.get(dbGroupKey);
            conn = SequenceHelper.getConnection(dataSource);
            stmt = conn.prepareStatement(getSelectSql());

            stmt.setString(1, name);
            stmt.setString(2, schemaName);

            rs = stmt.executeQuery();

            int index = 0;
            int unitCount, unitIndex, innerStep;

            if (rs.next()) {
                unitCount = rs.getInt(getUnitCountColumn());
                unitIndex = rs.getInt(getUnitIndexColumn());
                innerStep = rs.getInt(getInnerStepColumn());
                long value = rs.getLong(getValueColumnName());

                if (!check(unitCount, unitIndex, innerStep, value)) {
                    if (this.isAdjust()) {
                        this.adjustUpdate(unitCount, unitIndex, innerStep, value, name);
                    } else {
                        String errMsg = "Found wrong initial value for sequence '" + name
                            + "'. Please change it manually or enable 'adjust' for automatic correction.";
                        logger.error(errMsg);
                        throw new SequenceException(errMsg);
                    }
                }
            } else {
                if (this.isAdjust()) {
                    // Insert initial one since the sequence doesn't exist.
                    this.adjustInsert(index, name);

                    unitCount = DEFAULT_UNIT_COUNT;
                    unitIndex = DEFAULT_UNIT_INDEX;
                    innerStep = SequenceAttribute.DEFAULT_INNER_STEP;
                } else {
                    String errMsg = "Sequence '" + name
                        + "' doesn't exist. Please insert such a record manually or enable 'adjust' for automatic handling.";
                    logger.error(errMsg);
                    throw new SequenceException(errMsg);
                }
            }
            return new int[] {unitCount, unitIndex, innerStep};
        } catch (SQLException e) {
            logger.error("Failed to check and adjust initial value for sequence '" + name + "'.", e);
            throw e;
        } finally {
            SequenceHelper.closeDbResources(rs, stmt, conn);
        }
    }

    protected boolean check(int unitCount, int unitIndex, int innerStep, long value) {
        return value % (innerStep * unitCount) == unitIndex * innerStep;
    }

    protected void adjustUpdate(int unitCount, int unitIndex, int innerStep, long value,
                                String name) throws SequenceException, SQLException {
        long localOutStep = innerStep * unitCount;
        long newValue = (value - value % localOutStep) + localOutStep + unitIndex * innerStep;

        String dbGroupKey = checkAndGetDbGroupKey(name);
        DataSource dataSource = dataSourceMap.get(dbGroupKey);

        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = SequenceHelper.getConnection(dataSource);
            stmt = conn.prepareStatement(getUpdateSql());

            stmt.setLong(1, newValue);
            stmt.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
            stmt.setString(3, name);
            stmt.setLong(4, value);
            stmt.setString(5, schemaName);

            int updateCount = stmt.executeUpdate();

            if (updateCount == 0) {
                throw new SequenceException(
                    "Failed to adjust initial value for sequence '" + name + "': no row updated");
            }
            logger.info("Adjusted initial value successfully for sequence '" + name + "' on group '" + dbGroupKey
                + "': " + value + " -> " + newValue);
        } catch (SQLException e) {
            String errMsg = "Failed to adjust initial value for sequence '" + name + "' on group '" + dbGroupKey + "': "
                + value + " -> " + newValue;
            logger.error(errMsg, e);
            throw new SequenceException(e, errMsg);
        } finally {
            SequenceHelper.closeDbResources(null, stmt, conn);
        }
    }

    public long nextRangeStart(final String name) throws SequenceException {
        String dbGroupKey = checkAndGetDbGroupKey(name);

        configLock.lock();
        try {
            DataSource dataSource = dataSourceMap.get(dbGroupKey);
            return fetchNewValue(dataSource, name);
        } catch (SQLException e) {
            String errMsg = "Failed to get new value for group sequence '" + name + "' on group '" + dbGroupKey + "'.";
            logger.error(errMsg, e);
            throw new SequenceException(e, e.getMessage());
        } finally {
            configLock.unlock();
        }
    }

    protected long fetchNewValue(final DataSource dataSource, final String keyName) throws SQLException {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            long newValue = 0L;
            conn = SequenceHelper.getConnection(dataSource);
            stmt = conn.prepareStatement(getFetchSql(), Statement.RETURN_GENERATED_KEYS);

            stmt.setString(1, keyName);
            stmt.setString(2, schemaName);

            int updateCount = stmt.executeUpdate();

            if (updateCount == 1) {
                rs = stmt.getGeneratedKeys();
                if (rs.next()) {
                    newValue = rs.getLong(1);
                } else {
                    throwErrorRangeException(0, keyName);
                }
            } else if (updateCount > 1) {
                String errMsg = "Found multiple sequences with the same name '" + keyName + "' on group '"
                    + dbGroupKeys.get(0)
                    + "'. Please double check them and delete the extra records to keep only one. "
                    + "Also add a unique index for the name column to avoid potential issues.";
                logger.warn(errMsg);
                throw new SequenceException(errMsg);
            } else {
                String errMsg = "Sequence '" + keyName + "' doesn't exist on group '" + dbGroupKeys.get(0) + "'.";
                logger.warn(errMsg);
                throw new SequenceException(errMsg);
            }

            return newValue;
        } finally {
            SequenceHelper.closeDbResources(rs, stmt, conn);
        }
    }

    @Override
    protected String getSelectSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ").append(getValueColumnName()).append(", ").append(getUnitCountColumn()).append(", ");
        sb.append(getUnitIndexColumn()).append(", ").append(getInnerStepColumn());
        sb.append(" FROM ").append(getTableName());
        sb.append(" WHERE ").append(getNameColumnName()).append(" = ?");
        sb.append(" and schema_name = ?");
        return sb.toString();
    }

    protected String getFetchSql() {
        // outStep = inner_step * unit_count
        StringBuilder outStep = new StringBuilder();
        outStep.append("(").append(getInnerStepColumn()).append(" * ").append(getUnitCountColumn()).append(")");

        // newValue = value + outStep
        StringBuilder newValue = new StringBuilder();
        newValue.append("(").append(getValueColumnName()).append(" + ").append(outStep.toString()).append(")");

        // offset = unit_index * inner_step
        StringBuilder offset = new StringBuilder();
        offset.append("(").append(getUnitIndexColumn()).append(" * ").append(getInnerStepColumn()).append(")");

        StringBuilder sb = new StringBuilder();
        sb.append("UPDATE ").append(getTableName());
        sb.append(" SET ").append(getValueColumnName()).append(" = IF(");
        sb.append(newValue.toString()).append(" % ").append(outStep.toString()).append(" != ");
        sb.append(offset.toString()).append(", ").append("LAST_INSERT_ID(");
        if (isAdjust()) {
            sb.append(newValue.toString()).append(" - ").append(newValue.toString()).append(" % ");
            sb.append(outStep.toString()).append(" + ").append(outStep.toString()).append(" + ");
            sb.append(offset.toString());
        } else {
            sb.append(0);
        }
        sb.append("), LAST_INSERT_ID(").append(newValue.toString()).append(")), ");
        sb.append(getGmtModifiedColumnName()).append(" = NOW()");
        sb.append(" WHERE ").append(getNameColumnName()).append(" = ?");

        sb.append(" AND schema_name = ?");

        return sb.toString();
    }

    private String checkAndGetDbGroupKey(String name) {
        if (name == null) {
            logger.error("序列名为空！");
            throw new IllegalArgumentException("序列名称不能为空");
        }

        // Double check if there is only one group.
        if (dscount != 1) {
            logger.error("Unexpected dsCount: " + dscount);
            throw new SequenceException("dsCount must be 1 for Custom Unitized Group Sequence.");
        }

        // There is only 1 group used to generate values for group sequence.
        String dbGroupKey = dbGroupKeys.get(0);

        if (TStringUtil.isEmpty(dbGroupKey) || isOffState(dbGroupKey)) {
            logger.error("Unexpected dbGroupKey: " + dbGroupKey);
            throw new SequenceException("There is no available dbGroupKey.");
        }

        return dbGroupKey;
    }

    public void validateTable() {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        for (int i = 0; i < dbGroupKeys.size(); i++) {
            if (isOffState(dbGroupKeys.get(i))) {
                continue;
            }
            DataSource dataSource = getGroupDsByIndex(i);
            try {
                conn = SequenceHelper.getConnection(dataSource);
                stmt = conn.prepareStatement(getTableCheckSql());
                stmt.setString(1, tableName);
                stmt.setString(2, unitCountColumn);

                rs = stmt.executeQuery();

                if (rs.next()) {
                    if (rs.getInt(1) == 0) {
                        // The table doesn't contain new column, so alter it.
                        alterOldTable(conn);
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
    }

    private void alterOldTable(Connection conn) {
        PreparedStatement stmt = null;
        try {
            stmt = conn.prepareStatement(getTableAlterSql());
            stmt.executeUpdate();
        } catch (SQLException e) {
            String errMsg = "Failed to alter sequence table '" + tableName + "'.";
            logger.error(errMsg, e);
            throw new SequenceException(e, errMsg);
        } finally {
            SequenceHelper.closeDbResources(null, stmt, null);
        }
    }

    protected String getTableCheckSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT COUNT(*) ");
        sb.append("FROM INFORMATION_SCHEMA.COLUMNS ");
        sb.append("WHERE TABLE_SCHEMA = (SELECT DATABASE()) AND ");
        sb.append("TABLE_NAME = ? AND COLUMN_NAME = ?");
        return sb.toString();
    }

    protected String getTableAlterSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER TABLE ").append(tableName);
        sb.append(" ADD COLUMN ").append(unitCountColumn);
        sb.append(" INT NOT NULL DEFAULT ").append(DEFAULT_UNIT_COUNT);
        sb.append(" AFTER ").append(valueColumnName).append(",");
        sb.append(" ADD COLUMN ").append(unitIndexColumn);
        sb.append(" INT NOT NULL DEFAULT ").append(DEFAULT_UNIT_INDEX);
        sb.append(" AFTER ").append(unitCountColumn).append(",");
        sb.append(" ADD COLUMN ").append(innerStepColumn);
        sb.append(" INT NOT NULL DEFAULT ").append(SequenceAttribute.DEFAULT_INNER_STEP);
        sb.append(" AFTER ").append(unitIndexColumn).append(";");
        return sb.toString();
    }

    public String getUnitCountColumn() {
        return unitCountColumn;
    }

    public void setUnitCountColumn(String unitCountColumn) {
        this.unitCountColumn = unitCountColumn;
    }

    public String getUnitIndexColumn() {
        return unitIndexColumn;
    }

    public void setUnitIndexColumn(String unitIndexColumn) {
        this.unitIndexColumn = unitIndexColumn;
    }

    public String getInnerStepColumn() {
        return innerStepColumn;
    }

    public void setInnerStepColumn(String innerStepColumn) {
        this.innerStepColumn = innerStepColumn;
    }

}

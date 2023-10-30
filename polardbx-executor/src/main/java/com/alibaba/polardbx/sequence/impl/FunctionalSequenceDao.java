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

import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
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

import static com.alibaba.polardbx.common.constants.SequenceAttribute.CYCLE;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_INCREMENT_BY;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_RETRY_TIMES;
import static com.alibaba.polardbx.gms.metadb.GmsSystemTables.SEQUENCE_OPT;

public abstract class FunctionalSequenceDao extends AbstractLifecycle implements SequenceDao {

    private static final Logger logger = LoggerFactory.getLogger(FunctionalSequenceDao.class);

    protected static final String VALIDATE_FUNCTIONAL_SEQ =
        "SELECT increment_by, start_with, max_value, cycle FROM " + SEQUENCE_OPT
            + " WHERE name = ? AND schema_name = ?";

    protected int retryTimes = DEFAULT_RETRY_TIMES;

    protected String schemaName;

    @Override
    public void doInit() {
        outputInitResult();
    }

    public void validate(FunctionalSequence sequence) {
        final String seqName = sequence.getName();
        try (Connection metaDbConn = MetaDbUtil.getConnection();
            PreparedStatement stmt = metaDbConn.prepareStatement(VALIDATE_FUNCTIONAL_SEQ)) {

            stmt.setString(1, seqName);
            stmt.setString(2, schemaName);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    sequence.setIncrementBy(rs.getInt(1));
                    sequence.setStartWith(rs.getLong(2));
                    sequence.setMaxValue(rs.getLong(3));
                    sequence.setCycle(rs.getInt(4) & CYCLE);
                } else {
                    throw new SequenceException(
                        "Sequence '" + seqName + "' doesn't exist. Please double check and try again.");
                }
            }
        } catch (SQLException e) {
            logger.error("Failed to validate sequence '" + seqName + "'.", e);
            throw new TddlRuntimeException(ErrorCode.ERR_OTHER_WHEN_BUILD_SEQUENCE, e, e.getMessage());
        }
    }

    @Override
    public int getRetryTimes() {
        return retryTimes;
    }

    public void setRetryTimes(int retryTimes) {
        this.retryTimes = retryTimes;
    }

    @Override
    public int getStep() {
        return DEFAULT_INCREMENT_BY;
    }

    @Override
    public SequenceRange nextRange(String name) {
        throw new NotSupportException();
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    protected void outputInitResult() {
        String message =
            String.format("%s has been initialized: retryTimes - %s", this.getClass().getSimpleName(), retryTimes);
        logger.info(message);
        LoggerInit.TDDL_SEQUENCE_LOG.info(message);
    }

}

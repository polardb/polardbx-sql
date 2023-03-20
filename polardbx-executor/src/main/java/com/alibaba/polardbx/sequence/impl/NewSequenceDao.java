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
import com.alibaba.polardbx.common.logger.LoggerInit;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.seq.SequenceOptNewAccessor;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.rpc.compatible.XResultSet;
import com.alibaba.polardbx.rpc.pool.XConnection;
import com.alibaba.polardbx.rpc.result.XResult;
import com.alibaba.polardbx.sequence.SequenceDao;
import com.alibaba.polardbx.sequence.SequenceRange;
import com.alibaba.polardbx.sequence.exception.SequenceException;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_INCREMENT_BY;

public class NewSequenceDao extends AbstractLifecycle implements SequenceDao {

    private static final Logger logger = LoggerFactory.getLogger(NewSequenceDao.class);

    protected static final String SELECT_NEXTVAL_BATCH = "select nextval(%s, %s)";

    protected static final String SELECT_NEXTVAL_SKIP = "select nextval_skip(%s, %s)";

    protected String schemaName;

    public NewSequenceDao() {
    }

    @Override
    public void doInit() {
        outputInitResult();
    }

    public long nextValue(String name) {
        return nextValue(name, 1);
    }

    public long nextValue(String name, int batchSize) {
        String errPrefix = "Failed to get next value with batch size " + batchSize + " for sequence " + name + " - ";

        // Do some protection and conversion first.
        batchSize = batchSize <= 0 ? 1 : batchSize;
        String phySeqName = genNameForNewSequence(name);

        try (Connection metaDbConn = MetaDbUtil.getConnection();
            Statement stmt = metaDbConn.createStatement();

            ResultSet rs = stmt.executeQuery(String.format(SELECT_NEXTVAL_BATCH, phySeqName, batchSize))) {

            long minValueInBatch = getMinValueInBatch(rs, errPrefix);

            // PolarDB-X uses the max value in a batch to assign IDs,
            // so we need to return max value here.
            return minValueInBatch + batchSize - 1;
        } catch (SQLException e) {
            String errMsg = errPrefix + e.getMessage();
            logger.error(errMsg, e);
            throw new SequenceException(e, errMsg);
        }
    }

    public MergingResult nextValueMerging(List<Pair<String, Integer>> seqBatchSizes,
                                          Consumer<Pair<String, Long>> consumer) {
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            if (metaDbConn.isWrapperFor(XConnection.class)) {
                // Use stream mode to implement batch processing for XProtocol.
                return nextValueMergingXPROTO(seqBatchSizes, consumer, metaDbConn);
            } else {
                // Use normal way of multiple statements and result sets for JDBC.
                return nextValueMergingJDBC(seqBatchSizes, consumer, metaDbConn);
            }
        } catch (SQLException e) {
            String errMsg = "Failed to fetch next merging values - " + e.getMessage();
            logger.error(errMsg, e);
            MergingResult mergingResult = new MergingResult();
            mergingResult.errMsg = errMsg;
            mergingResult.origEx = e;
            return mergingResult;
        }
    }

    public MergingResult nextValueMergingXPROTO(List<Pair<String, Integer>> seqBatchSizes,
                                                Consumer<Pair<String, Long>> consumer,
                                                Connection metaDbConn) {
        MergingResult mergingResult = new MergingResult();
        String errPrefix = "Failed to fetch next merging values via XProtocol - ";

        List<XResultSet> xResultSets = new ArrayList<>();
        try {
            XConnection xMetaDbConn = (XConnection) metaDbConn;
            xMetaDbConn.setStreamMode(true);

            for (Pair<String, Integer> pair : seqBatchSizes) {
                String phySeqName = genNameForNewSequence(pair.getKey());
                String sql = String.format(SELECT_NEXTVAL_BATCH, phySeqName, pair.getValue());

                XResult result = xMetaDbConn.execQuery(sql);

                xResultSets.add(new XResultSet(result));
            }

            for (int index = 0; index < xResultSets.size(); index++) {
                String seqName = seqBatchSizes.get(index).getKey();

                long minValueInBatch = getMinValueInBatch(xResultSets.get(index), errPrefix);

                consumer.accept(Pair.of(seqName, minValueInBatch));

                mergingResult.failedBeforeAnyResponse = false;
                mergingResult.indexProcessed = index;
            }

            return null;
        } catch (Exception e) {
            String errMsg = errPrefix + e.getMessage();
            logger.error(errMsg, e);
            mergingResult.errMsg = errMsg;
            mergingResult.origEx = e;
            return mergingResult;
        } finally {
            if (GeneralUtil.isNotEmpty(xResultSets)) {
                xResultSets.forEach(xrs -> {
                    try {
                        xrs.close();
                    } catch (Throwable ignored) {
                    }
                });
            }
        }
    }

    public MergingResult nextValueMergingJDBC(List<Pair<String, Integer>> seqBatchSizes,
                                              Consumer<Pair<String, Long>> consumer,
                                              Connection metaDbConn) {
        MergingResult mergingResult = new MergingResult();
        String errPrefix = "Failed to fetch next merging values via JDBC - ";

        // Use normal way of multiple statements and result sets for JDBC.
        StringBuilder sql = new StringBuilder();
        for (Pair<String, Integer> pair : seqBatchSizes) {
            String phySeqName = genNameForNewSequence(pair.getKey());
            sql.append(String.format(SELECT_NEXTVAL_BATCH, phySeqName, pair.getValue())).append(";");
        }

        try (Statement stmt = metaDbConn.createStatement()) {
            boolean hasMoreResults = stmt.execute(sql.toString());

            int index = 0;
            while (hasMoreResults) {
                ResultSet rs = stmt.getResultSet();

                String seqName = seqBatchSizes.get(index).getKey();

                long minValueInBatch = getMinValueInBatch(rs, errPrefix);

                consumer.accept(Pair.of(seqName, minValueInBatch));

                mergingResult.failedBeforeAnyResponse = false;
                mergingResult.indexProcessed = index;

                // Move to next result set
                hasMoreResults = stmt.getMoreResults();
                index++;
            }

            return null;
        } catch (Exception e) {
            String errMsg = errPrefix + e.getMessage();
            logger.error(errMsg, e);
            mergingResult.errMsg = errMsg;
            mergingResult.origEx = e;
            return mergingResult;
        }
    }

    private long getMinValueInBatch(ResultSet rs, String errPrefix) throws SQLException {
        long value;
        if (rs.next()) {
            value = rs.getLong(1);
            // Finish the ResultSet object from Stream Mode in XProtocol.
            rs.next();
            if (value <= 0) {
                String errMsg = errPrefix + "unexpected next value " + value;
                logger.error(errMsg);
                throw new SequenceException(errMsg);
            }
        } else {
            String errMsg = errPrefix + "no next value fetched";
            logger.error(errMsg);
            throw new SequenceException(errMsg);
        }
        // Return min value as base in the batch.
        return value;
    }

    class MergingResult {
        boolean failedBeforeAnyResponse = true;
        int indexProcessed = 0;
        String errMsg = null;
        Exception origEx = null;
    }

    public void updateValue(String name, long maxValue) {
        if (maxValue <= 0L) {
            return;
        }

        String phySeqName = genNameForNewSequence(name);

        try (Connection metaDbConn = MetaDbUtil.getConnection();
            Statement stmt = metaDbConn.createStatement()) {

            // Update the sequence value only when new value > current value.
            // Don't care about return code.
            stmt.execute(String.format(SELECT_NEXTVAL_SKIP, phySeqName, maxValue));
        } catch (SQLException e) {
            String errMsg = "Failed to update sequence value to " + maxValue + ".";
            logger.error(errMsg, e);
            throw new SequenceException(e, errMsg);
        }

    }

    public void updateValueMerging(Map<String, Long> seqMaxValues) {
        try (Connection metaDbConn = MetaDbUtil.getConnection();
            Statement stmt = metaDbConn.createStatement()) {

            for (Map.Entry<String, Long> entry : seqMaxValues.entrySet()) {
                String phySeqName = genNameForNewSequence(entry.getKey());
                long maxValue = entry.getValue();

                if (maxValue <= 0L) {
                    continue;
                }

                String sql = String.format(SELECT_NEXTVAL_SKIP, phySeqName, maxValue);
                stmt.addBatch(sql);
            }

            stmt.executeBatch();
        } catch (SQLException e) {
            String errMsg = "Failed to update a batch of sequence values";
            logger.error(errMsg, e);
            throw new SequenceException(e, errMsg);
        }
    }

    private String genNameForNewSequence(String name) {
        return SequenceOptNewAccessor.genNameForNewSequence(schemaName, name);
    }

    @Override
    public int getRetryTimes() {
        // No retry.
        return 1;
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
        String message = "NewSequenceDao has been initialized";
        logger.info(message);
        LoggerInit.TDDL_SEQUENCE_LOG.info(message);
    }

}

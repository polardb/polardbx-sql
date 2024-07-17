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

package com.alibaba.polardbx.gms.metadb.seq;

import com.alibaba.polardbx.common.IdGenerator;
import com.alibaba.polardbx.common.constants.SequenceAttribute.Type;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import static com.alibaba.polardbx.common.constants.SequenceAttribute.CYCLE;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_INCREMENT_BY;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_INNER_STEP;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_MAX_VALUE;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_START_WITH;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_UNIT_COUNT;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_UNIT_INDEX;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.NEW_SEQ;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.NEW_SEQ_CACHE_SIZE;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.NOCYCLE;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.STR_NA;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.TIME_BASED;
import static com.alibaba.polardbx.gms.metadb.seq.SequenceAccessor.SEQ_TABLE;
import static com.alibaba.polardbx.gms.metadb.seq.SequenceOptAccessor.SEQ_OPT_TABLE;

public class SequencesAccessor extends AbstractAccessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(SequencesAccessor.class);

    private static final String WHERE_SCHEMA = " where `schema_name` = %s";

    private static final String SEQUENCE_UNION =
        "select * from ("
            + "select `id`, `schema_name`, `name`, if(`value` > 0, `value`, 'N/A') as `value`, "
            + "'N/A ' as `unit_count`, 'N/A  ' as `unit_index`, 'N/A   ' as `inner_step`, "
            + "if(`increment_by` > 0, `increment_by`, 'N/A') as `increment_by`, "
            + "if(`start_with` > 0, `start_with`, 'N/A') as `start_with`, "
            + "if(`max_value` > 0, `max_value`, 'N/A') as `max_value`, "
            + "if(`cycle` & 64 = 64, 'N/A', if(`cycle` & 1 = 1, 'Y', 'N')) as `cycle`, "
            + "if(`cycle` & 64 = 64, 'TIME', if(`cycle` & 128 = 128, 'NEW', 'SIMPLE')) as `type`, "
            + "`status`, `gmt_created`, `gmt_modified` "
            + "from " + SEQ_OPT_TABLE + WHERE_SCHEMA
            + " union "
            + "select `id`, `schema_name`, `name`, `value`, "
            + "`unit_count`, `unit_index`, `inner_step`, "
            + "'N/A' as `increment_by`, 'N/A' as `start_with`, 'N/A' as `max_value`, 'N/A' as `cycle`, "
            + "'GROUP' as `type`, `status`, `gmt_created`, `gmt_modified` "
            + "from " + SEQ_TABLE + WHERE_SCHEMA
            + ") as seq %s";

    private SequenceAccessor sequenceAccessor;
    private SequenceOptAccessor sequenceOptAccessor;
    private SequenceOptNewAccessor sequenceOptNewAccessor;

    public SequencesAccessor() {
        sequenceAccessor = new SequenceAccessor();
        sequenceOptAccessor = new SequenceOptAccessor();
        sequenceOptNewAccessor = new SequenceOptNewAccessor();
    }

    @Override
    public void setConnection(Connection connection) {
        super.setConnection(connection);
        sequenceAccessor.setConnection(connection);
        sequenceOptAccessor.setConnection(connection);
        sequenceOptNewAccessor.setConnection(connection);
    }

    public Set<String> queryNames(String schemaName) {
        Set<String> seqNames = new HashSet<>();

        List<SequenceRecord> sequenceRecords = sequenceAccessor.query(schemaName);
        if (sequenceRecords != null && sequenceRecords.size() > 0) {
            for (SequenceRecord sequenceRecord : sequenceRecords) {
                seqNames.add(sequenceRecord.name);
            }
        }

        List<SequenceOptRecord> sequenceOptRecords = sequenceOptAccessor.query(schemaName);
        if (sequenceOptRecords != null && sequenceOptRecords.size() > 0) {
            for (SequenceOptRecord sequenceOptRecord : sequenceOptRecords) {
                seqNames.add(sequenceOptRecord.name);
            }
        }

        return seqNames;
    }

    public SequenceBaseRecord query(String schemaName, String name) {
        SequenceRecord sequenceRecord = sequenceAccessor.query(schemaName, name);
        if (sequenceRecord == null) {
            SequenceOptRecord sequenceOptRecord = sequenceOptAccessor.query(schemaName, name);
            if (sequenceOptRecord != null && sequenceOptRecord.isNewSeq()) {
                long nextvalShown = sequenceOptNewAccessor.show(schemaName, sequenceOptRecord.name);
                sequenceOptRecord.value = nextvalShown;
            }
            return sequenceOptRecord;
        }
        return sequenceRecord;
    }

    public int count(String schemaName) {
        int count = sequenceAccessor.count(schemaName);
        count += sequenceOptAccessor.count(schemaName);
        return count;
    }

    public int insert(SequenceBaseRecord record, long newSeqCacheSize, Supplier<?> failPointInjector) {
        if (record instanceof SequenceRecord) {
            return sequenceAccessor.insert((SequenceRecord) record);
        } else if (record instanceof SequenceOptRecord) {
            SequenceOptRecord sequenceOptRecord = (SequenceOptRecord) record;
            if (sequenceOptRecord.isNewSeq()) {
                try {
                    sequenceOptNewAccessor.create(sequenceOptRecord, newSeqCacheSize);
                    failPointInjector.get();
                } catch (Exception e) {
                    boolean ignoreForIdempotentRecovery = false;
                    String seqName = sequenceOptNewAccessor.genNameForNewSequence(sequenceOptRecord);
                    if (TStringUtil.containsIgnoreCase(e.getMessage(),
                        String.format("Table '%s' already exists", seqName))) {
                        long start = sequenceOptNewAccessor.show(sequenceOptRecord.schemaName, sequenceOptRecord.name);
                        if (start == sequenceOptRecord.startWith) {
                            ignoreForIdempotentRecovery = true;
                        }
                    }
                    if (!ignoreForIdempotentRecovery) {
                        throw e;
                    }
                }
            }
            try {
                return sequenceOptAccessor.insert(sequenceOptRecord);
            } catch (Exception e) {
                // Roll the new sequence operation back.
                if (sequenceOptRecord.isNewSeq()) {
                    sequenceOptNewAccessor.drop(sequenceOptRecord);
                }
                throw e;
            }
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_UNEXPECTED, "record", record.getClass().getName());
        }
    }

    public int update(SequenceBaseRecord record, long newSeqCacheSize) {
        if (record instanceof SequenceRecord) {
            return sequenceAccessor.update((SequenceRecord) record);
        } else if (record instanceof SequenceOptRecord) {
            SequenceOptRecord sequenceOptRecord = (SequenceOptRecord) record;

            if (sequenceOptRecord.isNewSeq()) {
                SequenceOptRecord origRecord =
                    sequenceOptAccessor.query(sequenceOptRecord.schemaName, sequenceOptRecord.name);
                long origStartWith = origRecord != null ? origRecord.startWith : 0L;

                int affectedRows = sequenceOptNewAccessor.update(sequenceOptRecord);

                try {
                    boolean recreateUnderlyingSequence = false;

                    long actualNextval = origRecord.value;

                    try {
                        actualNextval =
                            sequenceOptNewAccessor.show(sequenceOptRecord.schemaName, sequenceOptRecord.name);
                    } catch (Exception e) {
                        if (e.getMessage().contains("has run out")) {
                            recreateUnderlyingSequence = true;
                        } else {
                            throw e;
                        }
                    }

                    if (!recreateUnderlyingSequence) {
                        if (sequenceOptRecord.isOnlyStartWithChanged()) {
                            if (sequenceOptRecord.startWith <= actualNextval) {
                                recreateUnderlyingSequence = true;
                            }
                        } else {
                            recreateUnderlyingSequence = true;
                        }
                    }

                    if (recreateUnderlyingSequence) {
                        // AliSQL Sequence doesn't support to change start value to a less value
                        // (no error, just ignore) or change other options, so we have to drop the
                        // old sequence and then create a new one with new start value and options.
                        sequenceOptNewAccessor.drop(sequenceOptRecord);
                        try {
                            fillSequenceRecord(sequenceOptRecord, origRecord, actualNextval);
                            sequenceOptNewAccessor.create(sequenceOptRecord, newSeqCacheSize);
                        } catch (Exception e) {
                            // Recover the original sequence since we failed to create a new one.
                            sequenceOptNewAccessor.create(origRecord, newSeqCacheSize);
                        }
                    } else {
                        sequenceOptNewAccessor.change(sequenceOptRecord);
                    }
                } catch (Exception e) {
                    // Rollback the sequence
                    if (origStartWith > 0L) {
                        sequenceOptNewAccessor.update(origRecord);
                    }
                    throw e;
                }

                return affectedRows;
            } else {
                return sequenceOptAccessor.update(sequenceOptRecord);
            }
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_UNEXPECTED, "record", record.getClass().getName());
        }
    }

    private void fillSequenceRecord(SequenceOptRecord newRecord, SequenceOptRecord oldRecord, long actualNextval) {
        if (newRecord.incrementBy <= 0) {
            newRecord.incrementBy = oldRecord.incrementBy > 0 ? oldRecord.incrementBy : DEFAULT_INCREMENT_BY;
        }

        if (newRecord.maxValue <= 0 || newRecord.maxValue < newRecord.startWith) {
            newRecord.maxValue =
                oldRecord.maxValue > 0 && oldRecord.maxValue > newRecord.maxValue ? oldRecord.maxValue :
                    DEFAULT_MAX_VALUE;
        }

        if (newRecord.startWith <= 0) {
            newRecord.startWith = oldRecord.startWith > 0 ? oldRecord.startWith : DEFAULT_START_WITH;
            if (actualNextval - oldRecord.incrementBy < newRecord.startWith) {
                newRecord.value = newRecord.startWith;
            } else {
                newRecord.value = actualNextval - oldRecord.incrementBy + newRecord.incrementBy;
                if (newRecord.value > newRecord.maxValue) {
                    newRecord.value = newRecord.startWith;
                }
            }
        } else {
            newRecord.value = newRecord.startWith;
        }

        if (!newRecord.cycleReset || (newRecord.cycle != (NEW_SEQ | CYCLE) && newRecord.cycle != (NEW_SEQ | NOCYCLE))) {
            newRecord.cycle = oldRecord.cycle;
        }
    }

    public int change(Pair<SequenceBaseRecord, SequenceBaseRecord> recordPair, long newSeqCacheSize,
                      Supplier<?> failPointInjector) {
        int count = 0;

        SequenceBaseRecord deletedRecord = recordPair.getKey();
        SequenceBaseRecord insertedRecord = recordPair.getValue();

        if ((deletedRecord instanceof SequenceOptRecord && ((SequenceOptRecord) deletedRecord).isNewSeq()) ||
            (insertedRecord instanceof SequenceOptRecord && ((SequenceOptRecord) insertedRecord).isNewSeq())) {
            // Change right now without transaction since New Sequence change is DDL.
            count += delete(deletedRecord);
            count += insert(insertedRecord, newSeqCacheSize, failPointInjector);
            return count;
        }

        try {
            MetaDbUtil.beginTransaction(connection);

            if (deletedRecord instanceof SequenceRecord) {
                count += sequenceAccessor.delete((SequenceRecord) deletedRecord);
            } else if (deletedRecord instanceof SequenceOptRecord) {
                count += sequenceOptAccessor.delete((SequenceOptRecord) deletedRecord);
            } else {
                throw new TddlRuntimeException(ErrorCode.ERR_GMS_UNEXPECTED, "record",
                    insertedRecord.getClass().getName());
            }

            if (insertedRecord instanceof SequenceRecord) {
                count += sequenceAccessor.insert((SequenceRecord) insertedRecord);
            } else if (insertedRecord instanceof SequenceOptRecord) {
                count += sequenceOptAccessor.insert((SequenceOptRecord) insertedRecord);
            } else {
                throw new TddlRuntimeException(ErrorCode.ERR_GMS_UNEXPECTED, "record",
                    insertedRecord.getClass().getName());
            }

            MetaDbUtil.commit(connection);
        } catch (SQLException e) {
            MetaDbUtil.rollback(connection, e, LOGGER, deletedRecord.schemaName, "change sequence type");
        } finally {
            MetaDbUtil.endTransaction(connection, LOGGER);
        }

        return count;
    }

    public int rename(SequenceBaseRecord record) {
        if (record instanceof SequenceRecord) {
            return sequenceAccessor.rename((SequenceRecord) record);
        } else if (record instanceof SequenceOptRecord) {
            SequenceOptRecord sequenceOptRecord = (SequenceOptRecord) record;
            if (sequenceOptRecord.isNewSeq()) {
                sequenceOptNewAccessor.rename(sequenceOptRecord);
            }
            try {
                return sequenceOptAccessor.rename(sequenceOptRecord);
            } catch (Exception e) {
                if (sequenceOptRecord.isNewSeq()) {
                    // Roll the new sequence operation back.
                    String tmpName = sequenceOptRecord.newName;
                    sequenceOptRecord.newName = sequenceOptRecord.name;
                    sequenceOptRecord.name = tmpName;
                    sequenceOptNewAccessor.rename(sequenceOptRecord);
                }
                throw e;
            }
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_UNEXPECTED, "record", record.getClass().getName());
        }
    }

    public boolean checkIfExists(String schemaName, String name) {
        return sequenceAccessor.checkIfExists(schemaName, name) || sequenceOptAccessor.checkIfExists(schemaName, name);
    }

    public int updateStatus(SequenceBaseRecord record, int newStatus) {
        if (record instanceof SequenceRecord) {
            SequenceRecord sequenceRecord = (SequenceRecord) record;
            sequenceRecord.status = newStatus;
            return sequenceAccessor.updateStatus(sequenceRecord);
        } else if (record instanceof SequenceOptRecord) {
            SequenceOptRecord sequenceOptRecord = (SequenceOptRecord) record;
            sequenceOptRecord.status = newStatus;
            return sequenceOptAccessor.updateStatus(sequenceOptRecord);
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_UNEXPECTED, "record", record.getClass().getName());
        }
    }

    public int delete(SequenceBaseRecord record) {
        if (record instanceof SequenceRecord) {
            return sequenceAccessor.delete((SequenceRecord) record);
        } else if (record instanceof SequenceOptRecord) {
            SequenceOptRecord sequenceOptRecord = (SequenceOptRecord) record;
            sequenceOptNewAccessor.drop(sequenceOptRecord);
            return sequenceOptAccessor.delete(sequenceOptRecord);
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_UNEXPECTED, "record", record.getClass().getName());
        }
    }

    public int deleteAll(String schemaName) {
        int count = 0;

        count += sequenceAccessor.deleteAll(schemaName);

        // Drop New Sequence first.
        List<SequenceOptRecord> records = sequenceOptAccessor.query(schemaName);
        for (SequenceOptRecord record : records) {
            if (record.isNewSeq()) {
                sequenceOptNewAccessor.drop(record);
            }
        }

        count += sequenceOptAccessor.deleteAll(schemaName);

        return count;
    }

    // Instance Level
    public List<SequencesRecord> show() {
        return show(null, "");
    }

    // Database Level
    public List<SequencesRecord> show(String schemaName, String whereClause) {
        try {
            schemaName = TStringUtil.isBlank(schemaName) ? "`schema_name`" : "'" + schemaName + "'";
            String sql = String.format(SEQUENCE_UNION, schemaName, schemaName, whereClause);
            List<SequencesRecord> records = MetaDbUtil.query(sql, SequencesRecord.class, connection);
            // Fetch currVal from each New Sequence.
            for (SequencesRecord record : records) {
                if (TStringUtil.equalsIgnoreCase(record.type, Type.NEW.name())) {
                    long nextvalShown = 0;
                    try {
                        nextvalShown = sequenceOptNewAccessor.show(record.schemaName, record.name);
                    } catch (Exception e) {
                        if (e.getMessage().contains("doesn't exist") ||
                            e.getMessage().contains("has run out")) {
                            record.value = STR_NA;
                        } else {
                            throw e;
                        }
                    }
                    record.value = String.valueOf(nextvalShown);
                }
            }
            return records;
        } catch (Exception e) {
            LOGGER.error("Failed to query sequence tables with union", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                "sequence tables with union",
                e.getMessage());
        }
    }

    public static int change(String schemaName, Type fromType, Type toType, Connection metaDbConn) {
        int numChanged = 0;

        SequencesAccessor sequencesAccessor = new SequencesAccessor();
        sequencesAccessor.setConnection(metaDbConn);

        if (toType == Type.GROUP) {
            numChanged = sequencesAccessor.changeToSeq(schemaName, fromType);
        } else if (fromType == Type.GROUP) {
            numChanged = sequencesAccessor.changeToSeqOpt(schemaName, toType);
        } else {
            numChanged = sequencesAccessor.changeAmongSeqOpt(schemaName, fromType, toType);
        }

        return numChanged;
    }

    private int changeToSeq(String schemaName, Type fromType) {
        int numChanged = 0;
        List<SequenceRecord> records = convertToSeq(schemaName, fromType);
        if (GeneralUtil.isNotEmpty(records)) {
            numChanged = moveToSeq(schemaName, records, fromType);
        }
        return numChanged;
    }

    private List<SequenceRecord> convertToSeq(String schemaName, Type fromType) {
        List<SequenceOptRecord> sourceRecords = querySourceRecords(schemaName, fromType);

        long timeBasedValue = DEFAULT_INNER_STEP;
        if (fromType == Type.TIME) {
            timeBasedValue = IdGenerator.getDefaultIdGenerator().nextId();
        }

        List<SequenceRecord> targetRecords = new ArrayList<>();

        for (SequenceOptRecord sourceRecord : sourceRecords) {
            SequenceRecord targetRecord = new SequenceRecord();

            targetRecord.schemaName = sourceRecord.schemaName;
            targetRecord.name = sourceRecord.name;
            targetRecord.newName = sourceRecord.newName;
            targetRecord.unitCount = DEFAULT_UNIT_COUNT;
            targetRecord.unitIndex = DEFAULT_UNIT_INDEX;
            targetRecord.innerStep = DEFAULT_INNER_STEP;
            targetRecord.status = sourceRecord.status;

            switch (fromType) {
            case NEW:
                targetRecord.value = sequenceOptNewAccessor.show(schemaName, sourceRecord.name);
                sequenceOptNewAccessor.drop(sourceRecord);
                break;
            case TIME:
                targetRecord.value = timeBasedValue;
                break;
            }

            targetRecords.add(targetRecord);
        }

        return targetRecords;
    }

    private List<SequenceOptRecord> querySourceRecords(String schemaName, Type fromType) {
        switch (fromType) {
        case NEW:
            return sequenceOptAccessor.query(schemaName, NEW_SEQ);
        case TIME:
            return sequenceOptAccessor.query(schemaName, TIME_BASED);
        }
        return new ArrayList<>();
    }

    private int moveToSeq(String schemaName, List<SequenceRecord> records, Type fromType) {
        removeSeqOpt(schemaName, fromType);
        sequenceAccessor.insert(schemaName, records);
        return records.size();
    }

    private void removeSeqOpt(String schemaName, Type fromType) {
        switch (fromType) {
        case NEW:
            sequenceOptAccessor.delete(schemaName, NEW_SEQ);
            break;
        case TIME:
            sequenceOptAccessor.delete(schemaName, TIME_BASED);
            break;
        }
    }

    private int changeToSeqOpt(String schemaName, Type toType) {
        int numChanged = 0;
        List<SequenceOptRecord> records = convertToSeqOpt(schemaName, toType);
        if (GeneralUtil.isNotEmpty(records)) {
            numChanged = moveToSeqOpt(schemaName, records);
        }
        return numChanged;
    }

    private List<SequenceOptRecord> convertToSeqOpt(String schemaName, Type toType) {
        List<SequenceRecord> sourceRecords = sequenceAccessor.queryForUpdate(schemaName);
        List<SequenceOptRecord> targetRecords = new ArrayList<>();

        for (SequenceRecord sourceRecord : sourceRecords) {
            SequenceOptRecord targetRecord = new SequenceOptRecord();

            targetRecord.schemaName = sourceRecord.schemaName;
            targetRecord.name = sourceRecord.name;
            targetRecord.newName = sourceRecord.newName;
            targetRecord.status = sourceRecord.status;

            switch (toType) {
            case NEW:
                targetRecord.value = sourceRecord.value + NEW_SEQ_CACHE_SIZE;
                targetRecord.incrementBy = DEFAULT_INCREMENT_BY;
                targetRecord.startWith = DEFAULT_START_WITH;
                targetRecord.maxValue = DEFAULT_MAX_VALUE;
                targetRecord.cycle = NEW_SEQ;
                sequenceOptNewAccessor.create(targetRecord, NEW_SEQ_CACHE_SIZE);
                break;
            case TIME:
                targetRecord.value = 0;
                targetRecord.incrementBy = 0;
                targetRecord.startWith = 0;
                targetRecord.maxValue = 0;
                targetRecord.cycle = TIME_BASED;
                break;
            }

            targetRecords.add(targetRecord);
        }

        return targetRecords;
    }

    private int moveToSeqOpt(String schemaName, List<SequenceOptRecord> records) {
        sequenceAccessor.deleteAll(schemaName);
        sequenceOptAccessor.insert(schemaName, records);
        return records.size();
    }

    private int changeAmongSeqOpt(String schemaName, Type fromType, Type toType) {
        int numChanged = 0;

        List<SequenceOptRecord> records = convertAmongSeqOpt(schemaName, fromType, toType);
        if (GeneralUtil.isNotEmpty(records)) {
            numChanged = moveAmongSeqOpt(schemaName, records, fromType);
        }

        return numChanged;
    }

    private List<SequenceOptRecord> convertAmongSeqOpt(String schemaName, Type fromType, Type toType) {
        List<SequenceOptRecord> sourceRecords = querySourceRecords(schemaName, fromType);

        long timeBasedValue = 1L;
        if (fromType == Type.TIME) {
            timeBasedValue = IdGenerator.getDefaultIdGenerator().nextId();
        }

        List<SequenceOptRecord> targetRecords = new ArrayList<>();

        for (SequenceOptRecord sourceRecord : sourceRecords) {
            SequenceOptRecord targetRecord = new SequenceOptRecord();

            targetRecord.schemaName = sourceRecord.schemaName;
            targetRecord.name = sourceRecord.name;
            targetRecord.newName = sourceRecord.newName;
            targetRecord.status = sourceRecord.status;

            switch (fromType) {
            case NEW:
                switch (toType) {
                case TIME:
                    targetRecord.value = 0;
                    targetRecord.incrementBy = 0;
                    targetRecord.startWith = 0;
                    targetRecord.maxValue = 0;
                    targetRecord.cycle = TIME_BASED;
                    sequenceOptNewAccessor.drop(sourceRecord);
                }
                break;
            case TIME:
                switch (toType) {
                case NEW:
                    targetRecord.value = timeBasedValue + DEFAULT_INCREMENT_BY;
                    targetRecord.startWith = DEFAULT_START_WITH;
                    targetRecord.incrementBy = DEFAULT_INCREMENT_BY;
                    targetRecord.maxValue = DEFAULT_MAX_VALUE;
                    targetRecord.cycle = NEW_SEQ | NOCYCLE;
                    sequenceOptNewAccessor.create(targetRecord, NEW_SEQ_CACHE_SIZE);
                }
                break;
            }

            targetRecords.add(targetRecord);
        }

        return targetRecords;
    }

    private int moveAmongSeqOpt(String schemaName, List<SequenceOptRecord> records, Type fromType) {
        removeSeqOpt(schemaName, fromType);
        sequenceOptAccessor.insert(schemaName, records);
        return records.size();
    }

}

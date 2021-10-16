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

import com.alibaba.polardbx.common.constants.SequenceAttribute;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.metadb.record.CountRecord;
import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.alibaba.polardbx.gms.metadb.seq.SequenceAccessor.SEQ_TABLE;
import static com.alibaba.polardbx.gms.metadb.seq.SequenceOptAccessor.SEQ_OPT_TABLE;

public class SequencesAccessor extends AbstractAccessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(SequencesAccessor.class);

    private static final String WHERE_SCHEMA = " where `schema_name` = ?";

    private static final String SEQUENCE_UNION =
        "select `schema_name`, `name`, if(`value` > 0, `value`, 'N/A') as `value`, 'N/A ' as `unit_count`,"
            + "'N/A  ' as `unit_index`, 'N/A   ' as `inner_step`, if(`increment_by` > 0, "
            + "`increment_by`, 'N/A') as `increment_by`, if(`start_with` > 0, `start_with`, 'N/A') as `start_with`, "
            + "if(`max_value` > 0, `max_value`, 'N/A') as `max_value`, "
            + "if(`cycle` & 64 = 64, 'N/A', if(`cycle` & 1 = 1, 'Y', 'N')) as `cycle`, "
            + "if(`cycle` & 64 = 64, 'TIME', if(`cycle` & 128 = 128, 'CACHE', 'SIMPLE')) as `type` "
            + "from " + SEQ_OPT_TABLE + WHERE_SCHEMA
            + "union "
            + "select `schema_name`, `name`, `value`, `unit_count`, `unit_index`, `inner_step`, "
            + "'N/A' as `increment_by`, 'N/A' as `start_with`, "
            + "'N/A' as `max_value`, 'N/A' as `cycle`, 'GROUP' as `type` "
            + "from " + SEQ_TABLE + WHERE_SCHEMA;

    private static final String WHERE_SEPARATE_SEQUENCES =
        " and `name` not like '" + SequenceAttribute.AUTO_SEQ_PREFIX + "%'";

    private static final String SEQUENCE_COUNT =
        "select count(*) from " + SEQ_TABLE + WHERE_SCHEMA + WHERE_SEPARATE_SEQUENCES;

    private static final String SEQUENCE_OPT_COUNT =
        "select count(*) from " + SEQ_OPT_TABLE + WHERE_SCHEMA + WHERE_SEPARATE_SEQUENCES;

    private SequenceAccessor sequenceAccessor;
    private SequenceOptAccessor sequenceOptAccessor;

    public SequencesAccessor() {
        sequenceAccessor = new SequenceAccessor();
        sequenceOptAccessor = new SequenceOptAccessor();
    }

    @Override
    public void setConnection(Connection connection) {
        super.setConnection(connection);
        sequenceAccessor.setConnection(connection);
        sequenceOptAccessor.setConnection(connection);
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

    public SystemTableRecord query(String schemaName, String name) {
        SystemTableRecord record = sequenceAccessor.query(schemaName, name);
        if (record == null) {
            record = sequenceOptAccessor.query(schemaName, name);
        }
        return record;
    }

    public int count(String schemaName) {
        int count = 0;
        List<CountRecord> seqRecords = query(SEQUENCE_COUNT, SEQ_TABLE, CountRecord.class, schemaName);
        if (seqRecords != null && seqRecords.size() > 0) {
            count += seqRecords.get(0).count;
        }
        List<CountRecord> seqOptRecords = query(SEQUENCE_OPT_COUNT, SEQ_OPT_TABLE, CountRecord.class, schemaName);
        if (seqOptRecords != null && seqOptRecords.size() > 0) {
            count += seqOptRecords.get(0).count;
        }
        return count;
    }

    public int insert(SystemTableRecord record) {
        if (record instanceof SequenceRecord) {
            return sequenceAccessor.insert((SequenceRecord) record);
        } else if (record instanceof SequenceOptRecord) {
            return sequenceOptAccessor.insert((SequenceOptRecord) record);
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_UNEXPECTED, "record", record.getClass().getName());
        }
    }

    public int update(SystemTableRecord record) {
        if (record instanceof SequenceRecord) {
            return sequenceAccessor.update((SequenceRecord) record);
        } else if (record instanceof SequenceOptRecord) {
            return sequenceOptAccessor.update((SequenceOptRecord) record);
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_UNEXPECTED, "record", record.getClass().getName());
        }
    }

    public int change(Pair<SystemTableRecord, SystemTableRecord> recordPair, String schemaName) {
        int count = 0;
        try {
            MetaDbUtil.beginTransaction(connection);

            SystemTableRecord deletedRecord = recordPair.getKey();
            SystemTableRecord insertedRecord = recordPair.getValue();

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
            MetaDbUtil.rollback(connection, e, LOGGER, schemaName, "switch sequence type");
        } finally {
            MetaDbUtil.endTransaction(connection, LOGGER);
        }
        return count;
    }

    public int rename(SystemTableRecord record) {
        if (record instanceof SequenceRecord) {
            return sequenceAccessor.rename((SequenceRecord) record);
        } else if (record instanceof SequenceOptRecord) {
            return sequenceOptAccessor.rename((SequenceOptRecord) record);
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_UNEXPECTED, "record", record.getClass().getName());
        }
    }

    public int updateStatus(SystemTableRecord record, int newStatus) {
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

    public int delete(SystemTableRecord record) {
        if (record instanceof SequenceRecord) {
            return sequenceAccessor.delete((SequenceRecord) record);
        } else if (record instanceof SequenceOptRecord) {
            return sequenceOptAccessor.delete((SequenceOptRecord) record);
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_UNEXPECTED, "record", record.getClass().getName());
        }
    }

    public int deleteAll(String schemaName) {
        int count = 0;
        count += sequenceAccessor.deleteAll(schemaName);
        count += sequenceOptAccessor.deleteAll(schemaName);
        return count;
    }

    public List<SequencesRecord> show(String schemaName) {
        try {
            Map<Integer, ParameterContext> params =
                MetaDbUtil.buildStringParameters(new String[] {schemaName, schemaName});
            return MetaDbUtil.query(SEQUENCE_UNION, params, SequencesRecord.class, connection);
        } catch (Exception e) {
            LOGGER.error("Failed to query sequence tables with union", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                "sequence tables with union",
                e.getMessage());
        }
    }

}

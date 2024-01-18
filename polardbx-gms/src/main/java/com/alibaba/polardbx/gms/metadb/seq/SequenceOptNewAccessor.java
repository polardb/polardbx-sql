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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.encrypt.MD5Utils;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.util.DdlMetaLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static com.alibaba.polardbx.common.constants.SequenceAttribute.CYCLE;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.NEW_SEQ;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.NEW_SEQ_PREFIX;
import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.UNDERSCORE;

public class SequenceOptNewAccessor extends SequenceOptAccessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(SequenceOptNewAccessor.class);

    private static final String CREATE_NEW_SEQ =
        "create sequence %s start with %s increment by %s minvalue %s maxvalue %s cache %s %s";

    private static final String SHOW_NEXTVAL = "select nextval_show(%s)";

    private static final String CHANGE_START_WITH = "select nextval_skip(%s, %s)";

    private static final String RENAME_NEW_SEQ = "rename sequence %s to %s";

    private static final String DROP_NEW_SEQ = "drop sequence if exists %s";

    public void create(SequenceOptRecord record, long cacheSize) {
        String seqName = genNameForNewSequence(record);
        String cycle = (record.cycle & CYCLE) == CYCLE ? "cycle" : "nocycle";
        execute(String.format(CREATE_NEW_SEQ, seqName, record.value, record.incrementBy, record.startWith,
            record.maxValue, cacheSize, cycle));
    }

    public long show(String schemaName, String name) {
        String seqName = genNameForNewSequence(schemaName, name);
        String sql = String.format(SHOW_NEXTVAL, seqName);
        try (Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                return rs.getLong(1);
            }
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_NEW_SEQUENCE, sql, "no result");
        } catch (SQLException e) {
            LOGGER.error(String.format("Failed to %s", sql), e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_NEW_SEQUENCE, e, sql, e.getMessage());
        }
    }

    public void change(SequenceOptRecord record) {
        String seqName = genNameForNewSequence(record);
        execute(String.format(CHANGE_START_WITH, seqName, record.startWith - 1), false);
    }

    @Override
    protected String buildCycle(SequenceOptRecord record) {
        StringBuilder sql = new StringBuilder();

        int newCycle = record.cycle & CYCLE;

        buildCycle(newCycle, record.cycleReset, sql);

        sql.append(")  | ").append(NEW_SEQ);

        return sql.toString();
    }

    @Override
    public int rename(SequenceOptRecord record) {
        String oldName = genNameForNewSequence(record);
        String newName = genNameForNewSequence(record.schemaName, record.newName);
        execute(String.format(RENAME_NEW_SEQ, oldName, newName));
        return 0;
    }

    public void drop(SequenceOptRecord record) {
        String seqName = genNameForNewSequence(record);
        execute(String.format(DROP_NEW_SEQ, seqName));
    }

    private void execute(String sql) {
        execute(sql, true);
    }

    private void execute(String sql, boolean isDDL) {
        try {
            DdlMetaLogUtil.logSql(sql);
            if (isDDL) {
                MetaDbUtil.executeDDL(sql, connection);
            } else {
                MetaDbUtil.execute(sql, connection);
            }
        } catch (SQLException e) {
            LOGGER.error(String.format("Failed to execute sequence operation '%s'", sql), e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_NEW_SEQUENCE, e, sql, e.getMessage());
        }
    }

    public String genNameForNewSequence(SequenceOptRecord record) {
        return genNameForNewSequence(record.schemaName, record.name);
    }

    public static String genNameForNewSequence(String schemaName, String name) {
        String origSeqName = (schemaName + UNDERSCORE + name).toLowerCase();
        // We have to use the MD5 string (32-char) of original sequence name to combine physical sequence
        // name since MySQL has 64-char limit for identifier name (AliSQL Sequence is actually a table).
        return NEW_SEQ_PREFIX + MD5Utils.getInstance().getMD5String(origSeqName);
    }

}

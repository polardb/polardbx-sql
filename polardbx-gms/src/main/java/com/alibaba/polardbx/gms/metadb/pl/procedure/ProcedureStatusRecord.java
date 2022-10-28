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

package com.alibaba.polardbx.gms.metadb.pl.procedure;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * @author yuehan.wcf
 */
public class ProcedureStatusRecord extends ProcedureMetaRecord {
    public String type;
    public String definer;
    public Timestamp lastAltered;
    public Timestamp created;
    public String securityType;
    public String routineComment;
    public String characterSetClient;
    public String collationConnection;
    public String databaseCollation;

    @Override
    public ProcedureStatusRecord fill(ResultSet rs) throws SQLException {
        ProcedureStatusRecord record = new ProcedureStatusRecord();
        record.schema = rs.getString("ROUTINE_SCHEMA");
        record.name = rs.getString("ROUTINE_NAME");
        record.type = rs.getString("ROUTINE_TYPE");
        record.definer = rs.getString("DEFINER");
        record.lastAltered = rs.getTimestamp("LAST_ALTERED");
        record.created = rs.getTimestamp("CREATED");
        record.securityType = rs.getString("SECURITY_TYPE");
        record.routineComment = rs.getString("ROUTINE_COMMENT");
        record.characterSetClient = rs.getString("CHARACTER_SET_CLIENT");
        record.collationConnection = rs.getString("COLLATION_CONNECTION");
        record.databaseCollation = rs.getString("DATABASE_COLLATION");
        return record;
    }
}

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

public class ProcedureDefinitionRecord extends ProcedureMetaRecord {
    public String definition;

    @Override
    public ProcedureDefinitionRecord fill(ResultSet rs) throws SQLException {
        ProcedureDefinitionRecord record = new ProcedureDefinitionRecord();
        record.schema = rs.getString("ROUTINE_SCHEMA");
        record.name = rs.getString("ROUTINE_NAME");
        record.definition = rs.getString("ROUTINE_DEFINITION");
        return record;
    }
}

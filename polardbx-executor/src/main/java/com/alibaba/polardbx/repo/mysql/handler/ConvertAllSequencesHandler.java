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

package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.executor.ddl.job.factory.ConvertAllSequencesJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.handler.ddl.LogicalCommonDdlHandler;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.metadb.table.SchemataAccessor;
import com.alibaba.polardbx.gms.metadb.table.SchemataRecord;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.sequence.exception.SequenceException;
import org.apache.calcite.sql.SqlConvertAllSequences;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static com.alibaba.polardbx.common.constants.SequenceAttribute.Type;

public class ConvertAllSequencesHandler extends LogicalCommonDdlHandler {

    public ConvertAllSequencesHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        SqlConvertAllSequences stmt = (SqlConvertAllSequences) logicalDdlPlan.getNativeSqlNode();

        Type fromType = stmt.getFromType();
        Type toType = stmt.getToType();
        String schemaName = stmt.getSchemaName();
        boolean allSchemata = stmt.isAllSchemata();

        final Set<String> userSchemata = new HashSet<>();
        List<SchemataRecord> schemataRecords = SchemataAccessor.getAllSchemata();
        schemataRecords.stream()
            .filter(s -> !SystemDbHelper.isDBBuildIn(s.schemaName))
            .forEach(s -> userSchemata.add(s.schemaName.toLowerCase()));

        final Set<String> schemaToBeConvert = new TreeSet<>(String::compareToIgnoreCase);
        if (allSchemata) {
            schemaToBeConvert.addAll(userSchemata);
        } else if (TStringUtil.isNotBlank(schemaName)) {
            if (userSchemata.contains(schemaName)) {
                schemaToBeConvert.add(schemaName);
            } else {
                throw new SequenceException("Invalid schema name '" + schemaName + "'");
            }
        } else {
            throw new SequenceException("Schema name should not be empty");
        }

        DdlJobFactory convertAllSequencesJobFactory = new ConvertAllSequencesJobFactory(
            new ArrayList<>(schemaToBeConvert),
            fromType,
            toType,
            !allSchemata,
            executionContext
        );

        return convertAllSequencesJobFactory.create();
    }

}

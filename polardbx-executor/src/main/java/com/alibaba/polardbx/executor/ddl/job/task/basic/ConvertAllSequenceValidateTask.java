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

package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.ddl.job.task.BaseValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.gms.metadb.table.SchemataAccessor;
import com.alibaba.polardbx.gms.metadb.table.SchemataRecord;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
@Getter
@TaskName(name = "ConvertAllSequenceValidateTask")
public class ConvertAllSequenceValidateTask extends BaseValidateTask {
    private List<String> allSchemaNamesTobeConvert;
    private boolean onlySingleSchema;

    @JSONCreator
    public ConvertAllSequenceValidateTask(List<String> allSchemaNamesTobeConvert, boolean onlySingleSchema) {
        super(allSchemaNamesTobeConvert.get(0));
        this.allSchemaNamesTobeConvert = allSchemaNamesTobeConvert;
        this.onlySingleSchema = onlySingleSchema;
    }

    @Override
    public void executeImpl(ExecutionContext executionContext) {
        final Set<String> userSchemata = new HashSet<>();
        List<SchemataRecord> schemataRecords = SchemataAccessor.getAllSchemata();
        schemataRecords.stream()
            .filter(s -> !SystemDbHelper.isDBBuildIn(s.schemaName))
            .forEach(s -> userSchemata.add(s.schemaName.toLowerCase()));

        if (!onlySingleSchema) {
            Set<String> schemasBefore = allSchemaNamesTobeConvert.stream().map(
                x -> x.toLowerCase()
            ).collect(Collectors.toSet());

            Set<String> schemasNow = userSchemata;
            for (String before : schemasBefore) {
                if (!schemasNow.contains(before)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                        String.format("not found schema [%s], please retry", before));
                }
            }
            for (String nowSc : schemasNow) {
                if (!schemasBefore.contains(nowSc)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                        String.format("newly built schema [%s] found, please retry", nowSc));
                }
            }
        } else {
            String schema = allSchemaNamesTobeConvert.get(0);
            if (!userSchemata.contains(schema.toLowerCase())) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                    String.format("schema [%s] not found, please retry", schema));
            }
        }
    }

}

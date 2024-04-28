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

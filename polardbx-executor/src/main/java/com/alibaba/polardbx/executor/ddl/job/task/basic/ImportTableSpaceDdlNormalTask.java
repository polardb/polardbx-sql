package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.ddl.job.builder.AlterTableImportTableSpaceBuilder;
import com.alibaba.polardbx.executor.ddl.job.builder.DdlPhyPlanBuilder;
import com.alibaba.polardbx.executor.ddl.job.task.BasePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Getter
@TaskName(name = "ImportTableSpaceDdlNormalTask")
public class ImportTableSpaceDdlNormalTask extends BasePhyDdlTask {

    private final String tableName;
    private final Map<String, List<List<String>>> tableTopology;

    @JSONCreator
    public ImportTableSpaceDdlNormalTask(String schemaName,
                                         String tableName,
                                         Map<String, List<List<String>>> tableTopology) {
        super(schemaName, null);
        this.tableName = tableName;
        this.tableTopology = tableTopology;
    }

    @Override
    public void executeImpl(ExecutionContext executionContext) {
        boolean executeInLeader = executionContext.getParamManager()
            .getBoolean(ConnectionParams.PHYSICAL_BACKFILL_IMPORT_TABLESPACE_BY_LEADER);
        if (executeInLeader) {
            DdlPhyPlanBuilder builder =
                AlterTableImportTableSpaceBuilder.createBuilder(
                    schemaName, tableName, true, tableTopology, executionContext).build();

            this.physicalPlanData = builder.genPhysicalPlanData();

            super.executeImpl(executionContext);
        }
    }

    @Override
    public String remark() {
        StringBuilder sb = new StringBuilder();
        sb.append("(");
        for (Map.Entry<String, List<List<String>>> entry : tableTopology.entrySet()) {
            sb.append(entry.getKey());
            sb.append(".(");
            sb.append(String.join(",", entry.getValue().get(0)));
            sb.append(") ");
        }
        sb.append(")");
        return "|alter table " + sb + " import tablespace";
    }

    public List<String> explainInfo() {
        StringBuilder sb = new StringBuilder();
        sb.append("(");
        for (Map.Entry<String, List<List<String>>> entry : tableTopology.entrySet()) {
            sb.append(entry.getKey());
            sb.append(".(");
            sb.append(String.join(",", entry.getValue().get(0)));
            sb.append(") ");
        }
        sb.append(")");
        List<String> command = new ArrayList<>(1);
        command.add(sb.toString());
        return command;
    }

}

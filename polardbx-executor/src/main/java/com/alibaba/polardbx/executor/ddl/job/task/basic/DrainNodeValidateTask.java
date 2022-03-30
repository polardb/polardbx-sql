package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.executor.balancer.stats.StatsUtils;
import com.alibaba.polardbx.executor.ddl.job.task.BaseValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.job.validator.GsiValidator;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.google.common.base.Joiner;
import lombok.Getter;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

@Getter
@TaskName(name = "DrainNodeValidateTask")
public class DrainNodeValidateTask extends BaseValidateTask {

    private final static Logger LOG = SQLRecorderLogger.ddlLogger;
    private List<TableGroupConfig> tableGroupConfigs;
    @JSONCreator
    public DrainNodeValidateTask(String schemaName, List<TableGroupConfig> tableGroupConfigs) {
        super(schemaName);
        this.tableGroupConfigs = tableGroupConfigs;
    }

    @Override
    public void executeImpl(ExecutionContext executionContext) {
        List<TableGroupConfig> curTableGroupConfigs = TableGroupUtils.getAllTableGroupInfoByDb(schemaName);
        Map<String, TableGroupConfig> curTableGroupMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        Map<String, TableGroupConfig> saveTableGroupMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

        curTableGroupConfigs.stream().forEach(o->curTableGroupMap.put(o.getTableGroupRecord().tg_name, o));
        tableGroupConfigs.stream().forEach(o->saveTableGroupMap.put(o.getTableGroupRecord().tg_name, o));

        if (curTableGroupMap.size() == saveTableGroupMap.size()) {
            for(Map.Entry<String, TableGroupConfig> entry:saveTableGroupMap.entrySet()) {
                if (!curTableGroupMap.containsKey(entry.getKey())) {
                    throw new TddlRuntimeException(ErrorCode.ERR_TABLEGROUP_META_TOO_OLD,
                        String.format("the metadata of tableGroup[%s] is too old, please retry this command",
                            entry.getKey()));
                }
                TableValidator.validateTableGroupChange(curTableGroupMap.get(entry.getKey()), entry.getValue());
                for (PartitionGroupRecord record : GeneralUtil.emptyIfNull(
                    entry.getValue().getPartitionGroupRecords())) {
                    LOG.info(record.digest());
                }
            }
        } else {
            Set<String> resSet1 = new HashSet<>();
            resSet1.addAll(curTableGroupMap.keySet());
            resSet1.removeAll(saveTableGroupMap.keySet());
            Set<String> resSet2 = new HashSet<>();
            resSet2.addAll(saveTableGroupMap.keySet());
            resSet2.removeAll(curTableGroupMap.keySet());
            resSet1.addAll(resSet2);

            throw new TddlRuntimeException(ErrorCode.ERR_TABLEGROUP_META_TOO_OLD,
                String.format("the metadata of tableGroup[%s] is too old, please retry this command",
                    resSet1.stream().collect(Collectors.joining(","))));
        }
    }

    @Override
    protected String remark() {
        return "|DrainNodeValidateTask: " + tableGroupConfigs.stream().map(TableGroupConfig::getTableGroupRecord).map(
            TableGroupRecord::getTg_name).collect(Collectors.joining(","));
    }
}

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

package com.alibaba.polardbx.executor.ddl.job.task.gsi;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.gsi.CheckerManager;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.apache.commons.collections.CollectionUtils;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Show checker report
 *
 * @author moyi
 * @since 2021/07
 */
public class ShowCheckReportTask extends CheckReportBaseTask {

    private List<CheckerManager.CheckerReport> checkerReports;

    public ShowCheckReportTask(String schemaName, String tableName, String indexName) {
        super(schemaName, tableName, indexName);
    }

    public List<CheckerManager.CheckerReport> getCheckerReports() {
        return this.checkerReports;
    }

    @Override
    protected void beforeTransaction(ExecutionContext ec) {
        show(ec);
    }

    public void show(ExecutionContext ec) {
        CheckerManager cm = new CheckerManager(schemaName);
        List<Long> jobs = cm.queryJobId(schemaName, tableName, indexName);
        long earlyFailNumber = ec.getParamManager()
            .getLong(ConnectionParams.GSI_EARLY_FAIL_NUMBER);
        FailPoint.injectRandomExceptionFromHint(ec);
        FailPoint.injectRandomSuspendFromHint(ec);
        if (CollectionUtils.isEmpty(jobs)) {
            checkerReports = Collections.emptyList();
            finalResult = "Checker result not found.";
        } else {
            final long targetJobId = jobs.get(jobs.size() - 1);
            final long reportCount = cm.countReports(targetJobId);
            if (reportCount >= earlyFailNumber) {
                final CheckerManager.CheckerReport finishMark = cm.queryFinishReport(targetJobId);
                checkerReports = Collections.emptyList();
                finalResult = (finishMark != null ? finishMark.getDetails() : "Checker is still running.")
                    + " Show report rows limit exceeded. Use SQL: { /*+TDDL: node(0)*/select * from `__drds__systable__checker_reports__` where `JOB_ID` = "
                    + targetJobId + "; } with proper filter condition to view all reports.";
            } else {
                checkerReports = cm.queryReports(targetJobId);
                Optional<CheckerManager.CheckerReport> finishMark = checkerReports.stream()
                    .filter(
                        report -> report.getStatus() == CheckerManager.CheckerReportStatus.FINISH.getValue())
                    .findFirst();
                // Drop the start and finish mark.
                checkerReports.removeIf(
                    report -> report.getStatus() == CheckerManager.CheckerReportStatus.FINISH.getValue()
                        || report.getStatus() == CheckerManager.CheckerReportStatus.START.getValue());
                if (finishMark.isPresent()) {
                    // Form the final summary.
                    if (checkerReports.isEmpty()) {
                        finalResult = "OK (" + finishMark.get().getDetails() + ") Finish time: "
                            + finishMark.get().getTimestamp();
                    } else {
                        if (checkerReports.size() >= earlyFailNumber) {
                            finalResult = "Error limit exceeded, stop checking ("
                                + finishMark.get().getDetails() + ") Finish time: "
                                + finishMark.get().getTimestamp();
                        } else {
                            finalResult = checkerReports.size() + " error found ("
                                + finishMark.get().getDetails() + ") Finish time: "
                                + finishMark.get().getTimestamp();
                        }
                    }
                } else {
                    finalResult = "Checker is still running.";
                }
            }
        }
    }

}

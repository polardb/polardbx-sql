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

package com.alibaba.polardbx.executor.handler.ddl;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ddl.job.MockDdlJob;
import com.alibaba.polardbx.executor.ddl.job.factory.oss.UnArchiveJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.TransientDdlJob;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.executor.utils.failpoint.FailPointKey;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalUnArchive;
import org.apache.calcite.sql.SqlUnArchive;

import static com.alibaba.polardbx.executor.utils.failpoint.FailPointKey.FP_HIJACK_DDL_JOB;

/**
 * @author Shi Yuxuan
 */
public class LogicalUnArchiveHandler extends LogicalCommonDdlHandler {

    public LogicalUnArchiveHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalUnArchive logicalUnArchive = (LogicalUnArchive) logicalDdlPlan;
        logicalUnArchive.preparedData();

        FlexDdlBuilder flexDdlBuilder = new FlexDdlBuilder();
        FailPoint.injectFromHint(FP_HIJACK_DDL_JOB, executionContext, (k, v) -> {
            String[] pair = v.replace(" ", "").split(",");
            int expectNodeCount = Integer.parseInt(pair[0]);
            int maxOutEdgeCount = Integer.parseInt(pair[1]);
            int edgeRate = Integer.parseInt(pair[2]);
            boolean mockSubJob = FailPoint.isKeyEnable(FailPointKey.FP_INJECT_SUBJOB);
            ExecutableDdlJob hiJackJob =
                new MockDdlJob(expectNodeCount, maxOutEdgeCount, edgeRate, mockSubJob).create();
            flexDdlBuilder.setHiJackJob(hiJackJob);
            //executableDdlJob.overrideTasks(hiJackJob);
        });
        return flexDdlBuilder.build(logicalUnArchive, executionContext);
    }

    @Override
    protected String getObjectName(BaseDdlOperation logicalDdlPlan) {
        final SqlUnArchive sqlUnArchive = (SqlUnArchive) logicalDdlPlan.getNativeSqlNode();
        return sqlUnArchive.getTarget().toString();
    }

    static class FlexDdlBuilder {

        private ExecutableDdlJob hiJackJob;

        public FlexDdlBuilder() {
            this.hiJackJob = null;
        }

        public void setHiJackJob(ExecutableDdlJob hiJackJob) {
            this.hiJackJob = hiJackJob;
        }

        ExecutableDdlJob build(LogicalUnArchive logicalUnArchive, ExecutionContext executionContext) {
            // mock
            if (hiJackJob != null) {
                ExecutableDdlJob newDdlJob =
                    UnArchiveJobFactory.create(logicalUnArchive.getPreparedData(), executionContext);
                newDdlJob.overrideTasks(hiJackJob);
                return newDdlJob;
            }

            return GeneralUtil.isEmpty(logicalUnArchive.getPreparedData().getTables()) ?
                new TransientDdlJob() :
                UnArchiveJobFactory.create(logicalUnArchive.getPreparedData(), executionContext);
        }
    }
}

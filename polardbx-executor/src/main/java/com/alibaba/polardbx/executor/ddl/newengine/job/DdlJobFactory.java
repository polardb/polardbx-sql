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

package com.alibaba.polardbx.executor.ddl.newengine.job;

import com.alibaba.polardbx.executor.ddl.job.MockDdlJob;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.executor.utils.failpoint.FailPointKey;
import org.apache.commons.lang3.StringUtils;

import java.util.Set;

import static com.alibaba.polardbx.executor.utils.failpoint.FailPointKey.FP_HIJACK_DDL_JOB;

/**
 * @author guxu
 */
public abstract class DdlJobFactory {

    public ExecutableDdlJob create() {
        return create(true);
    }

    public ExecutableDdlJob create(boolean validate) {
        if (validate) {
            validate();
        }
        ExecutableDdlJob executableDdlJob = doCreate();
        excludeResources(executableDdlJob.getExcludeResources());
        sharedResources(executableDdlJob.getSharedResources());

        //this is a quite interesting injection
        FailPoint.inject(FP_HIJACK_DDL_JOB, (k, v) -> {
            if (!(this instanceof MockDdlJob)) {
                String[] pair = v.replace(" ", "").split(",");
                int expectNodeCount = Integer.valueOf(pair[0]);
                int maxOutEdgeCount = Integer.valueOf(pair[1]);
                int edgeRate = Integer.valueOf(pair[2]);
                boolean mockSubJob = FailPoint.isKeyEnable(FailPointKey.FP_INJECT_SUBJOB);
                ExecutableDdlJob hiJackJob =
                    new MockDdlJob(expectNodeCount, maxOutEdgeCount, edgeRate, mockSubJob).create();
                executableDdlJob.overrideTasks(hiJackJob);
            }
        });

        return executableDdlJob;
    }

    /**
     * validate whether a Job can be created
     */
    protected abstract void validate();

    /**
     * @return gnerate an executable DDL Job
     */
    protected abstract ExecutableDdlJob doCreate();

    /**
     * try to acquire exclusive resource
     * blocked if fail to
     */
    protected abstract void excludeResources(Set<String> resources);

    /**
     * try to acquire shared resource
     * blocked if fail to
     */
    protected abstract void sharedResources(Set<String> resources);

    public static String concatWithDot(String schemaName, String tableName) {
        if (StringUtils.isEmpty(schemaName)) {
            return tableName;
        }
        return schemaName + "." + tableName;
    }

}

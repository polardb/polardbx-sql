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

package com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement;

import com.alibaba.polardbx.druid.sql.ast.SQLStatement;

import java.util.ArrayList;
import java.util.List;

/**
 * @version 1.0
 * @ClassName DrdsGenericDDLJob
 * @description
 * @Author chensr
 * @Date 2021-01-06
 */
public abstract class DrdsGenericDDLJob extends MySqlStatementImpl implements SQLStatement {

    private boolean allJobs = false;
    private List<Long> jobIds = new ArrayList<Long>();

    public boolean isAllJobs() {
        return allJobs;
    }

    public void setAllJobs(boolean allJobs) {
        this.allJobs = allJobs;
    }

    public List<Long> getJobIds() {
        return jobIds;
    }

    public void addJobId(long id) {
        jobIds.add(id);
    }
}

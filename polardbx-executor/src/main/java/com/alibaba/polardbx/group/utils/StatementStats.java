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

package com.alibaba.polardbx.group.utils;

/**
 * @author chenghui.lch
 */
public class StatementStats {

    protected long prepStmtEnvNano = 0;
    protected long createAndInitStmtNano = 0;
    protected long executeStmtNano = 0;

    protected long closeResultSetNano = 0;

    public long getExecuteStmtNano() {
        return executeStmtNano;
    }

    public void addExecuteStmtNano(long executeStmtNano) {
        this.executeStmtNano += executeStmtNano;
    }

    public long getCreateAndInitStmtNano() {
        return createAndInitStmtNano;
    }

    public void addCreateAndInitStmtNano(long createAndInitStmtNano) {
        this.createAndInitStmtNano += createAndInitStmtNano;
    }

    public long getPrepStmtEnvNano() {
        return prepStmtEnvNano;
    }

    public void addPrepStmtEnvNano(long prepStmtEnvNano) {
        this.prepStmtEnvNano += prepStmtEnvNano;
    }

    public long getCloseResultSetNano() {
        return closeResultSetNano;
    }

    public void addCloseResultSetNano(long closeResultSetNano) {
        this.closeResultSetNano += closeResultSetNano;
    }

}

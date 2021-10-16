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

package com.alibaba.polardbx.executor.ddl.newengine.sync;

import com.alibaba.polardbx.executor.ddl.newengine.DdlEngineScheduler;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.gms.sync.IGmsSyncAction;

public class DdlRequestSyncAction implements IGmsSyncAction {

    private DdlRequest ddlRequest;

    public DdlRequestSyncAction() {
    }

    public DdlRequestSyncAction(DdlRequest request) {
        this.ddlRequest = request;
    }

    @Override
    public Object sync() {
        if (ExecUtils.hasLeadership(null)) {
            DdlEngineScheduler.getInstance().notify(ddlRequest);
        }
        return null;
    }

    public DdlRequest getDdlRequest() {
        return ddlRequest;
    }

    public void setDdlRequest(DdlRequest ddlRequest) {
        this.ddlRequest = ddlRequest;
    }

}

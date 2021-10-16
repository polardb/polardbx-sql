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

import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.executor.ddl.newengine.DdlEngineRequester;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.gms.sync.IGmsSyncAction;

public class DdlResponseSyncAction implements IGmsSyncAction {

    private String responseNode;
    private DdlResponse ddlResponse;

    public DdlResponseSyncAction() {
    }

    public DdlResponseSyncAction(String responseNode, DdlResponse ddlResponse) {
        this.responseNode = responseNode;
        this.ddlResponse = ddlResponse;
    }

    @Override
    public Object sync() {
        String localServerKey = DdlHelper.getLocalServerKey();
        if (TStringUtil.isEmpty(responseNode) || TStringUtil.equals(responseNode, localServerKey)) {
            DdlEngineRequester.addResponses(ddlResponse.getResponses());
        }
        return null;
    }

    public String getResponseNode() {
        return responseNode;
    }

    public void setResponseNode(String responseNode) {
        this.responseNode = responseNode;
    }

    public DdlResponse getDdlResponse() {
        return ddlResponse;
    }

    public void setDdlResponse(DdlResponse ddlResponse) {
        this.ddlResponse = ddlResponse;
    }

}

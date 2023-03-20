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

package com.alibaba.polardbx;

import com.alibaba.polardbx.gms.node.GmsNodeManager;
import com.alibaba.polardbx.gms.node.GmsNodeManager.GmsNode;
import org.apache.commons.lang.StringUtils;

import java.util.List;

/**
 * @author xianmao.hexm
 */
public class ManagerPrivileges extends CobarPrivileges {

    @Override
    public boolean schemaExists(String schema) {
        return true;

    }

    @Override
    public boolean isTrustedIp(String host, String user) {
        if (super.isTrustedIp(host, user)) {
            return true;
        }

        List<GmsNode> gmsNodes = GmsNodeManager.getInstance().getAllTrustedNodes();

        for (GmsNode node : gmsNodes) {
            if (node != null && node.getHost().equals(host)) {
                // 严格校验非信任ip的user
                String instanceId = CobarServer.getInstance().getConfig().getSystem().getInstanceId();
                String clusterName = CobarServer.getInstance().getConfig().getSystem().getClusterName();
                if (StringUtils.equals(instanceId, user) || StringUtils.equals(clusterName, user)) {
                    return true;
                }
            }
        }
        return false;
    }
}

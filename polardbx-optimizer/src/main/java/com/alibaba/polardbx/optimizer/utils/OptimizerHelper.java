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

package com.alibaba.polardbx.optimizer.utils;

import com.alibaba.polardbx.optimizer.config.server.IServerConfigManager;

/**
 * @author chenghui.lch 2018年5月24日 下午3:17:07
 * @since 5.0.0
 */
public class OptimizerHelper {

    /**
     * 该组件在APP_NAME级别也是无状态的
     */
    private volatile static IServerConfigManager serverConfigManager;

    public static void init(IServerConfigManager svrConfigMgr) {
        if (serverConfigManager == null) {
            serverConfigManager = svrConfigMgr;
        }
    }

    public static IServerConfigManager getServerConfigManager() {
        return serverConfigManager;
    }
}

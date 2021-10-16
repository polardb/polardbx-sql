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

package com.alibaba.polardbx.optimizer.ccl;

import com.alibaba.polardbx.optimizer.ccl.service.impl.CclConfigService;
import com.alibaba.polardbx.optimizer.ccl.service.impl.CclService;
import com.alibaba.polardbx.optimizer.ccl.service.impl.CclTriggerService;
import com.alibaba.polardbx.optimizer.ccl.service.ICclConfigService;
import com.alibaba.polardbx.optimizer.ccl.service.ICclService;
import com.alibaba.polardbx.optimizer.ccl.service.ICclTriggerService;

/**
 * @author busu
 * date: 2020/10/30 2:29 下午
 */
public class CclManager {

    private final static ICclConfigService cclConfigService = new CclConfigService();

    private final static ICclService service = new CclService(cclConfigService);

    private final static ICclTriggerService cclTriggerService = new CclTriggerService(cclConfigService);

    static {
        cclConfigService.init(service, cclTriggerService);
    }

    public static ICclService getService() {
        return service;
    }

    public static ICclConfigService getCclConfigService() {
        return cclConfigService;
    }

    public static ICclTriggerService getTriggerService() {
        return cclTriggerService;
    }

}

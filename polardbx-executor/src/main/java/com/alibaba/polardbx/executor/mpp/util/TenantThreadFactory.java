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

package com.alibaba.polardbx.executor.mpp.util;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;

/**
 *
 **/
public class TenantThreadFactory extends NamedThreadFactory {

    public final static String THREAD_NAME_AP_PREFIX = "ap-processor-";

    private static final Logger log = LoggerFactory.getLogger(TenantThreadFactory.class);

    public TenantThreadFactory(String prefix, boolean daemon) {
        super(THREAD_NAME_AP_PREFIX + prefix, daemon);
    }

}

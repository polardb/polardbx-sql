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

package com.alibaba.polardbx.executor.mpp;

import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.executor.mpp.util.TenantThreadFactory;

import java.util.concurrent.ThreadFactory;

public class Threads {
    public static final boolean ENABLE_WISP = System.getProperties().containsKey(
        "com.alibaba.wisp.transparentWispSwitch");

    public static final boolean ENABLE_CGROUP;

    static {
        final String CGROUP_KEY = "com.alibaba.polardbx.cgroup";
        if (System.getProperties().containsKey(CGROUP_KEY)) {
            String val = System.getProperties().getProperty(CGROUP_KEY);
            ENABLE_CGROUP = Boolean.parseBoolean(val);
        } else {
            ENABLE_CGROUP = false;
        }
    }

    public static ThreadFactory threadsNamed(String nameFormat) {
        return new TenantThreadFactory(nameFormat, false);
    }

    public static ThreadFactory daemonThreadsNamed(String nameFormat) {
        return new TenantThreadFactory(nameFormat, true);
    }

    public static ThreadFactory tpThreadsNamed(String nameFormat) {
        return new NamedThreadFactory(nameFormat, false);
    }

    public static ThreadFactory tpDaemonThreadsNamed(String nameFormat) {
        return new NamedThreadFactory(nameFormat, true);
    }
}

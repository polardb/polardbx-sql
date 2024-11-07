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

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.polardbx.optimizer.spill;

import com.alibaba.polardbx.common.properties.FileConfig;

public class SpillSpaceManager extends SpillSpaceMonitor {

    public static final SpillSpaceMonitor getInstance() {
        return INSTANCE;
    }

    private static final SpillSpaceMonitor INSTANCE = new SpillSpaceManager();

    protected SpillSpaceManager() {
        this.isSpillManager = true;
    }

    @Override
    public long getCurrentMaxSpillBytes() {
        return FileConfig.getInstance().getSpillConfig().getMaxSpillSpaceThreshold().toBytes();
    }

    @Override
    public String tag() {
        return SpillSpaceManager.class.getSimpleName();
    }
}

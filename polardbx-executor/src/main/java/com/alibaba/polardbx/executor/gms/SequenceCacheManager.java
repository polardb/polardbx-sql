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

package com.alibaba.polardbx.executor.gms;

import com.alibaba.polardbx.optimizer.sequence.SequenceManagerProxy;

public class SequenceCacheManager {

    public static void reload(String schemaName, String seqName) {
        SequenceManagerProxy.getInstance().invalidate(schemaName, seqName);
        SequenceManagerProxy.getInstance().checkIfExists(schemaName, seqName);
    }

    public static void invalidate(String schemaName, String seqName) {
        SequenceManagerProxy.getInstance().invalidate(schemaName, seqName);
    }

}

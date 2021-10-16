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

package com.alibaba.polardbx.common.cdc;

import java.util.Set;

public interface ICdcManager {

    String TABLE_NEW_NAME = "CDC.TABLE_NEW_NAME";

    String TABLE_NEW_PATTERN = "CDC.TABLE_NEW_PATTERN";

    String ALTER_TRIGGER_TOPOLOGY_CHANGE_FLAG = "ALTER_TRIGGER_TOPOLOGY_CHANGE_FLAG";

    String NOT_IGNORE_GSI_JOB_TYPE_FLAG = "NOT_IGNORE_GSI_JOB_TYPE_FLAG";

    enum InstructionType {

        CdcStart,

        StorageInstChange
    }

    void sendInstruction(InstructionType instructionType, String instructionId, String instructionContent);

    void notifyDdl(CdcDDLContext cdcDDLContext);

    void checkCdcBeforeStorageRemove(Set<String> storageInstIds, String identifier);
}

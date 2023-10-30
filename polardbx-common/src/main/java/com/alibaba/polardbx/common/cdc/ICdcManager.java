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

    /**
     * 通知CdcManager，是否在打标记录中重新构建为CDC Meta模块提供的物理表建表SQL
     */
    String REFRESH_CREATE_SQL_4_PHY_TABLE = "REFRESH_CREATE_SQL_4_PHY_TABLE";

    /**
     * 通知CdcManager， 是否是标记 ORGINAL_DDL;
     */
    String USE_ORGINAL_DDL = "USE_ORGINAL_DDL";
    /**
     * 标识Foreign Keys DDL
     */
    String FOREIGN_KEYS_DDL = "FOREIGN_KEYS_DDL";

    /**
     * 是否使用OMC
     */
    String USE_OMC = "USE_OMC";

    /**
     * support multi mark in one task
     */
    String TASK_MARK_SEQ = "TASK_MARK_SEQ";

    String CDC_MARK_SQL_MODE = "cdc_mark_sql_mode";

    String CDC_ORIGINAL_DDL = "original_ddl";
    String CDC_IS_GSI = "CDC_IS_GSI";
    String CDC_GSI_PRIMARY_TABLE = "CDC_GSI_PRIMARY_TABLE";
    String CDC_GROUP_NAME = "cdc_group_name";

    /**
     * 发送Cdc通用指令
     */
    void sendInstruction(InstructionType instructionType, String instructionId, String instructionContent);

    void notifyDdl(CdcDDLContext cdcDDLContext);

    void checkCdcBeforeStorageRemove(Set<String> storageInstIds, String identifier);

    enum InstructionType {
        /**
         * Cdc初始化
         */
        CdcStart,
        /**
         * 存储实例发生了变更
         */
        StorageInstChange,
        /**
         * 元数据镜像
         */
        MetaSnapshot;
    }
}

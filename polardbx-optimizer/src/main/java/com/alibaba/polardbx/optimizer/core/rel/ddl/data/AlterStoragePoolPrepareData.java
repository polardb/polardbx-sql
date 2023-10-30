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

package com.alibaba.polardbx.optimizer.core.rel.ddl.data;

import java.util.List;

public class AlterStoragePoolPrepareData extends DdlPreparedData {

    public AlterStoragePoolPrepareData() {
    }

    public String getStoragePoolName() {
        return storagePoolName;
    }

    public void setStoragePoolName(String storagePoolName) {
        this.storagePoolName = storagePoolName;
    }

    public List<String> getDnIds() {
        return dnIds;
    }

    public void setDnIds(List<String> dnIds) {
        this.dnIds = dnIds;
    }

    public String storagePoolName;

    public List<String> dnIds;

    public String operationType;

    public Boolean getValidateStorageInstIdle() {
        return validateStorageInstIdle;
    }

    public void setValidateStorageInstIdle(Boolean validateStorageInstIdle) {
        this.validateStorageInstIdle = validateStorageInstIdle;
    }

    public Boolean validateStorageInstIdle;

    public String getUndeletableDnId() {
        return undeletableDnId;
    }

    public void setUndeletableDnId(String undeletableDnId) {
        this.undeletableDnId = undeletableDnId;
    }

    public String undeletableDnId;

    public String getOperationType() {
        return operationType;
    }

    public void setOperationType(String operationType) {
        this.operationType = operationType;
    }

}

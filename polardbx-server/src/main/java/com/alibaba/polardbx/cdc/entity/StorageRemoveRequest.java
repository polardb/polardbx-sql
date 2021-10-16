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

package com.alibaba.polardbx.cdc.entity;

import java.util.Set;

/**
 * Created by ziyang.lb
 **/
public class StorageRemoveRequest {
    private String identifier;
    private Set<String> toRemoveStorageInstIds;

    public String getIdentifier() {
        return identifier;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    public Set<String> getToRemoveStorageInstIds() {
        return toRemoveStorageInstIds;
    }

    public void setToRemoveStorageInstIds(Set<String> toRemoveStorageInstIds) {
        this.toRemoveStorageInstIds = toRemoveStorageInstIds;
    }
}

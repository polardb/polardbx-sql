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

package com.alibaba.polardbx.gms.ha.impl;

/**
 * @author chenghui.lch
 */
public enum StorageRole {
    LEADER("leader"),
    FOLLOWER("follower"),
    LEARNER("learner"),
    LOGGER("logger");

    protected String role;

    StorageRole(String role) {
        this.role = role;
    }

    public String getRole() {
        return role;
    }

    public static StorageRole getStorageRoleByString(String role) {
        StorageRole roleVal = null;
        if (StorageRole.LEADER.getRole().equalsIgnoreCase(role)) {
            roleVal = StorageRole.LEADER;
        } else if (StorageRole.FOLLOWER.getRole().equalsIgnoreCase(role)) {
            roleVal = StorageRole.FOLLOWER;
        } else if (StorageRole.LEARNER.getRole().equalsIgnoreCase(role)) {
            roleVal = StorageRole.LEARNER;
        } else {
            roleVal = StorageRole.LOGGER;
        }
        return roleVal;
    }
}

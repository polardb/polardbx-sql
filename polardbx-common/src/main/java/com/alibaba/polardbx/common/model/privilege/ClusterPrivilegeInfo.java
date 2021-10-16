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

package com.alibaba.polardbx.common.model.privilege;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ClusterPrivilegeInfo {

    String instId;

    String opVersion;

    boolean isValid;

    List<DbInfo> dbInfoList;

    List<UserInfo> userInfoList;

    List<DbUserInfo> dbUserMapList;

    List<TableUserInfo> dbTbMapList;

    public String getInstId() {
        return instId;
    }

    public void setInstId(String instId) {
        this.instId = instId;
    }

    public String getOpVersion() {
        return opVersion;
    }

    public void setOpVersion(String opVersion) {
        this.opVersion = opVersion;
    }

    public boolean isValid() {
        return isValid;
    }

    public void setValid(boolean valid) {
        isValid = valid;
    }

    public List<DbInfo> getDbInfoList() {
        return dbInfoList;
    }

    public void setDbInfoList(List<DbInfo> dbInfoList) {
        this.dbInfoList = dbInfoList;
    }

    public List<UserInfo> getUserInfoList() {
        return userInfoList;
    }

    public void setUserInfoList(List<UserInfo> userInfoList) {
        this.userInfoList = userInfoList;
    }

    public List<DbUserInfo> getDbUserMapList() {
        return dbUserMapList;
    }

    public void setDbUserMapList(List<DbUserInfo> dbUserMapList) {
        this.dbUserMapList = dbUserMapList;
    }

    public List<TableUserInfo> getDbTbMapList() {
        return dbTbMapList;
    }

    public void setDbTbMapList(List<TableUserInfo> dbTbMapList) {
        this.dbTbMapList = dbTbMapList;
    }
}

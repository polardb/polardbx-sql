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

package com.alibaba.polardbx.optimizer.utils;

import java.util.Objects;

/**
 * @author chenbhui.lch
 *
 * One GroupConnId means use a single write connection of one TGroupDatasouce in transaction
 */
public class GroupConnId {

    protected String group;
    protected Long connId;

    public GroupConnId(String grp, Long connId) {
        this.group = grp;
        this.connId = connId;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public Long getConnId() {
        return connId;
    }

    public void setConnId(Long connId) {
        this.connId = connId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GroupConnId that = (GroupConnId) o;
        return group.equalsIgnoreCase(that.group) && connId.equals(that.connId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(group.toUpperCase(), connId);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("");
        sb.append(group).append("@").append(connId);
        return sb.toString();
    }
}

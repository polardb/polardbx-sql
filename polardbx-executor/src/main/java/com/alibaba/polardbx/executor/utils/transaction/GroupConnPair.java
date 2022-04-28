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

package com.alibaba.polardbx.executor.utils.transaction;

import java.util.Objects;

/**
 * @author dylan
 * <p>
 * wuzhe move this class from InformationSchemaInnodbTrxHandler to here
 * <p>
 * A (group, DN connection id) pair, which can represent a unique transaction
 */
public class GroupConnPair {
    final private String group;
    final private long connId;

    public GroupConnPair(String group, long connId) {
        this.group = group;
        this.connId = connId;
    }

    public String getGroup() {
        return group;
    }

    public long getConnId() {
        return connId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final GroupConnPair that = (GroupConnPair) o;
        return connId == that.connId &&
            group.equals(that.group);
    }

    @Override
    public int hashCode() {
        return Objects.hash(group, connId);
    }
}

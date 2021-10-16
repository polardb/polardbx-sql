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

package com.alibaba.polardbx.gms.locality;

import java.util.Objects;

/**
 * @author moyi
 */
public class LocalityId {
    public int objectType;
    public long objectId;

    public LocalityId() {
    }

    public LocalityId(long objectId, int objectType) {
        this.objectId = objectId;
        this.objectType = objectType;
    }

    public static LocalityId from(int objectType, long objectId) {
        LocalityId result = new LocalityId();
        result.objectType = objectType;
        result.objectId = objectId;
        return result;
    }

    public static LocalityId ofDatabase(long dbId) {
        return from(LocalityInfoRecord.LOCALITY_TYPE_DATABASE, dbId);
    }

    public static LocalityId ofTable(long tableId) {
        return from(LocalityInfoRecord.LOCALITY_TYPE_TABLE, tableId);
    }

    public static LocalityId ofTableGroup(long tgId) {
        return from(LocalityInfoRecord.LOCALITY_TYPE_TABLEGROUP, tgId);
    }

    public static LocalityId defaultId() {
        return from(LocalityInfoRecord.LOCALITY_TYPE_DEFAULT, LocalityInfoRecord.LOCALITY_ID_DEFAULT);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LocalityId that = (LocalityId) o;
        return objectType == that.objectType && objectId == that.objectId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(objectType, objectId);
    }

    @Override
    public String toString() {
        return "{" + objectType + "," + objectId + "}";
    }
}

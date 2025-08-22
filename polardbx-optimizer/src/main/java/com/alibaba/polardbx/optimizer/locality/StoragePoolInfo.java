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

package com.alibaba.polardbx.optimizer.locality;

import com.alibaba.polardbx.gms.locality.LocalityDesc;
import com.alibaba.polardbx.gms.locality.StoragePoolInfoRecord;

import java.util.List;
import java.util.Objects;

public class StoragePoolInfo {
    static String DN_PREFIX = "dn=";
    private Long id;
    private String dnIds;

    private String name;

    private String undeletableDnId;

    private String extras;

    public StoragePoolInfo(Long id, String name, String dnIds, String undeletableDnId, String extras) {
        this.id = id;
        this.name = name;
        this.dnIds = dnIds;
        this.undeletableDnId = undeletableDnId;
        this.extras = extras;
    }

    public StoragePoolInfo() {
        this.id = -1L;
        this.name = "";
        this.dnIds = "";
        this.extras = "";
        this.undeletableDnId = "";
    }

    public StoragePoolInfo(StoragePoolInfo rhs) {
        this.id = rhs.id;
        this.name = rhs.name;
        this.dnIds = rhs.dnIds;
        this.undeletableDnId = rhs.undeletableDnId;
        this.extras = rhs.extras;
    }

    public boolean contains(List<String> dstDnList) {
        List<String> srcDnList = LocalityDesc.parse(DN_PREFIX + dnIds).getDnList();
        return srcDnList.containsAll(dstDnList);
    }

    public List<String> getDnLists() {
        return LocalityDesc.parse(DN_PREFIX + dnIds).getDnList();
    }

    public static StoragePoolInfo from(StoragePoolInfoRecord record) {
        StoragePoolInfo result = new StoragePoolInfo();
        result.id = record.id;
        result.name = record.name.toLowerCase();
        result.dnIds = record.dnIds.toLowerCase();
        result.extras = record.extras;
        result.undeletableDnId = record.undeletableDnId.toLowerCase();
        return result;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getUndeletableDnId() {
        return undeletableDnId;
    }

    public void setUndeletableDnId(String undeletableDnId) {
        this.undeletableDnId = undeletableDnId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StoragePoolInfo that = (StoragePoolInfo) o;
        return Objects.equals(id, that.id) && Objects.equals(name, that.name) && Objects.equals(dnIds, that.dnIds)
            && Objects.equals(extras, that.extras) && Objects.equals(undeletableDnId, that.undeletableDnId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, dnIds, undeletableDnId, extras);
    }

    @Override
    public StoragePoolInfo clone() {
        return new StoragePoolInfo(this.id, this.name, this.dnIds, this.undeletableDnId, this.extras);
    }

    @Override
    public String toString() {
        return "StoragePoolInfo{" +
            "id=" + id +
            ", name=" + name +
            ", dnIds='" + dnIds + '\'' +
            ", undeletableDnId='" + undeletableDnId + '\'' +
            '}';
    }

    public void setDnIds(String dnIds) {
        this.dnIds = dnIds;
    }

    public String getDnIds() {
        return dnIds;
    }
}

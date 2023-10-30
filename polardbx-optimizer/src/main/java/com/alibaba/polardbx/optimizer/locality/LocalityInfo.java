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
import com.alibaba.polardbx.gms.locality.LocalityId;
import com.alibaba.polardbx.gms.locality.LocalityInfoRecord;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class LocalityInfo {
    private LocalityId id;
    private String locality;

    public LocalityInfo(LocalityId id, String locality) {
        this.id = id;
        this.locality = locality;
    }

    public LocalityInfo() {
        this.locality = "";
    }

    public LocalityInfo(LocalityInfo rhs) {
        this.id = rhs.id;
        this.locality = rhs.locality;
    }

    public boolean contains(List<String> dstDnList) {
        List<String> srcDnList = LocalityDesc.parse(locality).getDnList();
        return srcDnList.containsAll(dstDnList);
    }

    public static LocalityInfo from(LocalityInfoRecord record) {
        LocalityInfo result = new LocalityInfo();
        LocalityId id = new LocalityId(record.objectId, record.objectType);
        result.id = id;
        result.locality = record.locality;
        return result;
    }

    public static void inherit(LocalityInfo base, List<String> localities) {
        String newLocality = base.getLocality() + " " + StringUtils.join(localities, " ");
        base.setLocality(LocalityDesc.parse(newLocality).toString());
    }

    private static List<String> intersect(List<String> list1, List<String> list2) {
        if (list1.isEmpty() || list2.isEmpty()) {
            list1.addAll(list2);
            return list1;
        } else {
            list1.retainAll(list2);
            return list1;
        }
    }

    public static LocalityDesc intersect(List<LocalityInfo> localityInfos) {
        // universe set INTERSECT non-empty set = universe set
        List<String> intersectionDnlist = new ArrayList<>();
        for (LocalityInfo localityInfo : localityInfos) {
            List<String> dnList;
            if (localityInfo == null) {
                dnList = new ArrayList<>();
            } else {
                dnList = LocalityDesc.parse(localityInfo.locality).getDnList();
            }
            intersectionDnlist = intersect(intersectionDnlist, dnList);
        }
        String locality = "dn=" + String.join(",", intersectionDnlist);
        return LocalityDesc.parse(locality);
    }

    /**
     * Inherit hierarchical locality: default/database/table-group/partition-group
     */
    public static LocalityInfo inherit(List<LocalityInfo> localities) {
        throw new RuntimeException("TODO");
    }

    public LocalityId getId() {
        return id;
    }

    public void setId(LocalityId id) {
        this.id = id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LocalityInfo that = (LocalityInfo) o;
        return Objects.equals(id, that.id) && Objects.equals(locality, that.locality);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, locality);
    }

    @Override
    public LocalityInfo clone() {
        return new LocalityInfo(this.id, this.locality);
    }

    @Override
    public String toString() {
        return "LocalityInfo{" +
            "id=" + id +
            ", locality='" + locality + '\'' +
            '}';
    }

    public String getLocality() {
        return locality;
    }

    public void setLocality(String locality) {
        this.locality = locality;
    }
}

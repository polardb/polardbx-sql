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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.TStringUtil;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * PrimaryZone of the system.
 * It could be set by `ALTER SYSTEM SET CONFIG primary_zone='az1,az2;az3'`.
 * 'az1,az2;az3' means az1 and az2 has the same weight, which is larger that az3
 *
 * @author moyi
 * @since 2021/03
 */
public class PrimaryZoneInfo {
    public static final int HIGH_WEIGHT = 10;
    public static final int PRIMARY_ZONE_WEIGHT = 6;
    public static final int DEFAULT_ZONE_WEIGHT = 5;

    private List<ZoneProperty> zoneList;

    public PrimaryZoneInfo() {
        this.zoneList = new ArrayList<>();
    }

    public static PrimaryZoneInfo build(List<String> zones) {
        PrimaryZoneInfo res = new PrimaryZoneInfo();
        for (String zone : zones) {
            String[] ss = zone.split(":");
            String name = ss[0];
            int weight = Integer.parseInt(ss[1]);
            res.zoneList.add(new ZoneProperty(name, weight));
        }
        return res;
    }

    public static PrimaryZoneInfo parse(String str) {
        PrimaryZoneInfo result = new PrimaryZoneInfo();
        str = str.trim();
        if (TStringUtil.isBlank(str)) {
            return result;
        }

        String[] ss = str.split(";");
        if (ss.length == 0) {
            throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS, "invalid primary_zone: " + str);
        }

        Set<String> existedZones = new HashSet<>();
        int weight = HIGH_WEIGHT;
        for (String s : ss) {
            s = s.trim();
            if (TStringUtil.isBlank(s)) {
                throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS, "invalid primary_zone: " + s);
            }
            String[] zones = s.split(",");
            if (zones.length == 0) {
                throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS, "invalid primary_zone: " + s);
            }
            for (String zone : zones) {
                if (existedZones.contains(zone)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS, "duplicate zone: " + zone);
                }
                existedZones.add(zone);
                ZoneProperty z = new ZoneProperty(zone, weight);
                result.zoneList.add(z);
            }
            weight--;
        }

        // make sure the lowest weight is `LOW_WEIGHT`
        int diff = weight + 1 - DEFAULT_ZONE_WEIGHT;
        for (ZoneProperty zone : result.zoneList) {
            zone.weight -= diff;
        }
        if (result.zoneList.size() == 1) {
            result.zoneList.get(0).weight = PRIMARY_ZONE_WEIGHT;
        }
        return result;
    }

    public List<ZoneProperty> getZoneList() {
        return this.zoneList;
    }

    public boolean isEmpty() {
        return this.zoneList.isEmpty();
    }

    /**
     * 1. "az1"
     * 2. "az1;az2,az3"
     */
    public boolean hasLeader() {
        return this.zoneList.size() == 1 ||
            (this.zoneList.size() >= 2 && this.zoneList.get(0).weight > this.zoneList.get(1).weight);
    }

    public boolean hasSinglePrimaryZone() {
        return this.zoneList.size() == 1;
    }

    public String getFirstZone() {
        return this.zoneList.get(0).zone;
    }

    public ZoneProperty getZone(String zone) {
        for (ZoneProperty zp : this.getZoneList()) {
            if (zone.equals(zp.zone)) {
                return zp;
            }
        }
        return null;
    }

    @Override
    public String toString() {
        return "PrimaryZoneInfo{" +
            "zoneList=" + zoneList +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PrimaryZoneInfo)) {
            return false;
        }
        PrimaryZoneInfo that = (PrimaryZoneInfo) o;
        return Objects.equals(zoneList, that.zoneList);
    }

    @Override
    public int hashCode() {
        return Objects.hash(zoneList);
    }

    /**
     * Serialize the primary_zone into string representation: "az1,az2;az3;az4"
     */
    public String serialize() {
        if (this.isEmpty()) {
            return "";
        }
        if (this.hasSinglePrimaryZone()) {
            return this.zoneList.get(0).zone;
        }

        StringBuilder sb = new StringBuilder();
        int prevWeight = this.zoneList.get(0).weight;
        for (ZoneProperty zone : this.zoneList) {
            if (sb.length() == 0) {
                // first zone, just skip separator
            } else if (prevWeight != zone.weight) {
                sb.append(";");
            } else {
                sb.append(",");
            }
            sb.append(zone.zone);
            prevWeight = zone.weight;
        }
        return sb.toString();
    }

    public static class ZoneProperty {
        public String zone;
        public int weight;

        public ZoneProperty(String zone, int weight) {
            this.zone = zone;
            this.weight = weight;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ZoneProperty)) {
                return false;
            }
            ZoneProperty that = (ZoneProperty) o;
            return weight == that.weight && Objects.equals(zone, that.zone);
        }

        @Override
        public int hashCode() {
            return Objects.hash(zone, weight);
        }

        @Override
        public String toString() {
            return zone + ":" + weight;
        }
    }

}

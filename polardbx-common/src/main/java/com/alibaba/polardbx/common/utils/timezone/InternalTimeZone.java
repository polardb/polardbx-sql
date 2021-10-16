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

package com.alibaba.polardbx.common.utils.timezone;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang.StringUtils;

import java.time.ZoneId;
import java.util.Objects;
import java.util.Optional;
import java.util.TimeZone;

public class InternalTimeZone {

    public static final ZoneId DEFAULT_ZONE_ID = ZoneId.systemDefault();
    public static final ZoneId UTC_ZONE_ID = ZoneId.of("Etc/UTC");
    public static final TimeZone UTC_TIME_ZONE = TimeZone.getTimeZone("Etc/UTC");
    public static final TimeZone DEFAULT_TIME_ZONE = TimeZone.getDefault();

    private TimeZone timeZone;
    private String mySqlTimeZoneName;
    private ZoneId zoneId;
    public static InternalTimeZone defaultTimeZone = createTimeZone("SYSTEM", TimeZone.getDefault());

    public InternalTimeZone(TimeZone timeZone, String mySqlTimeZoneName) {
        this.timeZone = timeZone;
        this.mySqlTimeZoneName = mySqlTimeZoneName;
    }

    @JsonCreator
    public InternalTimeZone(@JsonProperty("id") String id,
                            @JsonProperty("mySqlTimeZoneName") String mySqlTimeZoneName) {
        if (id != null && !StringUtils.isEmpty(id)) {
            this.timeZone = TimeZone.getTimeZone(id);
        }

        this.mySqlTimeZoneName = mySqlTimeZoneName;
    }

    public TimeZone getTimeZone() {
        return timeZone;
    }

    @JsonProperty("mySqlTimeZoneName")
    public String getMySqlTimeZoneName() {
        return mySqlTimeZoneName;
    }

    @JsonProperty("id")
    public String getId() {
        if (timeZone != null) {
            return timeZone.getID();
        } else {
            return "";
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof InternalTimeZone)) {
            return false;
        }
        InternalTimeZone timeZone1 = (InternalTimeZone) o;
        return timeZone.equals(timeZone1.timeZone) &&
            mySqlTimeZoneName.equals(timeZone1.mySqlTimeZoneName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timeZone, mySqlTimeZoneName);
    }

    public static InternalTimeZone createTimeZone(String mySqlTimeZoneName, TimeZone timeZone) {
        return new InternalTimeZone(timeZone, mySqlTimeZoneName);
    }

    public ZoneId getZoneId() {
        if (zoneId == null) {
            zoneId = Optional.ofNullable(getId())
                .map(TimeZoneUtils::zoneIdOf)
                .orElseGet(
                    () -> ZoneId.systemDefault()
                );
        }
        return zoneId;
    }

}

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

package com.alibaba.polardbx.gms.scheduler;

import com.alibaba.polardbx.common.utils.timezone.TimeZoneUtils;
import org.apache.commons.lang.StringUtils;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public interface ScheduleDateTimeConverter {

    static ZonedDateTime secondToZonedDateTime(Long seconds, String timeZone){
        return secondToZonedDateTime(seconds, TimeZoneUtils.zoneIdOf(timeZone));
    }

    static ZonedDateTime secondToZonedDateTime(Long seconds, ZoneId zoneId){
        if(seconds == null){
            return null;
        }
        return ZonedDateTime.ofInstant(Instant.ofEpochSecond(seconds), zoneId);
    }

}
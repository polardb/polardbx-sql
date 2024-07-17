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

import com.alibaba.polardbx.common.utils.timezone.InternalTimeZone;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;

import java.time.ZoneId;
import java.util.Optional;
import java.util.TimeZone;

public class TimestampUtils {
    public static ZoneId getZoneId(ExecutionContext context) {
        return Optional.ofNullable(context)
            .map(ExecutionContext::getTimeZone)
            .map(InternalTimeZone::getZoneId)
            .orElse(InternalTimeZone.DEFAULT_ZONE_ID);
    }

    public static TimeZone getTimeZone(ExecutionContext context) {
        return Optional.ofNullable(context)
            .map(ExecutionContext::getTimeZone)
            .map(InternalTimeZone::getTimeZone)
            .orElseGet(TimeZone::getDefault);
    }
}

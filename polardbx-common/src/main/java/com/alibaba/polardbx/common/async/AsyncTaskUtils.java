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

package com.alibaba.polardbx.common.async;

import java.text.ParseException;
import java.time.LocalTime;

public abstract class AsyncTaskUtils {

    public static TimeInterval parseTimeInterval(String str) throws ParseException {
        String[] splits = str.split("-");
        if (splits.length != 2) {
            throw new ParseException("Bad Input: " + str, 0);
        }
        LocalTime start = LocalTime.parse(splits[0].trim());
        LocalTime end = LocalTime.parse(splits[1].trim());
        return new TimeInterval(start, end);
    }

}

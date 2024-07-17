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

package com.alibaba.polardbx.executor.operator.scan.metrics;

import org.apache.orc.impl.StreamName;

public class MetricsNameBuilder {
    public static String streamMetricsKey(StreamName streamName, ProfileKey profileKey) {
        return String.join(".",
            profileKey.getName(),
            streamName.toString().replace(' ', '.'));
    }

    public static String columnsMetricsKey(boolean[] selectedColumns, ProfileKey profileKey) {
        return String.join(".",
            profileKey.getName(),
            "columns",
            bitmapSuffix(selectedColumns));
    }

    public static String columnMetricsKey(int targetColumnId, ProfileKey profileKey) {
        return String.join(".",
            profileKey.getName(),
            "column",
            String.valueOf(targetColumnId));
    }

    public static String bitmapSuffix(boolean[] bitmap) {
        StringBuilder builder = new StringBuilder();
        builder.append('[');
        for (int i = 0; i < bitmap.length; i++) {
            if (!bitmap[i]) {
                continue;
            }
            builder.append(i);
            if (i != bitmap.length - 1) {
                builder.append(',');
            }
        }
        builder.append(']');
        return builder.toString();
    }
}

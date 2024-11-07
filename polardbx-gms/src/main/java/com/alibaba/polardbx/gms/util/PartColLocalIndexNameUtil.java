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

package com.alibaba.polardbx.gms.util;

import java.util.Formatter;
import java.util.Random;
import java.util.Set;

public class PartColLocalIndexNameUtil {

    /**
     * Build a random local index name
     */
    public static String buildRandomName(Set<String> existsNames,
                                         String preferName) {
        // Assign new name with suffix.
        final Random random = new Random();
        String fullName;
        do {
            Formatter formatter = new Formatter();
            final String suffix = "$" + formatter.format("%04x", random.nextInt(0x10000));
            fullName = preferName + suffix;
        } while (existsNames.contains(fullName));
        return fullName;
    }
}

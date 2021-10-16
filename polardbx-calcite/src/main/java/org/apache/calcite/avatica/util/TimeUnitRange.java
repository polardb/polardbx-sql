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

package org.apache.calcite.avatica.util;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public enum TimeUnitRange {
                           YEAR(TimeUnit.YEAR, (TimeUnit) null), YEAR_MONTH(TimeUnit.YEAR, TimeUnit.MONTH),
                           MONTH(TimeUnit.MONTH, (TimeUnit) null), DAY(TimeUnit.DAY, (TimeUnit) null),
                           DAY_HOUR(TimeUnit.DAY, TimeUnit.HOUR), DAY_MINUTE(TimeUnit.DAY, TimeUnit.MINUTE),
                           DAY_SECOND(TimeUnit.DAY, TimeUnit.SECOND),
                           DAY_MICROSECOND(TimeUnit.DAY, TimeUnit.MICROSECOND), HOUR(TimeUnit.HOUR, (TimeUnit) null),
                           HOUR_MINUTE(TimeUnit.HOUR, TimeUnit.MINUTE), HOUR_SECOND(TimeUnit.HOUR, TimeUnit.SECOND),
                           HOUR_MICROSECOND(TimeUnit.HOUR, TimeUnit.MICROSECOND),
                           MINUTE(TimeUnit.MINUTE, (TimeUnit) null), MINUTE_SECOND(TimeUnit.MINUTE, TimeUnit.SECOND),
                           MINUTE_MICROSECOND(TimeUnit.MINUTE, TimeUnit.MICROSECOND),
                           SECOND(TimeUnit.SECOND, (TimeUnit) null),
                           SECOND_MICROSECOND(TimeUnit.SECOND, TimeUnit.MICROSECOND),
                           QUARTER(TimeUnit.QUARTER, (TimeUnit) null), WEEK(TimeUnit.WEEK, (TimeUnit) null),
                           MILLISECOND(TimeUnit.MILLISECOND, (TimeUnit) null),
                           MICROSECOND(TimeUnit.MICROSECOND, (TimeUnit) null), DOW(TimeUnit.DOW, (TimeUnit) null),
                           DOY(TimeUnit.DOY, (TimeUnit) null), EPOCH(TimeUnit.EPOCH, (TimeUnit) null),
                           DECADE(TimeUnit.DECADE, (TimeUnit) null), CENTURY(TimeUnit.CENTURY, (TimeUnit) null),
                           MILLENNIUM(TimeUnit.MILLENNIUM, (TimeUnit) null);

    public final TimeUnit                                                 startUnit;
    public final TimeUnit                                                 endUnit;
    private static final Map<TimeUnitRange.Pair<TimeUnit>, TimeUnitRange> MAP = createMap();

    private TimeUnitRange(TimeUnit startUnit, TimeUnit endUnit){
        assert startUnit != null;

        this.startUnit = startUnit;
        this.endUnit = endUnit;
    }

    public static TimeUnitRange of(TimeUnit startUnit, TimeUnit endUnit) {
        return (TimeUnitRange) MAP.get(new TimeUnitRange.Pair(startUnit, endUnit));
    }

    private static Map<TimeUnitRange.Pair<TimeUnit>, TimeUnitRange> createMap() {
        HashMap map = new HashMap();
        TimeUnitRange[] var1 = values();
        int var2 = var1.length;

        for (int var3 = 0; var3 < var2; ++var3) {
            TimeUnitRange value = var1[var3];
            map.put(new TimeUnitRange.Pair(value.startUnit, value.endUnit), value);
        }

        return Collections.unmodifiableMap(map);
    }

    public boolean monthly() {
        return this.ordinal() <= MONTH.ordinal();
    }

    private static class Pair<E> {

        final E left;
        final E right;

        private Pair(E left, E right){
            this.left = left;
            this.right = right;
        }

        public int hashCode() {
            int k = this.left == null ? 0 : this.left.hashCode();
            int k1 = this.right == null ? 0 : this.right.hashCode();
            return (k << 4 | k) ^ k1;
        }

        public boolean equals(Object obj) {
            return obj == this
                   || obj instanceof TimeUnitRange.Pair && Objects.equals(this.left, ((TimeUnitRange.Pair) obj).left)
                      && Objects.equals(this.right, ((TimeUnitRange.Pair) obj).right);
        }
    }
}

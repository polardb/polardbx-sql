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

package org.apache.calcite.util;

import java.util.Objects;

/**
 * @author lingce.ldm 2018-03-22 13:48
 */
public class IntervalString implements Comparable<IntervalString>, Cloneable {
    private final int sign;
    private final String intervalStr;

    public IntervalString(
        int sign,
        String intervalStr) {
        this.sign = sign;
        this.intervalStr = intervalStr;
    }

    @Override
    public Object clone() {
        try {
            return super.clone();
        } catch(CloneNotSupportedException e) {
            throw new AssertionError();
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(sign, intervalStr);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof IntervalString))  {
            return false;
        }
        IntervalString that = (IntervalString) obj;
        return Objects.equals(sign, that.sign)
            && Objects.equals(intervalStr, that.intervalStr);
    }


    @Override public int compareTo(IntervalString o) {
        return intervalStr.compareTo(o.intervalStr);
    }

    public int getSign() {
        return sign;
    }

    public String getIntervalStr() {
        return intervalStr;
    }

    @Override
    public String toString() {
        String signStr = sign == -1 ? "-1" : "";
        return signStr + intervalStr;
    }
}

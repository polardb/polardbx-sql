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

package com.alibaba.polardbx.rule.enumerator.utils;

import java.util.Date;

/**
 * @date 2019/4/3 上午11:01
 *
 *  对于跨月的时间点，由于当前月份的最后1个时间枚举点与下个月的第1个时间枚举点是不是平滑函数的，
 *  导致枚举值会产生不全，以及出现分片的缺失。
 *  为了弥补这个问题，对于所有在
 *      当月月末的最后1个时间枚举点 与 下个月的第1个枚举时间点之间
 *  之间产生的缺失分片的下标，都为其生成特殊的CrossMonthDate时间枚举点
 *  点，这些CrossMonthDate时间枚举点会保证路由到那些所缺失分片。
 *
 *  CrossMonthDate在当前的Date的hash值的基础上，通过增加偏移值，使用路由所缺失的物理分片
 *
 *  <pre>
 *
 *      例如，假设按dd_i(gmt_create)分 31 个表, DDL生成的规则是  dd_i(#gmt_create,1_date,31#)
 *      ，即范围查询时，最多枚举31次，即可得到全部分表（但实际上并非如此）：
 *
 *      如果用户是这样的跨月的范围查询
 *          gmt_create >= '2018-02-01' and gmt_create < '2018-05-31'
 *
 *      第1阶段枚举：DRDS 从 02-01 ~ 02-28，共枚举了 28次，枚举结果分别对应下标为 0 ~ 28 这28个分表；
 *      第2阶段枚举：DRDS 再继 从 03-01 ~ 03-03， 共枚举了 3次，枚举结果分别对应下标为 0 ~ 3 这3个分表。
 *      显然，第2阶段的枚举，由于时间跨月，导致其枚举的结果与第1阶段的产生重复，导致这31次的枚举无法遍历
 *      所有的物理分表，导致产生分片缺失。
 *
 *      出现这种现象的原因：
 *          不同月份的天数是不一样，所以跨月枚举时产生的枚举值不是一个线性函数，导致重复。
 *          类似地，对于 不同年份的周数、不同年份的总天数，如果出现跨年枚举，就会导致跨年枚举产生的枚举值
 *          也不是一个线性函数，导致分片缺失。
 *
 *      解决的办法：
 *          对于每一个跨月的时间点（定义为一个月的最后一天），当其进行枚举时，DRDS会对这一天及它的下一天之间，
 *          补上特殊的日期，CrossMonthDate，来填充这中间缺失的分表。
 *         例如，对于第1阶段枚举，DRDS针对 02-28日，再生成 3个特殊的时间点（因为这些时间在真实日历中是不存在的，
 *         所以，只能以 date + offset 的方式定义）：
 *          CrossMonthDate（02-28 + 1）， CrossMonthDate（02-28 + 2），CrossMonthDate（02-28 + 3）
 *          这样，DRDS对于CrossMonthDate计算路由结果时，就会通过增加偏移，得到 第 29 ~ 31 号 这3个表，从而
 *          补上缺失的分片。
 *
 *
 *
 *  <pre/>
 */
public class CrossMonthDate extends Date {

    /**
     * 当前时点点的哈希值的偏移值
     */
    protected long offset = 0;

    public CrossMonthDate() {
        super();
    }

    public CrossMonthDate(long date) {
        super(date);
    }

    @Override
    public boolean equals(Object obj) {

        if (obj == this) {
            return true;
        }

        if (obj instanceof CrossMonthDate) {
            CrossMonthDate crossMonthDate = (CrossMonthDate) obj;

            if (crossMonthDate.getTime() == this.getTime()) {
                if (crossMonthDate.getOffset() == this.offset) {
                    return true;
                } else {
                    return false;
                }
            } else {
                return false;
            }

        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        int hashVal = super.hashCode();
        int off = (int) offset << 31;
        hashVal = hashVal ^ off;
        return hashVal;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

}

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
 *  对于跨年的时间点，由于当前年份的最后1个时间枚举点与下一年的第1个时间枚举点是不是平滑函数的，
 *  导致枚举值会产生不全，以及出现分片的缺失。
 *  为了弥补这个问题，对于所有在
 *      当年年末的最后1个时间枚举点 与 下一年的第1个枚举时间点之间
 *  之间产生的缺失分片的下标，都为其生成特殊的CrossYearDate时间枚举点
 *  点，这些CrossYearDate时间枚举点会保证路由到那些所缺失分片。
 *
 *  CrossYearDate在当前的Date的hash值的基础上，通过增加偏移值，使用路由所缺失的物理分片
 */
public class CrossYearDate extends Date {

    /**
     * 当前时点点的哈希值的偏移值
     */
    protected long offset = 0;

    public CrossYearDate() {
        super();
    }

    public CrossYearDate(long date) {
        super(date);
    }

    @Override
    public boolean equals(Object obj) {

        if (obj == this) {
            return true;
        }

        if (obj instanceof CrossYearDate) {
            CrossYearDate crossYearDate = (CrossYearDate) obj;

            if (crossYearDate.getTime() == this.getTime()) {
                if (crossYearDate.getOffset() == this.offset) {
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

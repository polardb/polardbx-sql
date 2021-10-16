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

package com.alibaba.polardbx.rule.virtualnode;

/**
 * 虚拟节点的节点区间标识
 * <p>
 * <pre>
 * count 表示定义的分区数
 * length 表示对应每个分区的取值长度
 * 注意：其中count,length两个数组的长度必须是一致的。
 * 约束：1024 = sum((count[i]*length[i])). count和length两个向量的点积恒等于1024
 * </pre>
 * <p>
 * <pre>
 * 分区策略例子：希望将数据水平分成3份，前两份各占25%，第三份占50%。（故本例非均匀分区）
 *         // |<---------------------1024------------------------>|
 *         // |<----256--->|<----256--->|<----------512---------->|
 *         // | partition0 | partition1 | partition2 |
 *         // | 共2份,故count[0]=2 | 共1份，故count[1]=1 |
 *         int[] count = new int[] { 2, 1 };
 *         int[] length = new int[] { 256, 512 };
 * </pre>
 *
 * @author jianghang 2013-11-4 上午11:43:07
 * @since 5.0.0
 */
public class PartitionFunction {

    private int[] count;
    private int[] length;

    private int firstValue = -1;

    public void setFirstValue(int firstValue) {
        this.firstValue = firstValue;
    }

    public void setPartitionCount(String partitionCount) {
        this.count = this.toIntArray(partitionCount);
    }

    public void setPartitionLength(String partitionLength) {
        this.length = this.toIntArray(partitionLength);
    }

    public int[] getCount() {
        return count;
    }

    public int[] getLength() {
        return length;
    }

    public int getFirstValue() {
        return firstValue;
    }

    private int[] toIntArray(String string) {
        if (string == null || string.isEmpty()) {
            return null;
        }

        String[] strs = string.split(",");
        int[] ints = new int[strs.length];
        for (int i = 0; i < strs.length; ++i) {
            ints[i] = Integer.parseInt(strs[i]);
        }
        return ints;
    }
}

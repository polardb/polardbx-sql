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

package com.alibaba.polardbx.group.config;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * <p>
 * 数据库权重配置，权重越大，被选中的机率越大.
 * <p>
 * 权重配置模式:
 * <p>
 * [r|R](\\d*) [w|W](\\d*) [p|P](\\d*) [q|Q](\\d*) [i|I](\\d*)
 * <p>
 * 字母r或R表示可以对数据库进行读操作, 后面跟一个数字表示读操作的权重，如果字母r或R后面没有数字，则默认是10;
 * <p>
 * 字母w或W表示可以对数据库进行写操作, 后面跟一个数字表示写操作的权重，如果字母w或W后面没有数字，则默认是10;<
 * <p>
 * 字母p或P表示读操作的优先级, 数字越大优先级越高，读操作优先从优先级最高的数据库中读数据， 如果字母p或P后面没有数字，则默认优先级是0;
 * <p>
 * 字母q或Q表示写操作的优先级, 数字越大优先级越高，写操作优先从优先级最高的数据库中写数据， 如果字母q或Q后面没有数字，则默认优先级是0.
 * <p>
 * 字母i或I表示动态DBIndex, 和用户通过threadLocal指定的dbIndex结合，实现rw之上更灵活的路由
 * 一个db可以同时配置多个i；不同的db可以配置相同的i，例如 db0:i0i2,db1:i1,db2:i1,db3:i2则
 * 用户指定dbIndex=0，路由到db0；（只有db0有i0） 用户指定dbIndex=1，随机路由到db1和db2；（db1和db2都有i1）
 * 用户指定dbIndex=2，随机路由到db0和db3；（db0和db3都有i2）
 * <p>
 * 如：db1: r10w10p2, db2: r20p2, db3: rp3，则对应如下三个Weight: db1: Weight(r10w10p2)
 * db2: Weight(r20p2) db3: Weight(rp3)
 * <p>
 * 在这个例子中，对db1, db2，db3这三个数据库的读操作分成了两个优先级: p3->[db3] p2->[db1, db2]
 * 当进行读操作时，因为db3的优先级最高，所以优先从db3读， 如果db3无法进行读操作，再从db1,
 * db2中随机选一个，因为db2的读权重是20，而db1是10，所以db2被选中的机率比db1更大。
 * <p>
 * 如果在数据库名后面没有设置权重字符串，就认为权重字符串是null, 如: db1: r10w10, db2, db3，则对应如下三个Weight:
 * db1: Weight(r10w10) db2: Weight(null) db3: Weight(null)
 * <p>
 * <b>为了兼容2.4之前的老版本，当权重字符串是null时，相当于"r10w10p0q0", 对于上面的例子，实际的数据库权重配置是：db1:
 * r10w10p0q0, db2: r10w10p0q0, db3: r10w10p0q0。<b>
 *
 * @author yangzhu
 * @author linxuan add indexes i/I at 2011/01/21
 */
public class Weight {

    private static final Pattern weightPattern_r = Pattern.compile("[R](\\d*)");
    private static final Pattern weightPattern_w = Pattern.compile("[W](\\d*)");
    private static final Pattern weightPattern_p = Pattern.compile("[P](\\d*)");
    private static final Pattern weightPattern_q = Pattern.compile("[Q](\\d*)");
    private static final Pattern weightPattern_i = Pattern.compile("[I](\\d*)");
    private static final Pattern weightPattern_a = Pattern.compile("[A](\\d*)");

    /**
     * 读权重，默认是10
     */
    public final int r;

    /**
     * 写权重，默认是10
     */
    public final int w;

    /**
     * 读优先级，默认是0
     */
    public final int p;

    /**
     * 写优先级，默认是0
     */
    public final int q;

    public final Set<Integer> indexes;

    /**
     * 标记数据源是否为AP专用，0表示TP专用，1表示AP专用，默认是0
     */
    public final int a;

    public final String weightStr;

    public Weight(String weightStr) {
        // 兼容2.4之前的老版本，当权重字符串是null时，相当于"r10w10p0q0",
        this.weightStr = weightStr;
        if (weightStr == null) {
            r = 10;
            w = 10;
            p = 0;
            q = 0;
            indexes = null;
            a = 0;
        } else {
            weightStr = weightStr.trim().toUpperCase();
            // 如果字母'R'在weightStr中找不到，则读权重是0，
            // 如果字母'R'在weightStr中已找到了，但是在字母'R'后面没有数字，是读权重是10
            r = getUnitWeight(weightStr, 'R', weightPattern_r, 0, 10);
            w = getUnitWeight(weightStr, 'W', weightPattern_w, 0, 10);
            p = getUnitWeight(weightStr, 'P', weightPattern_p, 0, 0);
            q = getUnitWeight(weightStr, 'Q', weightPattern_q, 0, 0);
            indexes = getUnitWeights(weightStr, 'I', weightPattern_i);
            a = getUnitWeight(weightStr, 'A', weightPattern_a, 0, 0);
        }
    }

    public String toString() {
        return "Weight[r=" + r + ", w=" + w + ", p=" + p + ", q=" + q + ", indexes=" + indexes + ", a=" + a + "]";
    }

    // 如果字符c在weightStr中找不到，则返回defaultValue1，
    // 如果字符c在weightStr中已经找到了，但是在字母c后面没有数字，则返回defaultValue2,
    // 否则返回字母c后面 的数字.
    private static int getUnitWeight(String weightStr, char c, Pattern p, int defaultValue1, int defaultValue2) {
        if (weightStr.indexOf(c) == -1) {
            return defaultValue1;
        } else {
            Matcher m = p.matcher(weightStr);
            m.find();

            if (m.group(1).length() == 0) {
                return defaultValue2;
            } else {
                return Integer.parseInt(m.group(1));
            }
        }
    }

    private static Set<Integer> getUnitWeights(String weightStr, char c, Pattern p) {
        if (weightStr.indexOf(c) == -1) {
            return null;
        }
        Set<Integer> is = new HashSet<Integer>();
        int start = 0;
        Matcher m = p.matcher(weightStr);
        while (m.find(start)) {
            if (m.group(1).length() != 0) {
                is.add(Integer.valueOf(m.group(1)));
            }
            start = m.end();
        }
        return is;
    }
}

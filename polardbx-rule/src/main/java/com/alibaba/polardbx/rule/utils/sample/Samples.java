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

package com.alibaba.polardbx.rule.utils.sample;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * 一个描点集合的抽象。支持线程不安全的笛卡尔积迭代遍历。<br/>
 * 一个Sample表示一个笛卡尔积抽样，将多列独立的枚举值，用特殊的遍历方法，转换为笛卡尔积抽样（即不同列值的组合）
 * 一个Sample用一个Map<String, Object>表示，key包含了各列，value对应每列的一个取值
 * 
 * <pre>
 * 算法介绍：
 * 1. 构建多个列的Iterator
 * 2. 每个Iterator先取第一个值，此时处于最后一个列
 * 3. 遍历开始
 *   a. 从当前Iterator进行遍历，该列所有值遍历完成后，往前退到上一个列
 *   b. 上一列移动到下一个值，移动完成后进入下一个列进行遍历，如果无法移动，继续a的操作
 * </pre>
 * 
 * @author linxuan
 */
public class Samples implements Iterable<Map<String/* 列名 */, Object/* 列值 */>>, Iterator<Map<String, Object>> {

    private final Map<String, Set<Object>> columnEnumerates;
    private final String[]                 subColums;       // 使用哪几列，便于做sub
    private final Set<String>              subColumSet;     // 与subColums保持一致，便于判,只读

    public Samples(Map<String, Set<Object>> columnEnumerates){
        this.columnEnumerates = columnEnumerates;
        this.subColums = columnEnumerates.keySet().toArray(new String[columnEnumerates.size()]);
        this.subColumSet = columnEnumerates.keySet();// subColumSet只读，这样应该没问题
    }

    public Samples(Map<String, Set<Object>> columnEnumerates, String[] subColumns){
        this.columnEnumerates = columnEnumerates;
        this.subColums = subColumns;
        this.subColumSet = new HashSet<String>();
        this.subColumSet.addAll(Arrays.asList(subColumns));
        if (subColumSet.size() != subColums.length) {
            throw new IllegalArgumentException(Arrays.toString(subColumns) + " has duplicate columm");
        }
    }

    public Samples(Set<String> columnNames){
        this.columnEnumerates = new HashMap<String, Set<Object>>();
        for (String name : columnNames) {
            this.columnEnumerates.put(name, new HashSet<Object>(1));
        }
        this.subColums = columnNames.toArray(new String[columnEnumerates.size()]);
        this.subColumSet = Collections.unmodifiableSet(columnNames);// subColumSet只读
    }

    /**
     * @param columns 如果columns包含本对象columnEnumerates中不存在的key，后果不可预期
     */
    public Samples subSamples(String[] columns) {
        if (columns.length == this.subColums.length) {
            return this; // 这里就不判读columns是否都和this一致了，有一定风险
        }

        return new Samples(this.columnEnumerates, columns);// 可能会使第三层sub由小变大，但是不影响使用。也没有判读一致性
    }

    /**
     * @return 如果subColums和columnEnumerates相同，则直接返回，否则抽取
     */
    public Map<String, Set<Object>> getColumnEnumerates() {
        if (this.columnEnumerates.size() == subColums.length) {
            return this.columnEnumerates;
        } else {
            Map<String, Set<Object>> res = new HashMap<String, Set<Object>>(subColums.length);
            for (String column : subColums) {
                res.put(column, this.columnEnumerates.get(column));
            }
            return res;
        }
    }

    /**
     * @return 列个数
     */
    public int size() {
        return this.subColums.length;
    }

    // ======================== 笛卡尔积迭代遍历的相关操作方法 ===========================

    /**
     * 下面是笛卡尔积迭代遍历的实现
     */
    private Map<String, Object> currentCartesianSample; // currentCartesianProduct当前的笛卡尔值
    private Iterator<Object>[]  iterators;             // 这种方式尾端iterator要反复重新打开，KeyIterator对象会创建比较多。考虑用Object[]加游标
    private int                 cursor;

    /**
     * 向一个列添加枚举值
     */
    public void addEnumerates(String name, Set<Object> values) {
        if (columnEnumerates.containsKey(name)) {
            columnEnumerates.get(name).addAll(values);
        } else {
            throw new IllegalArgumentException(Arrays.toString(subColums) + ", Samples not contain key:" + name);
        }
    }

    /**
     * 添加一个Sample组合。若某个列名不在本Samples中，则直接抛空指针
     */
    public void addSample(Map<String, Object> aCartesianSample) {
        for (Map.Entry<String, Object> e : aCartesianSample.entrySet()) {
            columnEnumerates.get(e.getKey()).add(e.getValue());
        }
    }

    @SuppressWarnings("unchecked")
    public Iterator<Map<String, Object>> iterator() {
        // 每次迭代前清空上次迭代状态
        currentCartesianSample = new HashMap<String, Object>(subColums.length);
        iterators = new Iterator[subColums.length];
        int i = cursor = 0;
        for (String name : subColums) {
            iterators[i++] = columnEnumerates.get(name).iterator();
        }
        return this;
    }

    public boolean hasNext() {
        for (Iterator<Object> it : iterators) {
            if (it.hasNext()) {
                return true;
            }
        }
        return false;
    }

    /**
     * 返回结果只能读取。如若修改后果不可预期。 columnSamples每个列的枚举值集合必须至少有一个元素。
     */
    public Map<String, Object> next() {
        for (;;) {
            if (iterators[cursor].hasNext()) {
                currentCartesianSample.put(subColums[cursor], iterators[cursor].next());
                if (cursor == subColums.length - 1) {
                    break;
                } else {
                    cursor++;
                }
            } else {
                if (cursor == 0) {
                    break; // 全部结束了
                } else {
                    // 重新打开当前的iterator备下一轮用
                    iterators[cursor] = columnEnumerates.get(subColums[cursor]).iterator();
                    cursor--;
                }
            }
        }
        return currentCartesianSample;
    }

    public void remove() {
        throw new UnsupportedOperationException(getClass().getName() + ".remove()");
    }

    public Set<Object> getColumnEnumerates(String name) {
        return columnEnumerates.get(name);
    }

    public Set<String> getSubColumSet() {
        return subColumSet;
    }

    public String toString() {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        StringBuilder sb = new StringBuilder("Samples{");
        for (String column : this.subColumSet) {
            sb.append(column).append("=[");
            for (Object value : this.columnEnumerates.get(column)) {
                if (value instanceof Calendar) {
                    sb.append(df.format(((Calendar) value).getTime())).append(",");
                } else {
                    sb.append(value).append(",");
                }
            }
            sb.append("]");
        }
        sb.append("}");
        return sb.toString();
    }

}

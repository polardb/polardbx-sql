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

package com.alibaba.polardbx.rule.model;

import com.taobao.tddl.common.utils.TddlToStringStyle;
import org.apache.commons.lang.builder.ToStringBuilder;

import java.util.Calendar;

/**
 * 用于传递自增数字和自增数字对应在Calendar里的类型 继承Comparable是因为开始预留的接口是Comparable...
 *
 * @author shenxun
 */
public class DateEnumerationParameter implements Comparable {

    /**
     * 默认使用Date作为日期类型的基本自增单位
     */
    public DateEnumerationParameter(int atomicIncreateNumber) {
        this.atomicIncreatementNumber = atomicIncreateNumber;
        this.calendarFieldType = Calendar.DATE;
    }

    public DateEnumerationParameter(int atomicIncreateNumber, int calendarFieldType) {
        this.atomicIncreatementNumber = atomicIncreateNumber;
        this.calendarFieldType = calendarFieldType;
    }

    public final int atomicIncreatementNumber;
    public final int calendarFieldType;

    @Override
    public int compareTo(Object o) {
        throw new IllegalArgumentException("should not be here !");
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, TddlToStringStyle.DEFAULT_STYLE);
    }

}

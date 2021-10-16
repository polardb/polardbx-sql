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

package com.alibaba.polardbx.rule.utils;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.time.old.DateUtils;
import com.alibaba.polardbx.rule.TableRule;
import com.alibaba.polardbx.rule.impl.groovy.GroovyStaticMethod;
import org.apache.commons.lang.time.DateFormatUtils;

import java.util.Calendar;
import java.util.GregorianCalendar;

/**
 * noloop版本时间分片规则覆盖器
 */
public class NoloopCoverRuleProcessor implements CoverRuleProcessor {

    @Override
    public String process(String currentRule, TableRule currentTableRule, TableRule originTableRule) {
        if (currentTableRule == null || originTableRule == null) {
            return currentRule;
        }
        // 如果VirtualTableRoot的lazyInit属性配置为true,那么每次refresh VirtualTableRoot时,是不会init各个tableRule的
        // 所以连续两次建表时,由于第二次建表DDL需要拿到第一次DDL的起止时间,所以需要将第一次建表的originTableRule再次计算拓扑,
        // 通过计算拓扑AdvancedParameter.enumerateRange,将originTableRule的起止时间等noLoop属性赋值
        // 第一次建表DDL后的VirtualTableRoot会在pushRule后sync时被重新refresh装配,但VirtualTableRoot如果为lazyInit,是不会init第一次建完表后的各个tableRule
        if (originTableRule != null) {
            originTableRule.initActualTopology();
        }
        verify(currentTableRule, originTableRule);
        String freshRule = currentRule
            .replaceFirst(String.valueOf(currentTableRule.getStart()), String.valueOf(originTableRule.getStart()));
        return freshRule;
    }

    private void verify(TableRule currentTableRule, TableRule originTableRule) {
        // 对于第二次建表的开始时间需要连续跟随上一次建表的结束时间, 两次建表之间不能出现时间上的空洞
        Calendar currentStart = new GregorianCalendar();
        Calendar originEnd = new GregorianCalendar();
        currentStart.setTime(DateUtils.str_to_time(String.valueOf(currentTableRule.getStart())));
        originEnd.setTime(DateUtils.str_to_time(String.valueOf(originTableRule.getEnd())));

        if (!currentTableRule.getPartitionType().equals(originTableRule.getPartitionType())) {
            throw new TddlRuntimeException(ErrorCode.ERR_ROUTE, "current rule not equal origin rule");
        }

        switch (currentTableRule.getPartitionType()) {
        case DATE_ABS:
            originEnd.add(Calendar.DATE, 1);
            if (!originEnd.getTime().equals(currentStart.getTime())) {
                throw new TddlRuntimeException(ErrorCode.ERR_ROUTE,
                    "YYYYDD_NOLOOP sharding rule, current start must begin with origin end, please start with "
                        + DateFormatUtils.format(originEnd, "yyyyMMdd"));
            }
            break;
        case MONTH_ABS:
            originEnd.add(Calendar.MONTH, 1);
            if (!GroovyStaticMethod.yyyymm_noloop(originEnd.getTime())
                .equals(GroovyStaticMethod.yyyymm_noloop(currentStart.getTime()))) {
                throw new TddlRuntimeException(ErrorCode.ERR_ROUTE,
                    "YYYYMM_NOLOOP sharding rule, current start must begin with origin end, please start with "
                        + DateFormatUtils.format(originEnd, "yyyyMMdd"));
            }
            break;
        case WEEK_ABS:
            originEnd.add(Calendar.WEEK_OF_YEAR, 1);
            if (!GroovyStaticMethod.yyyyweek_noloop(originEnd.getTime())
                .equals(GroovyStaticMethod.yyyyweek_noloop(currentStart.getTime()))) {
                throw new TddlRuntimeException(ErrorCode.ERR_ROUTE,
                    "YYYYWEEK_NOLOOP sharding rule, current start must begin with origin end, please start with "
                        + DateFormatUtils.format(originEnd, "yyyyMMdd"));
            }
            break;
        default:
            throw new TddlRuntimeException(ErrorCode.ERR_ROUTE, "current rule is not noloop rule");
        }
    }
}
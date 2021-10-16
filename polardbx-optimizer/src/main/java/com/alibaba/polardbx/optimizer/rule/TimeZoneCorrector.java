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

package com.alibaba.polardbx.optimizer.rule;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.utils.timezone.InternalTimeZone;
import com.alibaba.polardbx.common.utils.timezone.TimeZoneUtils;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.rel.ShardProcessor;
import com.alibaba.polardbx.rule.TableRule;
import com.alibaba.polardbx.rule.utils.CalcParamsAttribute;
import com.alibaba.polardbx.rule.utils.GroovyRuleShardFuncFinder;

import java.sql.Timestamp;
import java.text.ParseException;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeSet;

/**
 * @author chenghui.lch
 */
public class TimeZoneCorrector {

    protected TimeZone connTimeZone;
    protected TimeZone ruleTimeZone;
    protected TableRule tableRule;

    public TimeZoneCorrector(InternalTimeZone logicalTimeZone, TableRule tableRule, TimeZone connTimeZone) {
        this.connTimeZone = connTimeZone;

        if (logicalTimeZone != null) {
            this.ruleTimeZone = logicalTimeZone.getTimeZone();
        }
        this.tableRule = tableRule;
    }

    @SuppressWarnings("rawtypes")
    public Object correctTimeZoneIfNeed(String colName, DataType dataType, Object originalVal,
                                        Map<String, Object> calcParams) {

        // Only correct timezone for TIMESTAMP & TIME dataType, other dataTypes return directly
        if (dataType.getSqlType() != java.sql.Types.TIMESTAMP && dataType.getSqlType() != java.sql.Types.TIME) {
            return originalVal;
        }

        if (connTimeZone == null) {
            return originalVal;
        }

        if (ruleTimeZone == null) {
            return originalVal;
        }

        if (connTimeZone.getID().equals(ruleTimeZone.getID())) {
            return originalVal;
        }

        if (tableRule != null) {
            TreeSet dbKeySet = (TreeSet) calcParams.get(CalcParamsAttribute.DB_SHARD_KEY_SET);
            TreeSet tbKeySet = (TreeSet) calcParams.get(CalcParamsAttribute.TB_SHARD_KEY_SET);

            if (dbKeySet == null) {
                dbKeySet = ShardProcessor.listToTreeSet(tableRule.getDbPartitionKeys());
                calcParams.put(CalcParamsAttribute.DB_SHARD_KEY_SET, dbKeySet);
            }
            if (tbKeySet == null) {
                tbKeySet = ShardProcessor.listToTreeSet(tableRule.getTbPartitionKeys());
                calcParams.put(CalcParamsAttribute.TB_SHARD_KEY_SET, tbKeySet);
            }

            if (dbKeySet != null && dbKeySet.contains(colName)) {
                String dbGroovyShardMethodName = tableRule.getDbGroovyShardMethodName();
                if (dbGroovyShardMethodName == null
                    || !GroovyRuleShardFuncFinder.groovyDateMethodFunctionSet.contains(dbGroovyShardMethodName)) {
                    return originalVal;
                }
                Object newVal = null;
                newVal = converTimeZone(originalVal, newVal);
                return newVal;
            }

            if (tbKeySet != null && tbKeySet.contains(colName)) {
                String tbGroovyShardMethodName = tableRule.getTbGroovyShardMethodName();
                if (tbGroovyShardMethodName == null
                    || !GroovyRuleShardFuncFinder.groovyDateMethodFunctionSet.contains(tbGroovyShardMethodName)) {
                    return originalVal;
                }
                Object newVal = null;
                newVal = converTimeZone(originalVal, newVal);
                return newVal;
            }
        }

        return originalVal;
    }

    private Object converTimeZone(Object originalVal, Object newVal) {

        try {
            if (originalVal instanceof String) {
                newVal = TimeZoneUtils.convertBetweenTimeZone((String) originalVal, connTimeZone, ruleTimeZone);
            } else if (originalVal instanceof Timestamp) {
                newVal = TimeZoneUtils.convertBetweenTimeZone((Timestamp) originalVal, connTimeZone, ruleTimeZone);
            } else {
                newVal = originalVal;
            }
        } catch (ParseException e) {
            throw new TddlNestableRuntimeException(e);
        }
        return newVal;
    }

}

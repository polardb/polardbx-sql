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

package com.alibaba.polardbx.rule.enumerator;

import com.alibaba.polardbx.rule.enumerator.handler.DefaultEnumerator;
import com.alibaba.polardbx.rule.Rule.RuleColumn;
import com.alibaba.polardbx.rule.enumerator.handler.AbstractCloseIntervalFieldsEnumeratorHandler;
import com.alibaba.polardbx.rule.enumerator.handler.BigDecimalPartDiscontinousRangeEnumerator;
import com.alibaba.polardbx.rule.enumerator.handler.BigIntegerPartDiscontinousRangeEnumerator;
import com.alibaba.polardbx.rule.enumerator.handler.CloseIntervalFieldsEnumeratorHandler;
import com.alibaba.polardbx.rule.enumerator.handler.DatePartDiscontinousRangeEnumerator;
import com.alibaba.polardbx.rule.enumerator.handler.IntegerPartDiscontinousRangeEnumerator;
import com.alibaba.polardbx.rule.enumerator.handler.LongPartDiscontinousRangeEnumerator;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * ${DESCRIPTION}
 *
 * @author hongxi.chx
 */
public abstract class BaseEnumerator implements Enumerator {

    protected static final String DEFAULT_ENUMERATOR = "DEFAULT_ENUMERATOR";
    protected boolean isDebug = false;

    //protected static final Map<String, CloseIntervalFieldsEnumeratorHandler> enumeratorMap      = new HashMap<String, CloseIntervalFieldsEnumeratorHandler>();
    //{
    //    enumeratorMap.put(Integer.class.getName(), new IntegerPartDiscontinousRangeEnumerator());
    //    enumeratorMap.put(Long.class.getName(), new LongPartDiscontinousRangeEnumerator());
    //    enumeratorMap.put(BigDecimal.class.getName(), new BigDecimalPartDiscontinousRangeEnumerator());
    //    enumeratorMap.put(BigInteger.class.getName(), new BigIntegerPartDiscontinousRangeEnumerator());
    //
    //    enumeratorMap.put(Date.class.getName(), new DatePartDiscontinousRangeEnumerator());
    //    enumeratorMap.put(java.sql.Date.class.getName(), new DatePartDiscontinousRangeEnumerator());
    //    enumeratorMap.put(java.sql.Timestamp.class.getName(), new DatePartDiscontinousRangeEnumerator());
    //
    //    enumeratorMap.put(DEFAULT_ENUMERATOR, new DefaultEnumerator());
    //}

    /**
     *
     */
    protected RuleColumn ruleColumnParams;
    protected Map<String, CloseIntervalFieldsEnumeratorHandler> enumeratorHandlerMap = new HashMap<>();

    public BaseEnumerator() {
        initEnumeratorMap();
    }

    public BaseEnumerator(RuleColumn ruleColumnParams) {
        this.ruleColumnParams = ruleColumnParams;
        initEnumeratorMap();
    }

    protected void initEnumeratorMap() {

        AbstractCloseIntervalFieldsEnumeratorHandler closeFieldsEnumeHandler = null;

        closeFieldsEnumeHandler = new IntegerPartDiscontinousRangeEnumerator();
        closeFieldsEnumeHandler.setRuleColumn(this.ruleColumnParams);
        this.enumeratorHandlerMap.put(Integer.class.getName(), closeFieldsEnumeHandler);

        closeFieldsEnumeHandler = new LongPartDiscontinousRangeEnumerator();
        closeFieldsEnumeHandler.setRuleColumn(this.ruleColumnParams);
        this.enumeratorHandlerMap.put(Long.class.getName(), closeFieldsEnumeHandler);

        closeFieldsEnumeHandler = new BigDecimalPartDiscontinousRangeEnumerator();
        closeFieldsEnumeHandler.setRuleColumn(this.ruleColumnParams);
        this.enumeratorHandlerMap.put(BigDecimal.class.getName(), closeFieldsEnumeHandler);

        closeFieldsEnumeHandler = new BigDecimalPartDiscontinousRangeEnumerator();
        closeFieldsEnumeHandler.setRuleColumn(this.ruleColumnParams);
        this.enumeratorHandlerMap.put(BigInteger.class.getName(), new BigIntegerPartDiscontinousRangeEnumerator());

        closeFieldsEnumeHandler = new DatePartDiscontinousRangeEnumerator();
        closeFieldsEnumeHandler.setRuleColumn(this.ruleColumnParams);
        this.enumeratorHandlerMap.put(Date.class.getName(), closeFieldsEnumeHandler);
        this.enumeratorHandlerMap.put(java.sql.Date.class.getName(), closeFieldsEnumeHandler);
        this.enumeratorHandlerMap.put(java.sql.Timestamp.class.getName(), closeFieldsEnumeHandler);

        closeFieldsEnumeHandler = new DefaultEnumerator();
        closeFieldsEnumeHandler.setRuleColumn(this.ruleColumnParams);
        this.enumeratorHandlerMap.put(DEFAULT_ENUMERATOR, closeFieldsEnumeHandler);
    }

    protected CloseIntervalFieldsEnumeratorHandler getEnumeratorMapByClassName(String className) {
        return this.enumeratorHandlerMap.get(className);
    }

    @Override
    public abstract Set<Object> getEnumeratedValue(Comparable condition, Integer cumulativeTimes,
                                                   Comparable<?> atomicIncrementValue,
                                                   boolean needMergeValueInCloseInterval);
}

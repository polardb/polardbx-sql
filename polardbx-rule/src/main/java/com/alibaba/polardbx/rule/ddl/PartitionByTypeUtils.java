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

package com.alibaba.polardbx.rule.ddl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.rule.utils.GroovyRuleConstant;

/**
 * @author chenghui.lch 2017年3月13日 上午1:28:52
 * @since 5.0.0
 */
public class PartitionByTypeUtils {

    /**
     * 保存所有DRDS支持分表的拆分函数
     */
    protected static Set<PartitionByType>  allSupportedTbPartitionByTypeSet = new HashSet<PartitionByType>();

    /**
     * 保存所有DRDS支持分库的拆分函数
     */
    protected static Set<PartitionByType>  allSupportedDbPartitionByTypeSet = new HashSet<PartitionByType>();

    /**
     * 保存所有DRDS支持时间拆分函数， 用于从规则中识别时间类的拆分函数
     */
    protected static List<PartitionByType> allTimePartitionByTypeList       = new ArrayList<PartitionByType>();

    /**
     * 保存所有DRDS支持新型的DDL的拆分函数(重点要的特点：允许用户输入自定义参数)
     */
    protected static Set<PartitionByType>  allNewShardFunctionTypeList      = new HashSet<PartitionByType>();

    static {

        allSupportedTbPartitionByTypeSet.add(PartitionByType.HASH);
        allSupportedTbPartitionByTypeSet.add(PartitionByType.MM);
        allSupportedTbPartitionByTypeSet.add(PartitionByType.DD);
        allSupportedTbPartitionByTypeSet.add(PartitionByType.WEEK);
        allSupportedTbPartitionByTypeSet.add(PartitionByType.MMDD);
        allSupportedTbPartitionByTypeSet.add(PartitionByType.YYYYMM);
        allSupportedTbPartitionByTypeSet.add(PartitionByType.YYYYDD);
        allSupportedTbPartitionByTypeSet.add(PartitionByType.YYYYWEEK);
        allSupportedTbPartitionByTypeSet.add(PartitionByType.YYYYMM_OPT);
        allSupportedTbPartitionByTypeSet.add(PartitionByType.YYYYDD_OPT);
        allSupportedTbPartitionByTypeSet.add(PartitionByType.YYYYWEEK_OPT);
        allSupportedTbPartitionByTypeSet.add(PartitionByType.YYYYMM_NOLOOP);
        allSupportedTbPartitionByTypeSet.add(PartitionByType.YYYYDD_NOLOOP);
        allSupportedTbPartitionByTypeSet.add(PartitionByType.YYYYWEEK_NOLOOP);
        allSupportedTbPartitionByTypeSet.add(PartitionByType.RIGHT_SHIFT);
        allSupportedTbPartitionByTypeSet.add(PartitionByType.RANGE_HASH);
        allSupportedTbPartitionByTypeSet.add(PartitionByType.RANGE_HASH1);
        allSupportedTbPartitionByTypeSet.add(PartitionByType.UNI_HASH);
        allSupportedTbPartitionByTypeSet.add(PartitionByType.STR_HASH);

        // ------------------------------------------
        allSupportedDbPartitionByTypeSet.add(PartitionByType.HASH);
        allSupportedDbPartitionByTypeSet.add(PartitionByType.YYYYMM);
        allSupportedDbPartitionByTypeSet.add(PartitionByType.YYYYDD);
        allSupportedDbPartitionByTypeSet.add(PartitionByType.YYYYWEEK);
        allSupportedDbPartitionByTypeSet.add(PartitionByType.YYYYMM_OPT);
        //allSupportedDbPartitionByTypeSet.add(PartitionByType.YYYYDD_OPT);
        //allSupportedDbPartitionByTypeSet.add(PartitionByType.YYYYWEEK_OPT);

        //allSupportedDbPartitionByTypeSet.add(PartitionByType.YYYYMM_NOLOOP);
        //allSupportedDbPartitionByTypeSet.add(PartitionByType.YYYYDD_NOLOOP);
        //allSupportedDbPartitionByTypeSet.add(PartitionByType.YYYYWEEK_NOLOOP);

        allSupportedDbPartitionByTypeSet.add(PartitionByType.RIGHT_SHIFT);
        allSupportedDbPartitionByTypeSet.add(PartitionByType.RANGE_HASH);
        allSupportedDbPartitionByTypeSet.add(PartitionByType.RANGE_HASH1);
        allSupportedDbPartitionByTypeSet.add(PartitionByType.UNI_HASH);
        allSupportedDbPartitionByTypeSet.add(PartitionByType.STR_HASH);

        // ------------------------------------------
        allTimePartitionByTypeList.add(PartitionByType.YYYYMM_OPT);
        allTimePartitionByTypeList.add(PartitionByType.YYYYWEEK_OPT);
        allTimePartitionByTypeList.add(PartitionByType.YYYYDD_OPT);

        //allTimePartitionByTypeList.add(PartitionByType.YYYYMM_NOLOOP);
        //allTimePartitionByTypeList.add(PartitionByType.YYYYDD_NOLOOP);
        //allTimePartitionByTypeList.add(PartitionByType.YYYYWEEK_NOLOOP);

        allTimePartitionByTypeList.add(PartitionByType.YYYYMM);
        allTimePartitionByTypeList.add(PartitionByType.YYYYWEEK);
        allTimePartitionByTypeList.add(PartitionByType.YYYYDD);

        allTimePartitionByTypeList.add(PartitionByType.MMDD);
        allTimePartitionByTypeList.add(PartitionByType.WEEK);
        allTimePartitionByTypeList.add(PartitionByType.MM);
        allTimePartitionByTypeList.add(PartitionByType.DD);

        allNewShardFunctionTypeList.add(PartitionByType.RIGHT_SHIFT);
        allNewShardFunctionTypeList.add(PartitionByType.RANGE_HASH);
        allNewShardFunctionTypeList.add(PartitionByType.RANGE_HASH1);
        allNewShardFunctionTypeList.add(PartitionByType.UNI_HASH);
        allNewShardFunctionTypeList.add(PartitionByType.STR_HASH);
    }

    /**
     * 识别是否是优化型的PartitionBy函数
     * 
     * @param type
     * @return
     */
    public static boolean isOptimizedPartitionByType(PartitionByType type) {
        if (type == PartitionByType.YYYYMM_OPT || type == PartitionByType.YYYYDD_OPT
            || type == PartitionByType.YYYYWEEK_OPT || type == PartitionByType.YYYYMM_NOLOOP
            || type == PartitionByType.YYYYDD_NOLOOP || type == PartitionByType.YYYYWEEK_NOLOOP) {
            return true;
        } else {
            return false;
        }
    }

    public static boolean isSupportedTbPartitionByType(PartitionByType type) {
        return allSupportedTbPartitionByTypeSet.contains(type);
    }

    public static boolean isSupportedDbPartitionByType(PartitionByType type) {
        return allSupportedDbPartitionByTypeSet.contains(type);
    }

    public static List<PartitionByType> getAllTimePartitionByTypeList() {
        return allTimePartitionByTypeList;
    }

    public static boolean isNewShardFunctionTypeByType(PartitionByType type) {
        return allNewShardFunctionTypeList.contains(type);
    }

    public static PartitionByType getPartitionTypeByGroovyRuleTimeMethodName( String groovyMethodName ) {

        String groovyMethodNameLowerCase = groovyMethodName.toLowerCase();
        if (groovyMethodNameLowerCase.equals(GroovyRuleConstant.DD_I_METHOD)) {
            return PartitionByType.DD;
        } else if (groovyMethodNameLowerCase.equalsIgnoreCase(GroovyRuleConstant.MM_I_METHOD)) {
            return PartitionByType.MM;
        } else if (groovyMethodNameLowerCase.equalsIgnoreCase(GroovyRuleConstant.WEEK_METHOD)) {
            return PartitionByType.WEEK;
        } else if (groovyMethodNameLowerCase.equalsIgnoreCase(GroovyRuleConstant.MM_DD_I_METHOD)) {
            return PartitionByType.MMDD;
        } else if (groovyMethodNameLowerCase.equalsIgnoreCase(GroovyRuleConstant.YYYY_MM_I_METHOD)) {
            return PartitionByType.YYYYMM;
        } else if (groovyMethodNameLowerCase.equalsIgnoreCase(GroovyRuleConstant.YYYY_WEEK_I_METHOD)) {
            return PartitionByType.YYYYWEEK;
        } else if (groovyMethodNameLowerCase.equalsIgnoreCase(GroovyRuleConstant.YYYY_DD_I_METHOD)) {
            return PartitionByType.YYYYDD;
        } else if (groovyMethodNameLowerCase.equalsIgnoreCase(GroovyRuleConstant.YYYY_MM_I_OPT_METHOD)) {
            return PartitionByType.YYYYMM_OPT;
        } else if (groovyMethodNameLowerCase.equalsIgnoreCase(GroovyRuleConstant.YYYY_WEEK_I_OPT_METHOD)) {
            return PartitionByType.YYYYWEEK_OPT;
        } else if (groovyMethodNameLowerCase.equalsIgnoreCase(GroovyRuleConstant.YYYY_DD_I_OPT_METHOD)) {
            return PartitionByType.YYYYDD_OPT;
        } else {
            throw  new NotSupportException();
        }

    }

}

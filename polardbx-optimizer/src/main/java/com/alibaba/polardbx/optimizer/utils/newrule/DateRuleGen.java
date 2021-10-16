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

package com.alibaba.polardbx.optimizer.utils.newrule;

import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.parse.bean.DBPartitionDefinition;
import com.alibaba.polardbx.optimizer.parse.bean.TBPartitionDefinition;
import com.alibaba.polardbx.rule.ddl.PartitionByType;
import com.alibaba.polardbx.rule.ddl.PartitionByTypeUtils;

/**
 * Created by simiao on 15-6-4.
 */
public class DateRuleGen implements IRuleGen {

    private final String schemaName;

    public DateRuleGen(String schemaName) {
        this.schemaName = schemaName;
    }

    /**
     * @param dbOrTb true for group and false for table
     */
    public void genMiddleRuleStr(PartitionByType type, String idText, int start, StringBuilder formerBuilder,
                                 long tablesPerGroup, long groups, Boolean dbOrTb, Integer startWith, Integer endWith) {
        formerBuilder.append("(");
        switch (type) {

        case MM:
            formerBuilder.append("mm_i(");
            formerBuilder.append("#").append(idText).append(",");
            formerBuilder.append(start).append("_month,");
            /* 规则中直接写明对于存在上限的最大枚举数目 mm=12 */
            formerBuilder.append("12#");
            break;
        case DD:
            formerBuilder.append("dd_i(");
            formerBuilder.append("#").append(idText).append(",");
            formerBuilder.append(start).append("_date,");
            /* 规则中直接写明对于存在上限的最大枚举数目 dd=31 */
            formerBuilder.append("31#");
            break;
        case WEEK:
            formerBuilder.append("week(");
            formerBuilder.append("#").append(idText).append(",");
            formerBuilder.append(start).append("_date,");
            /* 规则中直接写明对于存在上限的最大枚举数目 week=7 */
            formerBuilder.append("7#");
            break;
        case MMDD:
            formerBuilder.append("mmdd_i(");
            formerBuilder.append("#").append(idText).append(",");
            formerBuilder.append(start).append("_date,");
            /**
             * 规则中直接写明对于存在上限的最大枚举数目 mmdd=366 注意:
             * 虽然这里指定了期望枚举的数目，但是对于AdvancedParameterParser中
             * 对于dd/mmdd的特殊处理还是不能缺少的，因为如果不特殊处理将都是从当前时间开始枚举
             * 虽然枚举点多，但带入标准时间函数中将的到相同的结果从而被覆盖而不能达到需要的分库分表 拓扑数目要求。
             */
            formerBuilder.append("366#");
            break;

        case YYYYMM: {

            // 对于年份与月数，即 YYYY-MM 这类型，YYYY其实是没有取值限制的，所以为了保证规则枚举完整，枚举次数
            // 需要是各个分库的所有分表总和的次数

            long enumVal = 12;
            if (tablesPerGroup > TABLE_COUNT_OF_USE_STANT_ALONE && groups > GROUP_COUNT_OF_USE_STANT_ALONE) {
                // 非useStandAlone模式

                // 在非useStandAlone模式下，分表分表规则是先分表，再分库
                // 这个模式下，各个分库的分表表表名是不允许重复的, 即分表表名是全局唯一的
                enumVal = tablesPerGroup * groups;
            } else if (tablesPerGroup == TABLE_COUNT_OF_USE_STANT_ALONE && groups > GROUP_COUNT_OF_USE_STANT_ALONE) {
                // group的useStandAlone模式

                // 在useStandAlone模式下，分表规则与分库规则是独立枚举（这通常是因为拆分键不一样）
                // 这个模式下，各个分库的分表表表名是允许重复的

                enumVal = groups;
            } else if (tablesPerGroup > TABLE_COUNT_OF_USE_STANT_ALONE && groups == GROUP_COUNT_OF_USE_STANT_ALONE) {
                // table的useStandAlone模式

                // 在useStandAlone模式下，分表规则与分库规则是独立枚举（这通常是因为拆分键不一样）
                // 这个模式下，各个分库的分表表表名是允许重复的

                enumVal = tablesPerGroup;
            }

            formerBuilder.append("yyyymm_i(");
            formerBuilder.append("#").append(idText).append(",");
            formerBuilder.append(start).append("_month,");

            // 如果用户采用年月做YYYYMM做hash函数，由于yyyy是没有取值限制的，
            // 所以，表的枚举数必须是总枚举数，
            formerBuilder.append(enumVal).append("#");
        }

        break;

        case YYYYMM_NOLOOP: {
            String ruleFormat = "yyyymm_noloop(#%s,%s_MONTH_ABS,%s_%s#";
            formerBuilder.append(String.format(ruleFormat, idText, start, startWith, endWith));
        }

        break;
        case YYYYDD_NOLOOP: {
            String ruleFormat = "yyyydd_noloop(#%s,%s_DATE_ABS,%s_%s#";
            formerBuilder.append(String.format(ruleFormat, idText, start, startWith, endWith));
        }

        break;
        case YYYYWEEK_NOLOOP: {
            String ruleFormat = "yyyyweek_noloop(#%s,%s_WEEK_ABS,%s_%s#";
            formerBuilder.append(String.format(ruleFormat, idText, start, startWith, endWith));
        }

        break;

        case YYYYMM_OPT: {

            // YYYYMM_OPT是YYYYMM优化型Hash函数，它实现了数据按月hash并能够很均匀地分布在各个MySQL实例上

            // 对于年份与月数，即 YYYY-MM 这类型，YYYY其实是没有取值限制的，所以为了保证规则枚举完整，枚举次数
            // 需要是各个分库的所有分表总和的次数

            long enumVal = 12;
            if (tablesPerGroup > TABLE_COUNT_OF_USE_STANT_ALONE && groups > GROUP_COUNT_OF_USE_STANT_ALONE) {
                // 非useStandAlone模式

                // 在非useStandAlone模式下，分表分表规则是先分表，再分库
                // 这个模式下，各个分库的分表表表名是不允许重复的, 即分表表名是全局唯一的

                enumVal = tablesPerGroup * groups;

                int insts = OptimizerContext.getContext(schemaName).getMatrix().getMasterRepoInstMap().size();

                // 如果用户采用年月做YYYYMM_OPT做hash函数，由于yyyy是没有取值限制的，
                // 所以，表的枚举数必须是总枚举数，
                String ruleFormat = "yyyymm_i_opt(#%s,%s_month,%s#, %s, %s, %s";

                if (dbOrTb) {
                    if (tablesPerGroup > 1) {
                        formerBuilder.append(String.format(ruleFormat,
                            idText,
                            start,
                            enumVal,
                            enumVal,
                            groups,
                            insts));
                    } else {
                        // 即tablesPerGroup = 1， 只分库，不分表， 则不考虑规则里不考虑存储实例
                        formerBuilder.append(String.format(ruleFormat, idText, start, enumVal, enumVal, 0, 0));
                    }

                } else {
                    formerBuilder.append(String.format(ruleFormat, idText, start, enumVal, enumVal, 0, 0));
                }

            } else if (tablesPerGroup == TABLE_COUNT_OF_USE_STANT_ALONE && groups > GROUP_COUNT_OF_USE_STANT_ALONE) {
                // group的useStandAlone模式

                // 在useStandAlone模式下，分表规则与分库规则是独立枚举（这通常是因为拆分键不一样）
                // 这个模式下，各个分库的分表表表名是允许重复的

                enumVal = groups;

                // 如果用户采用年月做YYYYMM_OPT做hash函数，由于yyyy是没有取值限制的，
                // 所以，表的枚举数必须是总枚举数，
                formerBuilder.append(String.format("yyyymm_i_opt(#%s,%s_month,%s#, %s, 0, 0",
                    idText,
                    start,
                    enumVal,
                    enumVal));

            } else if (tablesPerGroup > TABLE_COUNT_OF_USE_STANT_ALONE && groups == GROUP_COUNT_OF_USE_STANT_ALONE) {
                // table的useStandAlone模式

                // 在useStandAlone模式下，分表规则与分库规则是独立枚举（这通常是因为拆分键不一样）
                // 这个模式下，各个分库的分表表表名是允许重复的

                enumVal = tablesPerGroup;

                // 如果用户采用年月做YYYYMM_OPT做hash函数，由于yyyy是没有取值限制的，
                // 所以，表的枚举数必须是总枚举数，
                formerBuilder.append(String.format("yyyymm_i_opt(#%s,%s_month,%s#, %s, 0, 0",
                    idText,
                    start,
                    enumVal,
                    enumVal));
            }

        }
        break;

        case YYYYWEEK: {
            // 对于年份与月数，即 YYYY-WEEK 这类型，YYYY其实是没有取值限制的，所以为了保证规则枚举完整，枚举次数
            // 需要是各个分库的所有分表总和的次数

            long enumVal = 53;
            if (tablesPerGroup > TABLE_COUNT_OF_USE_STANT_ALONE && groups > GROUP_COUNT_OF_USE_STANT_ALONE) {
                // 非useStandAlone模式

                // 在非useStandAlone模式下，分表分表规则是先分表，再分库
                // 这个模式下，各个分库的分表表表名是不允许重复的, 即分表表名是全局唯一的
                enumVal = tablesPerGroup * groups;
            } else if (tablesPerGroup == TABLE_COUNT_OF_USE_STANT_ALONE && groups > GROUP_COUNT_OF_USE_STANT_ALONE) {
                // group的useStandAlone模式

                // 在useStandAlone模式下，分表规则与分库规则是独立枚举（这通常是因为拆分键不一样）
                // 这个模式下，各个分库的分表表表名是允许重复的

                enumVal = groups;
            } else if (tablesPerGroup > TABLE_COUNT_OF_USE_STANT_ALONE && groups == GROUP_COUNT_OF_USE_STANT_ALONE) {
                // table的useStandAlone模式

                // 在useStandAlone模式下，分表规则与分库规则是独立枚举（这通常是因为拆分键不一样）
                // 这个模式下，各个分库的分表表表名是允许重复的

                enumVal = tablesPerGroup;
            }

            formerBuilder.append("yyyyweek_i(");
            formerBuilder.append("#").append(idText).append(",");
            formerBuilder.append(start).append("_week,");

            // 如果用户采用年月做YYYYWEEK做hash函数，由于yyyy是没有取值限制的，
            // 所以，表的枚举数必须是总枚举数，
            formerBuilder.append(enumVal).append("#");
        }

        break;
        case YYYYWEEK_OPT: {
            // YYYYWEEK_OPT是YYYYWEEK优化型Hash函数，它实现了数据按月hash并能够很均匀地分布在各个MySQL实例上

            // 对于年份与月数，即 YYYYWEEK这类型，YYYY其实是没有取值限制的，所以为了保证规则枚举完整，枚举次数
            // 需要是各个分库的所有分表总和的次数

            long enumVal = 53;
            if (tablesPerGroup > TABLE_COUNT_OF_USE_STANT_ALONE && groups > GROUP_COUNT_OF_USE_STANT_ALONE) {
                // 非useStandAlone模式

                // 在非useStandAlone模式下，分表分表规则是先分表，再分库
                // 这个模式下，各个分库的分表表表名是不允许重复的, 即分表表名是全局唯一的

                enumVal = tablesPerGroup * groups;

                // int insts = OptimizerContext.getContext();
                int insts = OptimizerContext.getContext(schemaName).getMatrix().getMasterRepoInstMap().size();

                // 如果用户采用年月做YYYYWEEK_OPT做hash函数，由于yyyy是没有取值限制的，
                // 所以，表的枚举数必须是总枚举数，
                String ruleFormat = "yyyyweek_i_opt(#%s,%s_week,%s#, %s, %s, %s";

                if (dbOrTb) {
                    if (tablesPerGroup > 1) {
                        formerBuilder.append(String.format(ruleFormat,
                            idText,
                            start,
                            enumVal,
                            enumVal,
                            groups,
                            insts));
                    } else {
                        // 即tablesPerGroup = 1， 只分库，不分表， 则不考虑规则里不考虑存储实例
                        formerBuilder.append(String.format(ruleFormat, idText, start, enumVal, enumVal, 0, 0));
                    }

                } else {
                    formerBuilder.append(String.format(ruleFormat, idText, start, enumVal, enumVal, 0, 0));
                }

            } else if (tablesPerGroup == TABLE_COUNT_OF_USE_STANT_ALONE && groups > GROUP_COUNT_OF_USE_STANT_ALONE) {
                // group的useStandAlone模式

                // 在useStandAlone模式下，分表规则与分库规则是独立枚举（这通常是因为拆分键不一样）
                // 这个模式下，各个分库的分表表表名是允许重复的

                enumVal = groups;

                // 如果用户采用年月做YYYYWEEK_OPT做hash函数，由于yyyy是没有取值限制的，
                // 所以，表的枚举数必须是总枚举数，
                formerBuilder.append(String.format("yyyyweek_i_opt(#%s,%s_week,%s#, %s, 0, 0",
                    idText,
                    start,
                    enumVal,
                    enumVal));

            } else if (tablesPerGroup > TABLE_COUNT_OF_USE_STANT_ALONE && groups == GROUP_COUNT_OF_USE_STANT_ALONE) {
                // table的useStandAlone模式

                // 在useStandAlone模式下，分表规则与分库规则是独立枚举（这通常是因为拆分键不一样）
                // 这个模式下，各个分库的分表表表名是允许重复的

                enumVal = tablesPerGroup;

                // 如果用户采用年月做YYYYWEEK_OPT做hash函数，由于yyyy是没有取值限制的，
                // 所以，表的枚举数必须是总枚举数，
                formerBuilder.append(String.format("yyyyweek_i_opt(#%s,%s_week,%s#, %s, 0, 0",
                    idText,
                    start,
                    enumVal,
                    enumVal));

            }
        }

        break;

        case YYYYDD: {
            // 对于年份与月数，即 YYYY-WEEK 这类型，YYYY其实是没有取值限制的，所以为了保证规则枚举完整，枚举次数
            // 需要是各个分库的所有分表总和的次数

            long enumVal = 366;
            if (tablesPerGroup > TABLE_COUNT_OF_USE_STANT_ALONE && groups > GROUP_COUNT_OF_USE_STANT_ALONE) {
                // 非useStandAlone模式

                // 在非useStandAlone模式下，分表分表规则是先分表，再分库
                // 这个模式下，各个分库的分表表表名是不允许重复的, 即分表表名是全局唯一的
                enumVal = tablesPerGroup * groups;
            } else if (tablesPerGroup == TABLE_COUNT_OF_USE_STANT_ALONE && groups > GROUP_COUNT_OF_USE_STANT_ALONE) {
                // group的useStandAlone模式

                // 在useStandAlone模式下，分表规则与分库规则是独立枚举（这通常是因为拆分键不一样）
                // 这个模式下，各个分库的分表表表名是允许重复的

                enumVal = groups;
            } else if (tablesPerGroup > TABLE_COUNT_OF_USE_STANT_ALONE && groups == GROUP_COUNT_OF_USE_STANT_ALONE) {
                // table的useStandAlone模式

                // 在useStandAlone模式下，分表规则与分库规则是独立枚举（这通常是因为拆分键不一样）
                // 这个模式下，各个分库的分表表表名是允许重复的

                enumVal = tablesPerGroup;
            }

            formerBuilder.append("yyyydd_i(");
            formerBuilder.append("#").append(idText).append(",");
            formerBuilder.append(start).append("_date,");

            // 如果用户采用年月做YYYYWEEK做hash函数，由于yyyy是没有取值限制的，
            // 所以，表的枚举数必须是总枚举数，
            formerBuilder.append(enumVal).append("#");
        }

        break;
        case YYYYDD_OPT: {
            // YYYYDD_OPT是YYYYDD优化型Hash函数，它实现了数据按月hash并能够很均匀地分布在各个MySQL实例上

            // 对于年份与月数，即 YYYY-MM 这类型，YYYY其实是没有取值限制的，所以为了保证规则枚举完整，枚举次数
            // 需要是各个分库的所有分表总和的次数

            long enumVal = 366;
            if (tablesPerGroup > TABLE_COUNT_OF_USE_STANT_ALONE && groups > GROUP_COUNT_OF_USE_STANT_ALONE) {
                // 非useStandAlone模式

                // 在非useStandAlone模式下，分表分表规则是先分表，再分库
                // 这个模式下，各个分库的分表表表名是不允许重复的, 即分表表名是全局唯一的

                enumVal = tablesPerGroup * groups;

                // int insts = OptimizerContext.getContext();
                int insts = OptimizerContext.getContext(schemaName).getMatrix().getMasterRepoInstMap().size();

                // 如果用户采用年月做YYYYDD_OPT做hash函数，由于yyyy是没有取值限制的，
                // 所以，表的枚举数必须是总枚举数，
                String ruleFormat = "yyyydd_i_opt(#%s,%s_date,%s#, %s, %s, %s";

                if (dbOrTb) {
                    if (tablesPerGroup > 1) {
                        formerBuilder.append(String.format(ruleFormat,
                            idText,
                            start,
                            enumVal,
                            enumVal,
                            groups,
                            insts));
                    } else {
                        // 即tablesPerGroup = 1， 只分库，不分表， 则不考虑规则里不考虑存储实例
                        formerBuilder.append(String.format(ruleFormat, idText, start, enumVal, enumVal, 0, 0));
                    }

                } else {
                    formerBuilder.append(String.format(ruleFormat, idText, start, enumVal, enumVal, 0, 0));
                }

            } else if (tablesPerGroup == TABLE_COUNT_OF_USE_STANT_ALONE && groups > GROUP_COUNT_OF_USE_STANT_ALONE) {
                // group的useStandAlone模式

                // 在useStandAlone模式下，分表规则与分库规则是独立枚举（这通常是因为拆分键不一样）
                // 这个模式下，各个分库的分表表表名是允许重复的

                enumVal = groups;

                // 如果用户采用年月做YYYYDD_OPT做hash函数，由于yyyy是没有取值限制的，
                // 所以，表的枚举数必须是总枚举数，
                formerBuilder.append(String.format("yyyydd_i_opt(#%s,%s_date,%s#, %s, 0, 0",
                    idText,
                    start,
                    enumVal,
                    enumVal));

            } else if (tablesPerGroup > TABLE_COUNT_OF_USE_STANT_ALONE && groups == GROUP_COUNT_OF_USE_STANT_ALONE) {
                // table的useStandAlone模式

                // 在useStandAlone模式下，分表规则与分库规则是独立枚举（这通常是因为拆分键不一样）
                // 这个模式下，各个分库的分表表表名是允许重复的

                enumVal = tablesPerGroup;

                // 如果用户采用年月做YYYYDD_OPT做hash函数，由于yyyy是没有取值限制的，
                // 所以，表的枚举数必须是总枚举数，
                formerBuilder.append(String.format("yyyydd_i_opt(#%s,%s_date,%s#, %s, 0, 0",
                    idText,
                    start,
                    enumVal,
                    enumVal));

            }
        }

        break;
        default:
            throw new IllegalArgumentException("Date column only support MM/DD/WEEK/MMDD/YYYYMM/YYYYMM_OPT method");
        }
    }

    @Override
    public String generateDbRuleArrayStr(PartitionByType type, String idText, int start, int groups, int tablesPerGroup,
                                         DBPartitionDefinition dbPartitionDefinition) {
        if (!PartitionByTypeUtils.isSupportedDbPartitionByType(type)) {
            throw new IllegalArgumentException(String.format("Date column not support %s method", type.toString()));
        }

        int totalTables = groups * tablesPerGroup;

        StringBuilder rule = new StringBuilder();

        genMiddleRuleStr(type, idText, start, rule, tablesPerGroup, groups, true, dbPartitionDefinition.getStartWith(),
            dbPartitionDefinition.getEndWith());

        if (PartitionByTypeUtils.isOptimizedPartitionByType(type)) {
            rule.append(").longValue() % ").append(groups).append(")");
        } else {
            rule.append(").longValue() % ").append(totalTables).append(")");
            rule.append(".intdiv(").append(tablesPerGroup).append(")");
        }

        return rule.toString();
    }

    @Override
    public String generateStandAloneDbRuleArrayStr(PartitionByType type, String idText, int start, int groups,
                                                   DBPartitionDefinition dbPartitionDefinition) {
        if (!PartitionByTypeUtils.isSupportedDbPartitionByType(type)) {
            throw new IllegalArgumentException(String.format("Date column not support %s method", type.toString()));
        }

        StringBuilder rule = new StringBuilder();
        // 因为当StandAlone模式时，分库规则与分表规则各自独立枚举
        // 各个group之间的表名是可以重复的，所以tbCount置为-1，用以标记
        int tbCount = TABLE_COUNT_OF_USE_STANT_ALONE;
        genMiddleRuleStr(type, idText, start, rule, tbCount, groups, true, dbPartitionDefinition.getStartWith(),
            dbPartitionDefinition.getEndWith());
        rule.append(").longValue() % ").append(groups).append(")");

        return rule.toString();
    }

    @Override
    public String generateTbRuleArrayStr(PartitionByType type, String idText, int start, int groups, int tablesPerGroup,
                                         TBPartitionDefinition tbPartitionDefinition) {
        if (!PartitionByTypeUtils.isSupportedTbPartitionByType(type)) {
            throw new IllegalArgumentException(String.format("Date column not support %s method", type.toString()));
        }

        StringBuilder rule = new StringBuilder();
        int totalTables = tablesPerGroup * groups;

        genMiddleRuleStr(type, idText, start, rule, tablesPerGroup, groups, false, tbPartitionDefinition.getStartWith(),
            tbPartitionDefinition.getEndWith());

        if (PartitionByTypeUtils.isOptimizedPartitionByType(type)) {
            rule.append(").longValue())");
        } else {
            rule.append(").longValue() % ").append(totalTables).append(")");
        }
        return rule.toString();
    }

    @Override
    public String generateStandAloneTbRuleArrayStr(PartitionByType type, String idText, int start, int tablesPerGroup,
                                                   TBPartitionDefinition tbPartitionDefinition) {
        if (!PartitionByTypeUtils.isSupportedTbPartitionByType(type)) {
            throw new IllegalArgumentException(String.format("Date column not support %s method", type.toString()));
        }
        StringBuilder rule = new StringBuilder();

        // 因为当StandAlone模式时，分库规则与分表规则各自独立枚举
        // 各个group之间的表名是可以重复的，所以dbCount置为-1，用以标记
        int dbCount = GROUP_COUNT_OF_USE_STANT_ALONE;
        genMiddleRuleStr(type, idText, start, rule, tablesPerGroup, dbCount, false,
            tbPartitionDefinition.getStartWith(), tbPartitionDefinition.getEndWith());
        rule.append(").longValue() % ").append(tablesPerGroup).append(")");

        return rule.toString();
    }
}

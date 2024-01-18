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

package com.alibaba.polardbx.gms.util;

import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.gms.tablegroup.TableGroupRecord;

/**
 * @author chenghui.lch
 */
public class TableGroupNameUtil {

    static final String TG_NAME_TEMPLATE = "tg%s";
    public static final String BROADCAST_TG_NAME_TEMPLATE = "broadcast_tg";
    public static final String SINGLE_DEFAULT_TG_NAME_TEMPLATE = "single_tg";
    static final String SINGLE_NON_DEFAULT_TG_NAME_TEMPLATE = "single_tg%s";
    public static final String OSS_TG_NAME_TEMPLATE = "oss_tg%s";

    public static String autoBuildTableGroupName(Long tgId, int tgType) {
        String tgName;
        if (tgType == TableGroupRecord.TG_TYPE_BROADCAST_TBL_TG) {
            tgName = BROADCAST_TG_NAME_TEMPLATE;
        } else if (tgType == TableGroupRecord.TG_TYPE_DEFAULT_SINGLE_TBL_TG) {
            tgName = SINGLE_DEFAULT_TG_NAME_TEMPLATE;
        } else if (tgType == TableGroupRecord.TG_TYPE_NON_DEFAULT_SINGLE_TBL_TG) {
            tgName = String.format(SINGLE_NON_DEFAULT_TG_NAME_TEMPLATE, tgId);
        } else if (tgType == TableGroupRecord.TG_TYPE_OSS_TBL_TG) {
            tgName = String.format(OSS_TG_NAME_TEMPLATE, tgId);
        } else {
            tgName = String.format(TG_NAME_TEMPLATE, tgId);
        }
        return tgName;
    }

    public static boolean isOssTg(String tgName) {
        if (StringUtils.isEmpty(tgName)) {
            return false;
        }
        return tgName.startsWith("oss_tg");
    }
}

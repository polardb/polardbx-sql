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

package com.alibaba.polardbx.optimizer.msha;

import java.util.Map;

import com.alibaba.polardbx.common.exception.TddlException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;

public class TddlMshaProcessor {

    public static final String IS_FOUND_UNIT    = "FOUND_UNIT";
    public static final String IS_SAME_UNIT     = "SAME_UNIT";
    public static final String IS_FORBID_WRITE  = "FORBID_WRITE";
    public static final String IS_FORBID_UPDATE = "FORBID_UPDATE";

    public static void processMsahHint(boolean isWrite, boolean isUpdateDelete,
                                       Map<String, Object> extraCmds) throws TddlException {

        /** 解析 sql,判断是否是 读或者 写请求 **/
        if (!isWrite) {
            // 直接 读库，不做任何操作
            return;
        }

        // 是否有 drds逻辑库的单元属性
        Boolean isFoundUnit = Boolean.valueOf((String) extraCmds.get(TddlMshaProcessor.IS_FOUND_UNIT));

        // drds 逻辑库的单元属性和当前 sql 流量的属性是否相等
        Boolean isSameUnitType = Boolean.valueOf((String) extraCmds.get(TddlMshaProcessor.IS_SAME_UNIT));

        // 当前流量 是否禁止更新
        Boolean isForbidWrite = Boolean.valueOf((String) extraCmds.get(TddlMshaProcessor.IS_FORBID_WRITE));

        // 当前流量是否禁写
        Boolean isForbidUpdate = Boolean.valueOf((String) extraCmds.get(TddlMshaProcessor.IS_FORBID_UPDATE));

        if (isFoundUnit == null || !isFoundUnit) {
            // 无单元化属性的逻辑库，放过
            return;
        }

        /** 走有单元化逻辑判断 **/
        if (isSameUnitType == null || isForbidUpdate == null || isForbidWrite == null) {
            // 单元化相关需要判断属性没有，不执行 sql，报错
            throw new TddlException(ErrorCode.ERR_ROUTE_MSHA_UNIT_PARAMS_INVALID,
                String.valueOf(isFoundUnit),
                String.valueOf(isSameUnitType),
                String.valueOf(isForbidWrite),
                String.valueOf(isForbidUpdate));
        }
        if (!isSameUnitType) {
            // 单元化类型不相等，不执行 sql，报错
            throw new TddlException(ErrorCode.ERR_ROUTE_MSHA_WRITE_SQL_UNIT_NOT_EQUAL_DB_UNIT,
                String.valueOf(isSameUnitType));

        }
        if (isForbidWrite) {
            // 禁写，不执行 sql，报错
            throw new TddlException(ErrorCode.ERR_ROUTE_MSHA_WRITE_SQL_FORBID_WRITE, String.valueOf(isForbidWrite));
        }

        if (isForbidUpdate && isUpdateDelete) {
            // 暂时全量禁止更新，后续 此处支持 < 某时间点的 禁止更新
            throw new TddlException(ErrorCode.ERR_ROUTE_MSHA_WRITE_SQL_FORBID_UPDATE, String.valueOf(isForbidWrite));
        }

        // 多活逻辑 ok，可以执行写操作
        return;

    }
}

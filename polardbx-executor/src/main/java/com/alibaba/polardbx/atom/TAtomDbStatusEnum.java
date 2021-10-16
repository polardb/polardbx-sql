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

package com.alibaba.polardbx.atom;

import com.alibaba.polardbx.atom.common.TAtomConstants;
import com.alibaba.polardbx.common.utils.TStringUtil;

/**
 * 数据库状态变量
 *
 * @author JIECHEN
 */
public enum TAtomDbStatusEnum {

    /**
     * R只读
     */
    R_STATUS(TAtomConstants.DB_STATUS_R),
    /**
     * W只写
     */
    W_STATUS(TAtomConstants.DB_STATUS_W),
    /**
     * RW可读写
     */
    RW_STATUS(TAtomConstants.DB_STATUS_RW),
    /**
     * NA不可读/写
     */
    NA_STATUS(TAtomConstants.DB_STATUS_NA);

    private final String status;

    TAtomDbStatusEnum(String status) {
        this.status = status;
    }

    public String getStatus() {
        return status;
    }

    public static TAtomDbStatusEnum getAtomDbStatusEnumByType(String type) {
        TAtomDbStatusEnum statusEnum = null;
        if (TStringUtil.isNotBlank(type)) {
            String typeStr = type.toUpperCase().trim();
            if (typeStr.length() > 1) {
                if (TAtomDbStatusEnum.NA_STATUS.getStatus().equals(typeStr)) {
                    statusEnum = TAtomDbStatusEnum.NA_STATUS;
                } else if (!TStringUtil.contains(typeStr, TAtomDbStatusEnum.NA_STATUS.getStatus())
                    && TStringUtil.contains(typeStr, TAtomDbStatusEnum.R_STATUS.getStatus())
                    && TStringUtil.contains(typeStr, TAtomDbStatusEnum.W_STATUS.getStatus())) {
                    statusEnum = TAtomDbStatusEnum.RW_STATUS;
                }
            } else {
                if (TAtomDbStatusEnum.R_STATUS.getStatus().equals(typeStr)) {
                    statusEnum = TAtomDbStatusEnum.R_STATUS;
                } else if (TAtomDbStatusEnum.W_STATUS.getStatus().equals(typeStr)) {
                    statusEnum = TAtomDbStatusEnum.W_STATUS;
                }
            }
        }
        return statusEnum;
    }

    public boolean isNaStatus() {
        return this == TAtomDbStatusEnum.NA_STATUS;
    }

    public boolean isRstatus() {
        return this == TAtomDbStatusEnum.R_STATUS || this == TAtomDbStatusEnum.RW_STATUS;
    }

    public boolean isWstatus() {
        return this == TAtomDbStatusEnum.W_STATUS || this == TAtomDbStatusEnum.RW_STATUS;
    }

    public boolean isRWstatus() {
        return this == TAtomDbStatusEnum.RW_STATUS;
    }
}

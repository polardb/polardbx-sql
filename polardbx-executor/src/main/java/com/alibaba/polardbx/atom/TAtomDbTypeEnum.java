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

/**
 * 数据库类型枚举类型
 *
 * @author qihao
 */
public enum TAtomDbTypeEnum {

    MYSQL;

    private final String driverClass = TAtomConstants.DEFAULT_MYSQL_DRIVER_CLASS;
    private final String sorterClass = TAtomConstants.DEFAULT_DRUID_MYSQL_SORTER_CLASS;

    public String getDriverClass() {
        return driverClass;
    }

    public String getSorterClass() {
        return sorterClass;
    }

}

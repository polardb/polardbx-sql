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

package com.alibaba.polardbx.optimizer.core.datatype;

import com.alibaba.polardbx.common.type.MySQLStandardFieldType;
import com.alibaba.polardbx.common.utils.convertor.ConvertorHelper;

/**
 * 标准的Bit类型实现
 *
 * @author jianghang 2014-1-21 下午5:37:38
 * @since 5.0.0
 */
public class BigBitType extends BigIntegerType {

    public BigBitType() {
        super();
        convertor = ConvertorHelper.bitBytesToNumber;
    }

    @Override
    public int getSqlType() {
        return java.sql.Types.BIT;
    }

    @Override
    public MySQLStandardFieldType fieldType() {
        return MySQLStandardFieldType.MYSQL_TYPE_BIT;
    }
}

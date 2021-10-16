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

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.optimizer.core.expression.bean.LobVal;

import java.nio.charset.StandardCharsets;

/**
 * mysql特殊的binary类型,按照iso-8859-1编码的字符串进行处理
 *
 * @author agapple 2015年3月4日 上午11:28:11
 * @since 5.1.18
 */
public class BinaryType extends BytesType {

    private final Calculator calculator = new AbstractDecimalCalculator() {

        @Override
        public Decimal convertToDecimal(Object v) {
            if (v instanceof byte[]) {
                // binary按照无编码字符串处理
                v = new String((byte[]) v, StandardCharsets.ISO_8859_1);
            }
            return DataTypes.DecimalType.convertFrom(v);
        }
    };

    public Calculator getCalculator() {
        return calculator;
    }

    @Override
    public byte[] convertFrom(Object value) {
        if (value != null && value.getClass() == LobVal.class) {
            value = ((LobVal) value).evaluation();
        }

        return super.convertFrom(value);
    }
}

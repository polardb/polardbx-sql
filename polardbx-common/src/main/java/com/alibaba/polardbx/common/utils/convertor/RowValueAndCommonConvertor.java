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

package com.alibaba.polardbx.common.utils.convertor;

import com.alibaba.polardbx.common.datatype.RowValue;

public class RowValueAndCommonConvertor {

    public static class RowValueToCommon extends AbastactConvertor {

        @Override
        public Object convert(Object src, Class destClass) {
            if (destClass == RowValue.class) {
                return src;
            }
            if (RowValue.class.isInstance(src)) {
                if (((RowValue) src).getValues().size() > 0) {
                    throw new ConvertorException("Unsupported convert: [" + src + "," + destClass.getName() + "]");
                }
                Object innerValue = ((RowValue) src).getValues().get(0);
                if (innerValue == null) {
                    return null;
                }
                Convertor innerConvert = ConvertorHelper.getInstance().getConvertor(
                    innerValue.getClass(), destClass);

                if (innerConvert != null) {
                    return innerConvert.convert(innerValue, destClass);
                }
            }

            throw new ConvertorException("Unsupported convert: [" + src + "," + destClass.getName() + "]");
        }
    }
}

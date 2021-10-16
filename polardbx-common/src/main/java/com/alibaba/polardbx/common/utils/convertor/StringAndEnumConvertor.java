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


public class StringAndEnumConvertor {

    public static class StringToEnum extends AbastactConvertor {

        @Override
        public Object convert(Object src, Class destClass) {
            if (src instanceof String && destClass.isEnum()) {
                return Enum.valueOf(destClass, (String) src);
            }

            throw new ConvertorException("Unsupported convert: [" + src.getClass() + "," + destClass.getName() + "]");

        }
    }

    public static class EnumToString extends AbastactConvertor {

        @Override
        public Object convert(Object src, Class destClass) {
            if (src.getClass().isEnum() && destClass == String.class) {
                return ((Enum) src).name();
            }

            throw new ConvertorException("Unsupported convert: [" + src.getClass() + "," + destClass.getName() + "]");
        }
    }
}

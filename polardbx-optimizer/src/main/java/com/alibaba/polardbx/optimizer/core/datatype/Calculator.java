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

public interface Calculator {

    Object add(Object v1, Object v2);

    Object sub(Object v1, Object v2);

    Object multiply(Object v1, Object v2);

    Object divide(Object v1, Object v2);

    Object mod(Object v1, Object v2);

    Object and(Object v1, Object v2);

    Object or(Object v1, Object v2);

    Object xor(Object v1, Object v2);

    Object not(Object v1);

    Object bitAnd(Object v1, Object v2);

    Object bitOr(Object v1, Object v2);

    Object bitXor(Object v1, Object v2);

    Object bitNot(Object v1);
}

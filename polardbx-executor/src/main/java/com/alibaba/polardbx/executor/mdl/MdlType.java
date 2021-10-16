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

package com.alibaba.polardbx.executor.mdl;

/**
 * MDL 锁的类型，目前仅使用 MDL_EXCLUSIVE 和 MDL_SHARED_WRITE
 */
public enum MdlType {
    MDL_INTENTION_EXCLUSIVE,
    MDL_SHARED,
    MDL_SHARED_HIGH_PRIO,
    MDL_SHARED_READ,
    MDL_SHARED_WRITE,
    MDL_SHARED_UPGRADABLE,
    MDL_SHARED_READ_ONLY,
    MDL_SHARED_NO_WRITE,
    MDL_SHARED_NO_READ_WRITE,
    MDL_EXCLUSIVE
}

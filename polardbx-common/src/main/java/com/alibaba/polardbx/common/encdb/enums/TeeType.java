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

package com.alibaba.polardbx.common.encdb.enums;

public enum TeeType {
    /*
     * 服务端使用Intel SGX机制
     * Include det, rnd
     */
    IntelSGX,
    /*
     * 服务端使用Intel SGX2， RA 形式是DCAP
     */
    IntelSGX2,
    /*
     * 服务端使用FPGA机制，并且使用国密算法
     */
    FPGA_SMX,
    /*
     * 服务端使用HSM_MEK机制，并且使用国密算法.对外表现跟FPGA类似
     */
    HSM_MEK,
    /*
     * 类似于SGX Type
     */
    MOCK,
    /*
     * In this mode, only det,rnd,ore types available
     */
    NO_TEE,
}


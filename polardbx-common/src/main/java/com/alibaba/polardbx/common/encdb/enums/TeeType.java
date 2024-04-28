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


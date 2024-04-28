package com.alibaba.polardbx.common;

public interface IOrderInvariantHash {
    IOrderInvariantHash add(long x);

    Long getResult();
}

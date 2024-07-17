package com.alibaba.polardbx.qatest.ddl.cdc.entity;

import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * description:
 * author: ziyang.lb
 * create: 2023-12-11 18:55
 **/

public class DdlCheckContext {
    private final Function<String, List<DdlRecordInfo>> ddlRecordSupplier;
    private final Map<String, List<DdlRecordInfo>> ddlMarkInfos = new ConcurrentHashMap<>();
    @Getter
    private final Map<String, Object> metadata = new ConcurrentHashMap<>();
    @Getter
    @Setter
    private boolean removeImplicitTg;

    public DdlCheckContext(Function<String, List<DdlRecordInfo>> ddlRecordSupplier) {
        this.ddlRecordSupplier = ddlRecordSupplier;
    }

    public List<DdlRecordInfo> getMarkList(String key) {
        return ddlMarkInfos.computeIfAbsent(key, ddlRecordSupplier);
    }

    public List<DdlRecordInfo> updateAndGetMarkList(String key) {
        ddlMarkInfos.remove(key);
        return getMarkList(key);
    }
}

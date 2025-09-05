package com.alibaba.polardbx.optimizer.core.rel;

import com.alibaba.polardbx.common.jdbc.BytesSql;

public class LookupSql {

    public LookupSql(BytesSql bytesSql, BytesSql startSql, String orderBy, String selectNode) {
        this.bytesSql = bytesSql;
        this.startSql = startSql;
        this.orderBy = orderBy;
        this.selectNode = selectNode;
    }

    public BytesSql bytesSql;
    public BytesSql startSql;
    public String orderBy;
    public String selectNode;

}

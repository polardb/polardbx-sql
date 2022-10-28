package com.alibaba.polardbx.optimizer.core.rel.ddl;

import org.apache.calcite.rel.ddl.PushDownUdf;
import org.apache.calcite.sql.SqlCreateFunction;

public class LogicalPushDownUdf extends BaseDdlOperation{

    public LogicalPushDownUdf(PushDownUdf pushDownUdf) {
        super(pushDownUdf);
    }

    public static LogicalPushDownUdf create(PushDownUdf pushDownUdf) {
        return new LogicalPushDownUdf(pushDownUdf);
    }
}

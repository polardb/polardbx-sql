package com.alibaba.polardbx.executor.handler.ddl;

import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.apache.calcite.rel.RelNode;

public class LogicalCreateJavaFunctionHandler extends HandlerCommon {

  public LogicalCreateJavaFunctionHandler(IRepository repo) {
    super(repo);
  }

  @Override
  public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
    return null;
  }
}

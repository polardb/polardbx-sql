package com.alibaba.polardbx.optimizer.core.rel.ddl;

import org.apache.calcite.rel.ddl.CreateJavaFunction;

public class LogicalCreateJavaFunction extends BaseDdlOperation{
  public LogicalCreateJavaFunction(CreateJavaFunction createJavaFunction) {
    super(createJavaFunction.getCluster(), createJavaFunction.getTraitSet(), createJavaFunction);
  }

  public static LogicalCreateJavaFunction create(CreateJavaFunction createJavaFunction) {
    return new LogicalCreateJavaFunction(createJavaFunction);
  }
}

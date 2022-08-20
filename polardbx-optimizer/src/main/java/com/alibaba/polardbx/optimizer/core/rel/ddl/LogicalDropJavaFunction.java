package com.alibaba.polardbx.optimizer.core.rel.ddl;

import org.apache.calcite.rel.ddl.DropJavaFunction;

public class LogicalDropJavaFunction extends BaseDdlOperation{
  public LogicalDropJavaFunction(DropJavaFunction dropJavaFunction) {
    super(dropJavaFunction.getCluster(), dropJavaFunction.getTraitSet(), dropJavaFunction);
  }

  public static LogicalDropJavaFunction create(DropJavaFunction dropJavaFunction) {
    return new LogicalDropJavaFunction(dropJavaFunction);
  }
}

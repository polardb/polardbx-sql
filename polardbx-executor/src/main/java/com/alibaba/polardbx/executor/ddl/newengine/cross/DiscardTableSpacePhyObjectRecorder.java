package com.alibaba.polardbx.executor.ddl.newengine.cross;

import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.apache.calcite.rel.RelNode;

/**
 * @author luoyanxin.pt
 */
public class DiscardTableSpacePhyObjectRecorder extends GenericPhyObjectRecorder {

    /**
     * 构造函数用于初始化物理计划和执行上下文
     *
     * @param physicalPlan 物理计划节点，代表了具体的物理执行计划
     * @param executionContext 执行上下文，包含了执行物理计划所需的各种上下文信息
     */
    public DiscardTableSpacePhyObjectRecorder(RelNode physicalPlan, ExecutionContext executionContext) {
        super(physicalPlan, executionContext);
    }

    /**
     * 检查物理对象是否已经完成的方法
     *
     * 通过调用DdlHelper的方法来检查表空间是否已经被丢弃
     *
     * @return 如果表空间已经被丢弃，则返回true；否则返回false
     */
    @Override
    protected boolean checkIfPhyObjectDone() {
        return DdlHelper.checkIfPhyTableSpaceDiscard(schemaName, groupName, phyTableName);
    }

    /**
     * 通过哈希码检查物理对象是否已经完成的方法
     *
     * 和上一个方法类似，也是检查表空间是否已经被丢弃，可能会用于特定的场景或优化
     *
     * @return 如果表空间已经被丢弃，则返回true；否则返回false
     */
    @Override
    protected boolean checkIfPhyObjectDoneByHashcode() {
        return DdlHelper.checkIfPhyTableSpaceDiscard(schemaName, groupName, phyTableName);
    }


}

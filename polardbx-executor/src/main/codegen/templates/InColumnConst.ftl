<@pp.dropOutputFile />

<#list inOperators.operators as operator>
    <#list operator.types as type>

        <#assign className = "In${type.leftType}Col${type.rightType}Const${operator.operandCount}OperandsVectorizedExpression">
        <@pp.changeOutputFile name="/com/alibaba/polardbx/executor/vectorized/compare/${className}.java" />
package com.alibaba.polardbx.executor.vectorized.compare;

import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.executor.vectorized.compare.*;
import com.alibaba.polardbx.executor.vectorized.*;
import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionSignatures;
import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.*;
import com.alibaba.polardbx.executor.chunk.*;

/*
* This class is generated using freemarker and the ${.template_name} template.
*/
@SuppressWarnings("unused")
@ExpressionSignatures(
    names = {"IN"},
    argumentTypes = {
        "${type.leftType}",
        <#list 1..(operator.operandCount) as i>
            <#if i == (operator.operandCount)>
        "${type.rightType}"
            <#else>
        "${type.rightType}",
            </#if>
        </#list>
    },
    argumentKinds = {
        Variable,
        <#list 1..(operator.operandCount) as i>
            <#if i == (operator.operandCount)>
        Const
            <#else>
        Const,
            </#if>
        </#list>
    })
public class ${className} extends ${type.abstractClass} {

        public ${className}(int outputIndex, VectorizedExpression[] children) {
            super(outputIndex, children);
        }

        @Override
        public int operandCount() {
            return ${operator.operandCount};
        }
}
    </#list>
</#list>
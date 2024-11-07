package com.alibaba.polardbx.optimizer.core.rel;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlReplace;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.commons.lang.StringUtils;

public class RemoveIndexNodeVisitor extends SqlShuttle {
    private final static Logger logger = LoggerFactory.getLogger(PlannerUtils.class);

    private final TableMeta tMeta;

    public RemoveIndexNodeVisitor(TableMeta tMeta) {
        this.tMeta = tMeta;
    }

    /**
     * invalidate not exist force index
     * add alias to exist force index
     * @param call Call
     * @return new SqlDelete
     */
    @Override
    public SqlNode visit(SqlCall call) {
        SqlCall copy = SqlNode.clone(call);
        SqlKind kind = copy.getKind();
        if (kind == SqlKind.DELETE) {
            SqlDelete delete = (SqlDelete) copy;
            SqlIdentifier indexId = delete.getForceIndex();

            if (indexId != null) {
                String tablePart = indexId.names.get(0);
                String indexPart = indexId.names.size() > 1 ? indexId.names.get(1) : null;

                if (indexPart == null) {
                    IndexMeta localIndex = tMeta.findLocalIndexByName(tablePart);
                    if (localIndex == null) {
                        // if index not exists, remove sourceSelect.from.indexNode
                        ((SqlIdentifier) delete.getSourceSelect().getFrom()).indexNode = null;
                    }else{
                        // add alias to sqlDelete
                        delete.setOperand(7,delete.getSourceSelect().getFrom());
                        delete.setWithTableAlias(true);
                    }
                } else {
                    // 抛出异常报错“table with GSI should not exist here”
                    throw GeneralUtil.nestedException(
                        "table with GSI should not exist here");
                }
            }
            return delete;
        }
        return super.visit(call);
    }
}

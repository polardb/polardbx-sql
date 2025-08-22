package org.apache.calcite.rel.dal;

import com.alibaba.polardbx.common.utils.TStringUtil;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCheckTableGroup;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;

import java.util.List;

/**
 * @author luoyanxin.pt
 * @date 2025/2/22
 */
public class CheckTableGroup extends Dal {

    /**
     * Creates an <code>AbstractRelNode</code>.
     *
     */
    public CheckTableGroup(SqlCheckTableGroup checkTableGroup, RelDataType rowType, RelOptCluster cluster, RelTraitSet traitSet){
        super(checkTableGroup, rowType, cluster, traitSet);
    }

    public static CheckTableGroup create(SqlCheckTableGroup sqlCheckTableGroup, RelDataType rowType,
                                         RelOptCluster cluster) {
        return new CheckTableGroup(sqlCheckTableGroup, rowType, cluster, cluster.traitSetOf(Convention.NONE));
    }

    @Override
    public SqlKind kind(){
        return SqlKind.CHECK_TABLEGROUP;
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        pw.item(RelDrdsWriter.REL_NAME, "CHECK TABLEGROUP");
        String sql = TStringUtil.replace(getAst().toSqlString(MysqlSqlDialect.DEFAULT).toString(), System.lineSeparator(), " ");
        pw.item("sql", sql);
        return pw;
    }

    @Override
    public CheckTableGroup copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new CheckTableGroup((SqlCheckTableGroup) dal, rowType, getCluster(), traitSet);
    }
}

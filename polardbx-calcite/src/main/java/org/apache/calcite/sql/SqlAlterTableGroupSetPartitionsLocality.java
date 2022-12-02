package org.apache.calcite.sql;

import com.alibaba.polardbx.gms.locality.LocalityDesc;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.Map;

/**
 * Created by taojinkun.
 *
 * @author taojinkun
 */
public class SqlAlterTableGroupSetPartitionsLocality extends SqlAlterSpecification {

    private static final SqlOperator OPERATOR =
        new SqlSpecialOperator("SET PARTITIONS LOCALITY", SqlKind.SET_PARTITIONS_LOCALITY);

    private final SqlNode targetLocality;

    private SqlAlterTableGroup parent;

    private final SqlNode partition;

    private Boolean isLogical;

    public Boolean getLogical() {
        return isLogical;
    }

    public void setLogical(Boolean logical) {
        isLogical = logical;
    }

    public SqlAlterTableGroupSetPartitionsLocality(SqlParserPos pos, SqlNode partition, SqlNode targetLocality) {
        super(pos);
        this.targetLocality = targetLocality;
        this.partition = partition;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return null;
    }

    public void setParent(SqlAlterTableGroup parent) {
        this.parent = parent;
    }


    public String getPartition(){
        return partition.toString();
    }
    public SqlAlterTableGroup getParent() {
        return parent;
    }

    public String getTargetLocality(){
        if (targetLocality == null) {
            return "";
        }
        String localityString = targetLocality.toString();
        localityString = StringUtils.strip(localityString, "'");
        return localityString;
    }

    @Override
    public String toString(){
        String str = String.format(" set partitions %s locality='%s'", getPartition(), getTargetLocality());
        return str;
    }
}

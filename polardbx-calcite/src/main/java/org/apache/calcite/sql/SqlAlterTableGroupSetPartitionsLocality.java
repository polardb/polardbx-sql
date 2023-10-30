/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.calcite.sql;

import com.alibaba.polardbx.gms.locality.LocalityDesc;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.lang.StringUtils;

import java.util.List;

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
        localityString = unqotoaString(localityString);
        return localityString;
    }

    private String unqotoaString(String localityString){
        localityString = StringUtils.strip(localityString).toString();
        int len = localityString.length();
        if(localityString.startsWith("'") && localityString.endsWith("'")){
            localityString = localityString.substring(1, len - 1);
        }
        return localityString;
    }

    @Override
    public String toString(){
        String str = String.format(" set partitions %s locality='%s'", getPartition(), getTargetLocality());
        return str;
    }
}

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

package com.alibaba.polardbx.druid.bvt.sql.mysql.param;

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.parser.SQLParserUtils;
import com.alibaba.polardbx.druid.sql.parser.SQLStatementParser;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTOutputVisitor;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import junit.framework.TestCase;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by wenshao on 16/8/23.
 */
public class MySqlParameterizedOutputVisitorTest_54_or extends TestCase {
    public void test_for_parameterize() throws Exception {
        final DbType dbType = JdbcConstants.MYSQL;
        String sql = "SELECT r.id, r.plugin_rule_key as \"ruleKey\", r.plugin_name as \"repositoryKey\", r.description, r.description_format as \"descriptionFormat\", r.status, r.name, r.plugin_config_key as \"configKey\", r.priority as \"severity\", r.is_template as \"isTemplate\", r.language as \"language\", r.template_id as \"templateId\", r.note_data as \"noteData\", r.note_user_login as \"noteUserLogin\", r.note_created_at as \"noteCreatedAt\", r.note_updated_at as \"noteUpdatedAt\", r.remediation_function as \"remediationFunction\", r.def_remediation_function as \"defRemediationFunction\", r.remediation_gap_mult as \"remediationGapMultiplier\", r.def_remediation_gap_mult as \"defRemediationGapMultiplier\", r.remediation_base_effort as \"remediationBaseEffort\", r.def_remediation_base_effort as \"defRemediationBaseEffort\", r.gap_description as \"gapDescription\", r.tags as \"tagsField\", r.system_tags as \"systemTagsField\", r.rule_type as \"type\", r.created_at as \"createdAt\", r.updated_at as \"updatedAt\" FROM rules r WHERE (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?) or (r.plugin_name=? and r.plugin_rule_key=?)";

        SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql, dbType);
        List<SQLStatement> stmtList = parser.parseStatementList();
        SQLStatement statement = stmtList.get(0);

        StringBuilder out = new StringBuilder();
        SQLASTOutputVisitor visitor = SQLUtils.createOutputVisitor(out, JdbcConstants.MYSQL);
        List<Object> parameters = new ArrayList<Object>();
        visitor.setParameterized(true);
        visitor.setParameterizedMergeInList(true);
        visitor.setParameters(parameters);
        /*visitor.setPrettyFormat(false);*/
        statement.accept(visitor);
       /* JSONArray array = new JSONArray();
        for(String table : visitor.getTables()){
            array.add(table.replaceAll("`",""));
        }*/

        String psql = out.toString();

        System.out.println(psql);


        assertEquals("SELECT r.id, r.plugin_rule_key AS \"ruleKey\", r.plugin_name AS \"repositoryKey\", r.description, r.description_format AS \"descriptionFormat\"\n" +
                "\t, r.status, r.name, r.plugin_config_key AS \"configKey\", r.priority AS \"severity\", r.is_template AS \"isTemplate\"\n" +
                "\t, r.language AS \"language\", r.template_id AS \"templateId\", r.note_data AS \"noteData\", r.note_user_login AS \"noteUserLogin\", r.note_created_at AS \"noteCreatedAt\"\n" +
                "\t, r.note_updated_at AS \"noteUpdatedAt\", r.remediation_function AS \"remediationFunction\", r.def_remediation_function AS \"defRemediationFunction\", r.remediation_gap_mult AS \"remediationGapMultiplier\", r.def_remediation_gap_mult AS \"defRemediationGapMultiplier\"\n" +
                "\t, r.remediation_base_effort AS \"remediationBaseEffort\", r.def_remediation_base_effort AS \"defRemediationBaseEffort\", r.gap_description AS \"gapDescription\", r.tags AS \"tagsField\", r.system_tags AS \"systemTagsField\"\n" +
                "\t, r.rule_type AS \"type\", r.created_at AS \"createdAt\", r.updated_at AS \"updatedAt\"\n" +
                "FROM rules r\n" +
                "WHERE (r.plugin_name = ?\n" +
                "\t\tAND r.plugin_rule_key = ?)\n" +
                "\tOR (r.plugin_name = ?\n" +
                "\t\tAND r.plugin_rule_key = ?)", psql);
    }
}

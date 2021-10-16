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
public class MySqlParameterizedOutputVisitorTest_51_schema extends TestCase {
    public void test_for_parameterize() throws Exception {
        final DbType dbType = JdbcConstants.MYSQL;
        String sql = "UPDATE `galicdb_0102`.auction_auctions_0201 SET `starts` = ?, `pict_url` = ?, `category` = ?, `minimum_bid` = ?, `reserve_price` = ?, `city` = ?, `prov` = ?, `ends` = ?, `current_bid` = NULL, `quantity` = ?, `zoo` = ?, `secure_trade_ordinary_post_fee` = ?, `secure_trade_fast_post_fee` = ?, `old_quantity` = ?, `options` = ?, `secure_trade_ems_post_fee` = ?, `property` = ?, `last_modified` = ?, `desc_path` = ?, `postage_id` = ?, `shop_categories_id_lists` = ?, `spu_id` = ?, `sync_version` = ?, `auction_status` = ?, `features` = ?, `feature_cc` = ?, `main_color` = ?, `outer_id` = ?, `auction_sub_status` = ?, `commodity_id` = ? WHERE `auction_id` = ?";

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


        assertEquals("UPDATE galicdb.auction_auctions\n" +
                "SET `starts` = ?, `pict_url` = ?, `category` = ?, `minimum_bid` = ?, `reserve_price` = ?, `city` = ?, `prov` = ?, `ends` = ?, `current_bid` = NULL, `quantity` = ?, `zoo` = ?, `secure_trade_ordinary_post_fee` = ?, `secure_trade_fast_post_fee` = ?, `old_quantity` = ?, `options` = ?, `secure_trade_ems_post_fee` = ?, `property` = ?, `last_modified` = ?, `desc_path` = ?, `postage_id` = ?, `shop_categories_id_lists` = ?, `spu_id` = ?, `sync_version` = ?, `auction_status` = ?, `features` = ?, `feature_cc` = ?, `main_color` = ?, `outer_id` = ?, `auction_sub_status` = ?, `commodity_id` = ?\n" +
                "WHERE `auction_id` = ?", psql);
    }
}

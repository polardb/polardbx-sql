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

package com.alibaba.polardbx.druid.bvt.sql;

import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.parser.SQLParserUtils;
import com.alibaba.polardbx.druid.sql.parser.SQLSelectListCache;
import com.alibaba.polardbx.druid.sql.parser.SQLStatementParser;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import junit.framework.TestCase;

import java.util.List;

public class SQLSelectListCacheTest extends TestCase {
    public void test_selectListCache() throws Exception {
        SQLSelectListCache selectListCache = new SQLSelectListCache(JdbcConstants.MYSQL);
        selectListCache.add("SELECT `mtk_p_mg`.`id` AS `id`, `mtk_p_mg`.`gmt_create` AS `gmtCreate`, `mtk_p_mg`.`gmt_modified` AS `gmtModified`, `mtk_p_mg`.`target` AS `target`, `mtk_p_mg`.`msg_id` AS `msgId` , `mtk_p_mg`.`uuid` AS `uuid`, `mtk_p_mg`.`app_key` AS `appKey`, `mtk_p_mg`.`utdid` AS `utdid`, `mtk_p_mg`.`digest` AS `digest`, `mtk_p_mg`.`content` AS `content` , `mtk_p_mg`.`user_info` AS `userInfo`, `mtk_p_mg`.`status` AS `status`, `mtk_p_mg`.`mesg_status` AS `mesgStatus`, `mtk_p_mg`.`start_time` AS `startTime`, `mtk_p_mg`.`expired` AS `expired` , `mtk_p_mg`.`online` AS `online`, `mtk_p_mg`.`gmt_commit` AS `gmtCommit`, `mtk_p_mg`.`extra_info` AS `extraInfo`, `mtk_p_mg`.`auto_commit` AS `autoCommit`, `mtk_p_mg`.`task_id` AS `taskId` , `mtk_p_mg`.`msg_type` AS `msgType`, `mtk_p_mg`.`push_user_token` AS `pushUserToken`, `mtk_p_mg`.`tb_app_device_token` AS `tbAppDeviceToken`, `mtk_p_mg`.`sdk_version` AS `sdkVersion`, `mtk_p_mg`.`biz_ext_info` AS `bizExtInfo` FROM");

        String sql = "SELECT `mtk_p_mg`.`id` AS `id`, `mtk_p_mg`.`gmt_create` AS `gmtCreate`, `mtk_p_mg`.`gmt_modified` AS `gmtModified`, `mtk_p_mg`.`target` AS `target`, `mtk_p_mg`.`msg_id` AS `msgId` , `mtk_p_mg`.`uuid` AS `uuid`, `mtk_p_mg`.`app_key` AS `appKey`, `mtk_p_mg`.`utdid` AS `utdid`, `mtk_p_mg`.`digest` AS `digest`, `mtk_p_mg`.`content` AS `content` , `mtk_p_mg`.`user_info` AS `userInfo`, `mtk_p_mg`.`status` AS `status`, `mtk_p_mg`.`mesg_status` AS `mesgStatus`, `mtk_p_mg`.`start_time` AS `startTime`, `mtk_p_mg`.`expired` AS `expired` , `mtk_p_mg`.`online` AS `online`, `mtk_p_mg`.`gmt_commit` AS `gmtCommit`, `mtk_p_mg`.`extra_info` AS `extraInfo`, `mtk_p_mg`.`auto_commit` AS `autoCommit`, `mtk_p_mg`.`task_id` AS `taskId` , `mtk_p_mg`.`msg_type` AS `msgType`, `mtk_p_mg`.`push_user_token` AS `pushUserToken`, `mtk_p_mg`.`tb_app_device_token` AS `tbAppDeviceToken`, `mtk_p_mg`.`sdk_version` AS `sdkVersion`, `mtk_p_mg`.`biz_ext_info` AS `bizExtInfo` FROM mtk_p_mg `mtk_p_mg` FORCE INDEX (idx_targetid) WHERE `mtk_p_mg`.`target` = ? ORDER BY `mtk_p_mg`.`id` DESC LIMIT ?, ?";
        SQLStatementParser statementParser = SQLParserUtils.createSQLStatementParser(sql, JdbcConstants.MYSQL);
        statementParser.setSelectListCache(selectListCache);

        List<SQLStatement> statementList = statementParser.parseStatementList();
        assertEquals(1, statementList.size());

        SQLStatement stmt = statementList.get(0);
        assertEquals("SELECT `mtk_p_mg`.`id` AS `id`, `mtk_p_mg`.`gmt_create` AS `gmtCreate`, `mtk_p_mg`.`gmt_modified` AS `gmtModified`, `mtk_p_mg`.`target` AS `target`, `mtk_p_mg`.`msg_id` AS `msgId`\n" +
                "\t, `mtk_p_mg`.`uuid` AS `uuid`, `mtk_p_mg`.`app_key` AS `appKey`, `mtk_p_mg`.`utdid` AS `utdid`, `mtk_p_mg`.`digest` AS `digest`, `mtk_p_mg`.`content` AS `content`\n" +
                "\t, `mtk_p_mg`.`user_info` AS `userInfo`, `mtk_p_mg`.`status` AS `status`, `mtk_p_mg`.`mesg_status` AS `mesgStatus`, `mtk_p_mg`.`start_time` AS `startTime`, `mtk_p_mg`.`expired` AS `expired`\n" +
                "\t, `mtk_p_mg`.`online` AS `online`, `mtk_p_mg`.`gmt_commit` AS `gmtCommit`, `mtk_p_mg`.`extra_info` AS `extraInfo`, `mtk_p_mg`.`auto_commit` AS `autoCommit`, `mtk_p_mg`.`task_id` AS `taskId`\n" +
                "\t, `mtk_p_mg`.`msg_type` AS `msgType`, `mtk_p_mg`.`push_user_token` AS `pushUserToken`, `mtk_p_mg`.`tb_app_device_token` AS `tbAppDeviceToken`, `mtk_p_mg`.`sdk_version` AS `sdkVersion`, `mtk_p_mg`.`biz_ext_info` AS `bizExtInfo`\n" +
                "FROM mtk_p_mg `mtk_p_mg` FORCE INDEX (idx_targetid)\n" +
                "WHERE `mtk_p_mg`.`target` = ?\n" +
                "ORDER BY `mtk_p_mg`.`id` DESC\n" +
                "LIMIT ?, ?", stmt.toString());
    }
}

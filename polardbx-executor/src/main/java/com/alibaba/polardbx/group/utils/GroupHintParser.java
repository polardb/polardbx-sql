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

package com.alibaba.polardbx.group.utils;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.optimizer.parse.HintParser;

/**
 * @author <a href="junyu@taobao.com">junyu</a>
 * @version 1.0
 * @since 1.6
 */
public class GroupHintParser {

    public static final int NOT_EXIST_USER_SPECIFIED_INDEX = -1;
    public static Logger log = LoggerFactory.getLogger(GroupHintParser.class);

    public static int extraIndexFromHint(ByteString sql) {
        String groupIndexHint = extractTDDLGroupHint(sql);
        if (null != groupIndexHint && !groupIndexHint.equals("")) {
            String[] hintWithRetry = groupIndexHint.split(",");
            if (hintWithRetry.length == 1 || hintWithRetry.length == 2) {
                return GroupHintParser.getIndexFromHintPiece(hintWithRetry[0]);
            } else {
                throw new IllegalArgumentException("the standard group hint is:'groupIndex:12[,failRetry:true]'"
                    + ",current hint is:" + groupIndexHint);
            }
        } else {
            return NOT_EXIST_USER_SPECIFIED_INDEX;
        }
    }

    private static int getIndexFromHintPiece(String indexPiece) {
        String[] piece = indexPiece.split(":");
        if (piece[0].trim().equalsIgnoreCase("groupIndex")) {
            return Integer.valueOf(piece[1]);
        } else {
            throw new IllegalArgumentException(
                "the standard group hint is:'groupIndex:12[,failRetry:true]'" + ",current index hint is:" + indexPiece);
        }
    }

    private static boolean getFailRetryFromHintPiece(String retryPiece) {
        String[] piece = retryPiece.split(":");
        if (piece[0].trim().equalsIgnoreCase("failRetry")) {
            return Boolean.valueOf(piece[1]);
        } else {
            throw new IllegalArgumentException(
                "the standard group hint is:'groupIndex:12[,failRetry:true]'" + ",current retry hint is:" + retryPiece);
        }
    }

    public static String extractTDDLGroupHint(ByteString sql) {

        int i = 0;
        for (; i < sql.length(); ++i) {
            switch (sql.charAt(i)) {
            case ' ':
            case '\t':
            case '\r':
            case '\n':
                continue;
            }
            break;
        }

        if (sql.startsWith("/*+TDDL_GROUP", i)) {
            return HintParser.getInstance().getTddlGroupHint(sql);
        }
        return null;
    }

    public static ByteString removeTddlGroupHint(ByteString sql) {
        String tddlHint = extractTDDLGroupHint(sql);
        if (null == tddlHint || "".equals(tddlHint)) {
            return sql;
        }

        sql = HintParser.getInstance().removeGroupHint(sql);
        return sql;
    }

    public static String buildTddlGroupHint(String groupHint) {
        return "/*+TDDL_GROUP({" + groupHint + "})*/";
    }

}

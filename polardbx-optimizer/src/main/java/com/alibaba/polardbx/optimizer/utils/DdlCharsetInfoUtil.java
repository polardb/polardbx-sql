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

package com.alibaba.polardbx.optimizer.utils;

import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.charset.CollationName;
import com.alibaba.polardbx.common.charset.MySQLCharsetDDLValidator;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.apache.commons.lang.StringUtils;

/**
 * @author chenghui.lch
 */
public class DdlCharsetInfoUtil {

    public static DdlCharsetInfo fetchServerDefaultCharsetInfo(ExecutionContext executionContext, boolean useMySql80) {
        String serverDefaultCharsetStr = null;
        String serverDefaultCollationStr = DbTopologyManager.defaultCollationForCreatingDb;
        String sessionServerDefaultCollationStr =
            (String) executionContext.getExtraCmds()
                .get(ConnectionProperties.COLLATION_SERVER);
        if (!StringUtils.isEmpty(sessionServerDefaultCollationStr)) {
            serverDefaultCollationStr = sessionServerDefaultCollationStr;
        }

        CollationName serverDefaultCollation = CollationName.of(serverDefaultCollationStr);
        if (serverDefaultCollation == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                String.format(
                    "The server default charset collate[%s] is not supported",
                    serverDefaultCollationStr));
        }
        if (serverDefaultCollation.isMySQL80NewSupported() && !useMySql80) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                String.format(
                    "The server default charset collate[%s] is only supported for mysql 8.0",
                    serverDefaultCollationStr));
        }
        CharsetName serverDefaultCharset = CollationName.getCharsetOf(serverDefaultCollation);
        if (serverDefaultCharset == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                String.format(
                    "The server default charset collate[%s] is not supported",
                    serverDefaultCollationStr));
        }
        serverDefaultCharsetStr = serverDefaultCharset.name();
        DdlCharsetInfo serverCharInfo = new DdlCharsetInfo();
        serverCharInfo.finalCharset = serverDefaultCharsetStr;
        serverCharInfo.finalCollate = serverDefaultCollationStr;
        serverCharInfo.finalCharsetDef = serverDefaultCharset;
        serverCharInfo.finalCollateDef = serverDefaultCollation;
        return serverCharInfo;
    }

    public static DdlCharsetInfo decideDdlCharsetInfo(ExecutionContext executionContext,
                                                      String parentCharset,
                                                      String parentCollation,
                                                      String ddlSpecifyCharset,
                                                      String ddlSpecifyCollate,
                                                      boolean useMySql80) {
        boolean specifyCharset = !StringUtils.isEmpty(ddlSpecifyCharset);
        boolean specifyCollate = !StringUtils.isEmpty(ddlSpecifyCollate);
        String charset = null;
        String collate = null;
        CharsetName charsetDef;
        CollationName collateDef;

        if (specifyCharset) {
            charset = ddlSpecifyCharset;
            if (specifyCollate) {
                collate = ddlSpecifyCollate;
                charsetDef = CharsetName.of(charset);
                collateDef = CollationName.of(collate);
            } else {
                charsetDef = CharsetName.of(charset);
                collateDef = charsetDef.getDefaultCollationName();
                collate = collateDef.name();
            }
        } else {
            if (specifyCollate) {
                collate = ddlSpecifyCollate;
            } else {
                collate = parentCollation;
            }
            collateDef = CollationName.of(collate);
            charsetDef = CollationName.getCharsetOf(collateDef);
            charset = charsetDef.name();
        }
        charset = SQLUtils.normalize(charset);
        collate = SQLUtils.normalize(collate);
        if (!MySQLCharsetDDLValidator.checkCharsetSupported(charset, collate, true)) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                String.format(
                    "The specified charset[%s] or collate[%s] is not supported",
                    charset, collate));
        }
        if (MySQLCharsetDDLValidator.checkIfMySql80NewCollation(collate) && !useMySql80) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                String.format(
                    "The specified charset[%s] or collate[%s] is only supported for mysql 8.0",
                    charset, collate));
        }

        if (!MySQLCharsetDDLValidator.checkCharset(charset)) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                String.format(
                    "Unknown character set: %s",
                    charset));
        }

        if (!StringUtils.isEmpty(collate)) {
            if (!MySQLCharsetDDLValidator.checkCollation(collate)) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                    String.format(
                        "Unknown collation: %s",
                        collate));
            }

            if (!MySQLCharsetDDLValidator.checkCharsetCollation(charset, collate)) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                    String.format(
                        "Unknown character set and collation: %s %s",
                        charset, collate));
            }
        }

        DdlCharsetInfo ddlCharsetInfo = new DdlCharsetInfo();
        ddlCharsetInfo.finalCharset = charset;
        ddlCharsetInfo.finalCollate = collate;
        ddlCharsetInfo.finalCharsetDef = charsetDef;
        ddlCharsetInfo.finalCollateDef = collateDef;
        return ddlCharsetInfo;
    }

}

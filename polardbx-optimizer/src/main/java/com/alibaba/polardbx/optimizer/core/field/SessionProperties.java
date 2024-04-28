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

package com.alibaba.polardbx.optimizer.core.field;

import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.utils.timezone.InternalTimeZone;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.utils.TimestampUtils;

import java.time.ZoneId;
import java.util.Optional;

public class SessionProperties {
    private static final SessionProperties EMPTY =
        new SessionProperties(ZoneId.systemDefault(), CharsetName.defaultCharset(), 0);

    /**
     * session variables 'time_zone'
     */
    private final ZoneId timezone;

    /**
     * session variables 'character_set_connection', 'character_set_results' and 'character_set_client'
     * from statement 'set names'.
     */
    private final CharsetName sessionCharacterSet;

    /**
     * parse from session variables 'sql_mode'.
     */
    private final long sqlModeFlag;

    /**
     * Determine the check level of field storage.
     */

    private FieldCheckLevel checkLevel;

    public SessionProperties(ZoneId timezone, CharsetName sessionCharacterSet, long sqlModeFlag) {
        this(timezone, sessionCharacterSet, sqlModeFlag, FieldCheckLevel.CHECK_FIELD_IGNORE);
    }

    public SessionProperties(ZoneId timezone, CharsetName sessionCharacterSet, long sqlModeFlag,
                             FieldCheckLevel checkLevel) {
        this.timezone = timezone;
        this.sessionCharacterSet = sessionCharacterSet;
        this.sqlModeFlag = sqlModeFlag;
        this.checkLevel = checkLevel;
    }

    /**
     * TODO: It may cost some cpu time, for parser of charset name, time zone id and sql mode.
     */
    public static SessionProperties fromExecutionContext(ExecutionContext context) {
        // get time zone and character set info from session variables or default values.
        ZoneId zoneId = TimestampUtils.getZoneId(context);

        CharsetName charsetName = Optional.ofNullable(context)
            .map(ExecutionContext::getEncoding)
            .map(CharsetName::of)
            .orElseGet(() -> CharsetName.defaultCharset());

        long sqlModeFlags = Optional.ofNullable(context).map(ExecutionContext::getSqlModeFlags).orElse(0L);
        return new SessionProperties(zoneId, charsetName, sqlModeFlags);
    }

    public static SessionProperties empty() {
        return EMPTY;
    }

    public ZoneId getTimezone() {
        return timezone;
    }

    public CharsetName getSessionCharacterSet() {
        return sessionCharacterSet;
    }

    public long getSqlModeFlag() {
        return sqlModeFlag;
    }

    public FieldCheckLevel getCheckLevel() {
        return checkLevel;
    }

    public void setCheckLevel(FieldCheckLevel checkLevel) {
        this.checkLevel = checkLevel;
    }
}

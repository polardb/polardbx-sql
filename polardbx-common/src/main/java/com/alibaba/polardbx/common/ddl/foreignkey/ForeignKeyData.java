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

package com.alibaba.polardbx.common.ddl.foreignkey;

import com.alibaba.polardbx.common.utils.Pair;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

/**
 * @version 1.0
 */
@Data
public class ForeignKeyData {
    public String schema;
    public String tableName;
    public String constraint;
    public String indexName;
    public List<String> columns;
    public String refSchema;
    public String refTableName;
    public List<String> refColumns;
    public ReferenceOptionType onDelete;
    public ReferenceOptionType onUpdate;
    /**
     * 由于扩缩容问题，允许下推与非下推外键同时存在，非下推外键优先级更高
     * 0b11 = logical & physical
     * 0b10 = logical
     * 0b01 = physical
     */
    public Long pushDown = 2L;

    public static final String FOREIGN_KEY_CHECKS = "foreign_key_checks";

    public static enum ReferenceOptionType {
        RESTRICT("RESTRICT"), CASCADE("CASCADE"), SET_NULL("SET NULL"), SET_DEFAULT("SET DEFAULT"),
        NO_ACTION("NO ACTION");

        public final String name;
        public final String name_lcase;

        ReferenceOptionType(String name) {
            this.name = name;
            this.name_lcase = name.toLowerCase();
        }

        public String getText() {
            return name;
        }

        public static ReferenceOptionType fromString(String text) {
            for (ReferenceOptionType v : ReferenceOptionType.values()) {
                if (v.name.equalsIgnoreCase(text)) {
                    return v;
                }
            }
            return null;
        }
    }

    @Override
    public String toString() {
        // CONSTRAINT `fk_test_src_tbl_3_ibfk_1` FOREIGN KEY (`h`, `i`) REFERENCES `test`.`fk_test_src_tbl_2` (`d`, `e`) ON DELETE CASCADE ON UPDATE CASCADE

        List<Pair<String, String>> cols = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            cols.add(new Pair<>(columns.get(i), refColumns.get(i)));
        }

        String t = "CONSTRAINT `" + constraint + "` FOREIGN KEY (";
        for (int i = 0; i < cols.size(); i++) {
            if (i != 0) {
                t += ", `" + cols.get(i).getKey() + "`";
            } else {
                t += "`" + cols.get(i).getKey() + "`";
            }
        }
        t += ") REFERENCES `" + refSchema + "`.`" + refTableName + "` (";
        for (int i = 0; i < cols.size(); i++) {
            if (i != 0) {
                t += ", `" + cols.get(i).getValue() + "`";
            } else {
                t += "`" + cols.get(i).getValue() + "`";
            }
        }
        t += ")" + (onDelete != null ? " ON DELETE " + onDelete.getText() : "") +
            (onUpdate != null ? " ON UPDATE " + onUpdate.getText() : "");

        return t;
    }

    public String convertReferenceOption(ReferenceOptionType optionType) {
        if (optionType == null) {
            return "NO ACTION";
        } else {
            return optionType.getText();
        }
    }

    public void setPushDown(boolean pushDown) {
        if (pushDown) {
            this.pushDown = 1L;
        } else {
            this.pushDown |= 2;
        }
    }

    public void setPushDown(long pushDown) {
        this.pushDown = pushDown;
    }

    public Long getPushDown() {
        return this.pushDown;
    }

    public boolean isPushDown() {
        return (this.pushDown & 1) == 1 && !((this.pushDown & 2) == 2);
    }

    /**
     * 0 = ON DELETE/UPDATE RESTRICT
     * 1 = ON DELETE CASCADE
     * 2 = ON DELETE SET NULL
     * 4 = ON UPDATE CASCADE
     * 8 = ON UPDATE SET NULL
     * 16 = ON DELETE NO ACTION
     * 32 = ON UPDATE NO ACTION
     */
    public static long convertOption2Type(ReferenceOptionType delete, ReferenceOptionType update) {
        long type = 0;
        if (delete != null) {
            switch (delete) {
            case RESTRICT:
                break;
            case CASCADE:
                type |= 1;
                break;
            case SET_NULL:
                type |= 2;
                break;
            case NO_ACTION:
                type |= 16;
                break;
            }
        }
        if (update != null) {
            switch (update) {
            case RESTRICT:
                break;
            case CASCADE:
                type |= 4;
                break;
            case SET_NULL:
                type |= 8;
                break;
            case NO_ACTION:
                type |= 32;
                break;
            }
        }
        return type;
    }

    public void convertType2Option(long type) {
        if (type == 0) {
            this.onDelete = ReferenceOptionType.RESTRICT;
            this.onUpdate = ReferenceOptionType.RESTRICT;
        }
        if ((type & 1) == 1) {
            this.onDelete = ReferenceOptionType.CASCADE;
        }
        if ((type & 2) == 2) {
            this.onDelete = ReferenceOptionType.SET_NULL;
        }
        if ((type & 4) == 4) {
            this.onUpdate = ReferenceOptionType.CASCADE;
        }
        if ((type & 8) == 8) {
            this.onUpdate = ReferenceOptionType.SET_NULL;
        }
        if ((type & 16) == 16) {
            this.onDelete = ReferenceOptionType.NO_ACTION;
        }
        if ((type & 32) == 32) {
            this.onUpdate = ReferenceOptionType.NO_ACTION;
        }
    }

}

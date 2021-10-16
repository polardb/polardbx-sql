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

package com.alibaba.polardbx.optimizer.core.sequence.bean;

import com.alibaba.polardbx.common.constants.SequenceAttribute.Type;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.sequence.SequenceManagerProxy;

/**
 * @author chensr 2016年12月2日 上午11:56:38
 * @since 5.0.0
 */
public class SequenceFactory {

    public static CreateSequence getCreateSequence(Type type, String name, String schema) {
        CreateSequence seq;
        switch (type) {
        case SIMPLE:
            seq = new CreateSimpleSequence(name);
            break;
        case TIME:
            seq = new CreateTimeBasedSequence(name);
            break;
        case GROUP:
        default:
            seq = getCreateGroupSequence(name, schema);
            break;
        }
        return seq;
    }

    public static AlterSequence getAlterSequence(String schemaName, Type type, String name) {
        AlterSequence seq;
        switch (type) {
        case GROUP:
            seq = getAlterGroupSequence(schemaName, name);
            break;
        case TIME:
            seq = new AlterTimeBasedSequence(schemaName, name);
            break;
        case SIMPLE:
            seq = new AlterSimpleSequence(schemaName, name);
            break;
        default:
            // User doesn't specify sequence type, i.e. doesn't want to
            // change it, so we return an object with the same type.
            Type existingType = SequenceManagerProxy.getInstance().checkIfExists(schemaName, name);
            switch (existingType) {
            case TIME:
                seq = new AlterTimeBasedSequence(schemaName, name);
                break;
            case SIMPLE:
                seq = new AlterSimpleSequence(schemaName, name);
                break;
            case GROUP:
            default:
                // Specified sequence doesn't exist, so let's return
                // any object since SequenceDdlHandler will handle
                // this exception.
                seq = getAlterGroupSequence(schemaName, name);
                break;
            }
            break;
        }
        return seq;
    }

    public static DropSequence getDropSequence(String schemaName, String name, boolean onDdlSuccess) {
        DropSequence seq = null;
        Type existingType = SequenceManagerProxy.getInstance().checkIfExists(schemaName, name);
        switch (existingType) {
        case GROUP:
            seq = new DropGroupSequence(schemaName, name);
            break;
        case TIME:
            seq = new DropTimeBasedSequence(schemaName, name);
            break;
        case SIMPLE:
            seq = new DropSimpleSequence(schemaName, name);
            break;
        default:
            // Type.NA means that the sequence doesn't exist. Do nothing
            // if this is from a DROP TABLE operation, or throw an
            // exception for a standalone DROP SEQUENCE operation.
            if (!onDdlSuccess) {
                throw new TddlRuntimeException(ErrorCode.ERR_SEQUENCE,
                    "Sequence '" + name + "' doesn't exist.");
            }
        }
        return seq;
    }

    public static RenameSequence getRenameSequence(String schemaName, Pair<String, String> namePair,
                                                   boolean onDdlSuccess) {
        RenameSequence seq = null;
        // The key is original name and the value is new name.
        Type existingType = SequenceManagerProxy.getInstance().checkIfExists(schemaName, namePair.getKey());
        switch (existingType) {
        case GROUP:
            seq = new RenameGroupSequence(namePair);
            break;
        case TIME:
            seq = new RenameTimeBasedSequence(namePair);
            break;
        case SIMPLE:
            seq = new RenameSimpleSequence(namePair);
            break;
        default:
            // Type.NA means that the sequence doesn't exist. Do nothing
            // if this is from a RENAME TABLE operation, or throw an
            // exception for a standalone DROP SEQUENCE operation.
            if (!onDdlSuccess) {
                // Type.NA means that the sequence doesn't exist.
                throw new TddlRuntimeException(ErrorCode.ERR_SEQUENCE,
                    "Sequence '" + namePair.getKey() + "' doesn't exist and cannot be renamed to '"
                        + namePair.getValue() + " '.");
            }
        }
        return seq;
    }

    private static CreateSequence getCreateGroupSequence(String name, String schema) {
        if (SequenceManagerProxy.getInstance().isCustomUnitGroupSeqSupported(schema)) {
            return new CreateCustomUnitGroupSequence(name, schema);
        } else {
            return new CreateGroupSequence(name);
        }
    }

    private static AlterSequence getAlterGroupSequence(String schemaName, String name) {
        if (SequenceManagerProxy.getInstance().isCustomUnitGroupSeqSupported(schemaName)) {
            return new AlterCustomUnitGroupSequence(schemaName, name);
        } else {
            return new AlterGroupSequence(schemaName, name);
        }
    }

}

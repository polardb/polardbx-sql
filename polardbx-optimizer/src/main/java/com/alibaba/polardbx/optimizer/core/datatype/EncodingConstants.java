/*
 * Copyright 2008-2010 Amazon Technologies, Inc. or its affiliates.
 * Amazon, Amazon.com and Carbonado are trademarks or registered trademarks
 * of Amazon Technologies, Inc. or its affiliates.  All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.optimizer.core.datatype;

import java.math.BigInteger;

/**
 * @author Brian S O'Neill
 */
class EncodingConstants {

    static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    /**
     * Byte to use for null, low ordering
     */
    static final byte NULL_BYTE_LOW = 0;

    /**
     * Byte to use for null, high ordering
     */
    static final byte NULL_BYTE_HIGH = (byte) ~NULL_BYTE_LOW;

    /**
     * Byte to use for not-null, low ordering
     */
    static final byte NOT_NULL_BYTE_HIGH = (byte) 128;

    /**
     * Byte to use for not-null, high ordering
     */
    static final byte NOT_NULL_BYTE_LOW = (byte) ~NOT_NULL_BYTE_HIGH;

    /**
     * Byte to terminate variable data encoded for ascending order
     */
    static final byte TERMINATOR = (byte) 1;

    static final BigInteger ONE_HUNDRED = BigInteger.valueOf(100);
    static final BigInteger ONE_THOUSAND = BigInteger.valueOf(1000);
}

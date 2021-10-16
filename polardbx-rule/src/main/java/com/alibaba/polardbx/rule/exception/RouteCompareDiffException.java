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

package com.alibaba.polardbx.rule.exception;

import com.alibaba.polardbx.common.exception.TddlException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;

public class RouteCompareDiffException extends TddlException {

    private static final long serialVersionUID = -1050306101643415508L;

    public RouteCompareDiffException(String... params) {
        super(ErrorCode.ERR_ROUTE_COMPARE_DIFF, params);
    }

    public RouteCompareDiffException(Throwable cause, String... params) {
        super(ErrorCode.ERR_ROUTE_COMPARE_DIFF, cause, params);
    }

}

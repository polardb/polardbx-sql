package com.alibaba.polardbx.optimizer.config.table.collation;

import com.alibaba.polardbx.common.charset.CharsetHandler;
import com.alibaba.polardbx.common.charset.CollationName;
import com.alibaba.polardbx.common.collation.Utf8GeneralCiCollationHandler;

public class Utf8GeneralMySQL500CiCollationHandler extends Utf8GeneralCiCollationHandler {
    public Utf8GeneralMySQL500CiCollationHandler(
        CharsetHandler charsetHandler) {
        super(charsetHandler);
    }

    @Override
    public CollationName getName() {
        return CollationName.UTF8_GENERAL_MYSQL500_CI;
    }
}

package com.alibaba.polardbx.executor.gms;

public class ColumnarStoreUtils {
    /**
     * The default column index of `pk` (immutable).
     */
    //public static final int PK_COLUMN_INDEX = 1;
    /**
     * The default column index of `tso` (immutable).
     */
    public static final int TSO_COLUMN_INDEX = 0;
    /**
     * The default column index of `pk_tso` (immutable).
     */
    //public static final int PK_TSO_COLUMN_INDEX = 3;
    /**
     * The default column index of `position` (immutable).
     */
    public static final int POSITION_COLUMN_INDEX = 1;

    /**
     * number of implicit column BEFORE real physical column
     */
    public static final int IMPLICIT_COLUMN_CNT = 2;
}

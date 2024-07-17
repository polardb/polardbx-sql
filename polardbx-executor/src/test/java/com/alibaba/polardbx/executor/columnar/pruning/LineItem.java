package com.alibaba.polardbx.executor.columnar.pruning;

import java.sql.Date;
import java.util.Objects;

/**
 * @author fangwu
 */
public class LineItem {
    private final long rowNumber;
    private final long orderKey;
    private final long partKey;
    private final long supplierKey;
    private final int lineNumber;
    private final long quantity;
    private final long extendedPrice;
    private final long discount;
    private final long tax;
    private final String returnFlag;
    private final String status;
    private final Date shipDate;
    private final Date commitDate;
    private final Date receiptDate;
    private final String shipInstructions;
    private final String shipMode;
    private final String comment;

    public LineItem(long rowNumber, long orderKey, long partKey, long supplierKey, int lineNumber, long quantity,
                    long extendedPrice, long discount, long tax, String returnFlag, String status, Date shipDate,
                    Date commitDate, Date receiptDate, String shipInstructions, String shipMode, String comment) {
        this.rowNumber = rowNumber;
        this.orderKey = orderKey;
        this.partKey = partKey;
        this.supplierKey = supplierKey;
        this.lineNumber = lineNumber;
        this.quantity = quantity;
        this.extendedPrice = extendedPrice;
        this.discount = discount;
        this.tax = tax;
        this.returnFlag = (String) Objects.requireNonNull(returnFlag, "returnFlag is null");
        this.status = (String) Objects.requireNonNull(status, "status is null");
        this.shipDate = shipDate;
        this.commitDate = commitDate;
        this.receiptDate = receiptDate;
        this.shipInstructions = (String) Objects.requireNonNull(shipInstructions, "shipInstructions is null");
        this.shipMode = (String) Objects.requireNonNull(shipMode, "shipMode is null");
        this.comment = (String) Objects.requireNonNull(comment, "comment is null");
    }

    public long getRowNumber() {
        return this.rowNumber;
    }

    public long getOrderKey() {
        return this.orderKey;
    }

    public long getPartKey() {
        return this.partKey;
    }

    public long getSupplierKey() {
        return this.supplierKey;
    }

    public int getLineNumber() {
        return this.lineNumber;
    }

    public long getQuantity() {
        return this.quantity;
    }

    public double getExtendedPrice() {
        return (double) this.extendedPrice / 100.0D;
    }

    public long getExtendedPriceInCents() {
        return this.extendedPrice;
    }

    public double getDiscount() {
        return (double) this.discount / 100.0D;
    }

    public long getDiscountPercent() {
        return this.discount;
    }

    public double getTax() {
        return (double) this.tax / 100.0D;
    }

    public long getTaxPercent() {
        return this.tax;
    }

    public String getReturnFlag() {
        return this.returnFlag;
    }

    public String getStatus() {
        return this.status;
    }

    public Date getShipDate() {
        return this.shipDate;
    }

    public Date getCommitDate() {
        return this.commitDate;
    }

    public Date getReceiptDate() {
        return this.receiptDate;
    }

    public String getShipInstructions() {
        return this.shipInstructions;
    }

    public String getShipMode() {
        return this.shipMode;
    }

    public String getComment() {
        return this.comment;
    }
}

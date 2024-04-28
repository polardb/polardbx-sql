package com.alibaba.polardbx.transfer.utils;

/**
 * @author wuzhe
 */
public class Account implements Comparable<Account> {
    public int id;
    public long balance;
    public long version;

    public Account(int id, long balance, long version) {
        this.id = id;
        this.balance = balance;
        this.version = version;
    }

    @Override
    public int compareTo(Account other) {
        return this.id - other.id;
    }
}

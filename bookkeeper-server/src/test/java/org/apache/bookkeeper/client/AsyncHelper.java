package org.apache.bookkeeper.client;

import java.util.Enumeration;
import java.util.concurrent.atomic.AtomicInteger;

public class AsyncHelper {

    /*
    @Override
    public void addComplete(int rc, LedgerHandle lh, long entryId, Object ctx) {
        AsyncHelper.SyncObj sync = (AsyncHelper.SyncObj) ctx;
        sync.setReturnCode(rc);
        synchronized (sync) {
            sync.counter++;
            sync.notify();
        }
    }

    @Override
    public void readComplete(int rc, LedgerHandle lh, Enumeration<LedgerEntry> seq, Object ctx) {
        AsyncHelper.SyncObj sync = (AsyncHelper.SyncObj) ctx;
        sync.setLedgerEntries(seq);
        sync.setReturnCode(rc);
        synchronized (sync) {
            sync.value = true;
            sync.notify();
        }
    }

     */



    static class SyncObj {
        long lastConfirmed;
        volatile int counter;
        boolean value;
        AtomicInteger rc = new AtomicInteger(BKException.Code.OK);
        Enumeration<LedgerEntry> ls = null;

        public SyncObj() {
            counter = 0;
            lastConfirmed = LedgerHandle.INVALID_ENTRY_ID;
            value = false;
        }

        void setReturnCode(int rc) {
            this.rc.compareAndSet(BKException.Code.OK, rc);
        }

        int getReturnCode() {
            return rc.get();
        }

        void setLedgerEntries(Enumeration<LedgerEntry> ls) {
            this.ls = ls;
        }

        Enumeration<LedgerEntry> getLedgerEntries() {
            return ls;
        }
    }
}

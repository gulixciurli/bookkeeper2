package org.apache.bookkeeper.client;

import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;

import static org.junit.Assert.assertEquals;

@RunWith(value = Parameterized.class)
public class MyAsyncReadEntryTest extends BookKeeperClusterTestCase implements AsyncCallback.AddCallback, AsyncCallback.ReadCallback {

    private static int numEntries;
    private LedgerHandle lh;
    private byte[] data;

    private long firstEntry;
    private long lastEntry;
    private AsyncCallback.ReadCallback cb;   //bisogna implementare callbackinterface
    private boolean isCbValid;
    private boolean expectedRes;
    private static AsyncHelper.SyncObj sync;       //corrisponde a Object ctx, cioè un ogetto di CONTROLLO
    //SyncObj controlla che tutte le entry siano scritte nel Ledger andandole a contare


    private byte[] ledgerPassword = "pwd".getBytes();
    private final BookKeeper.DigestType digestType = BookKeeper.DigestType.CRC32;
    private long ledgerId;

    public MyAsyncReadEntryTest(boolean expectedRes, long firstEntry, long lastEntry, boolean isCbValid, AsyncHelper.SyncObj sync) {
        super(3);

        this.expectedRes = expectedRes;
        this.firstEntry = firstEntry;
        this.lastEntry = lastEntry;
        this.isCbValid = isCbValid;
        this.sync = sync;

    }

    @Before
    public void setup() {

        // creazione del ledger
        sync = new AsyncHelper.SyncObj();
        try {
            lh = bkc.createLedger(digestType, ledgerPassword);
        } catch (BKException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        ledgerId = lh.getId();
        System.out.println("\n------------------------- Ledger ID: " + lh.getId());

        //scrittura nel ledger tramite AddEntry

        data = new byte[]{'h', 'e', 'l', 'l', 'o'};

        System.out.println("\n------data lenght: " + data.length);


        lh.asyncAddEntry(data, 0, data.length, this, sync);
        lh.asyncAddEntry(data, 2, 2, this, sync);
        lh.asyncAddEntry(data, 0, 2, this, sync);



        numEntries = 3;


        // aspetta che tutte le entries siano aggiunte -> scrittura a buon fine
        synchronized (sync) {
            while (sync.counter < numEntries) {
                try {
                    sync.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

    }


    //addComplete notifica al SyncObj che la scrittura è stata completata
    @Override
    public void addComplete(int rc, LedgerHandle lh, long entryId, Object ctx) {
        AsyncHelper.SyncObj sync = (AsyncHelper.SyncObj) ctx;
        sync.setReturnCode(rc);
        synchronized (sync) {
            sync.counter++;
            sync.notify();
        }
    }


    //addComplete notifica al SyncObj che la lettura è stata completata
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


    @Parameterized.Parameters
    public static Collection<?> getTestParameters() {
        return Arrays.asList(new Object[][]{

                //FALSE se :
                //1. firstEntry<0 || firstEntry > lastEntry
                //2. lastEntry > lastAddConfirmed (last add pushed)


                //boolean expectedRes, long firstEntry, long lastEntry, boolean isCbValid, AsyncHelper.SyncObj sync


                {true, 0, numEntries, true, sync},      //false 1 if
                {false, 0, 50, true, sync},             //true 2 if
                {true, 0, 0, true, sync},               //false 2 if
                {false, -50, -2, true, sync},           //true 1 if
                {true, 0, numEntries, true, null},      //ctx non valido
                {false, 1, -1, true, sync}     //cb non valido


        });
    }

    @After
    public void tearDown() throws Exception {
        lh.close();
        super.tearDown();
    }

    @Test
    public void myTest()  {

            boolean res;

            res = true;

            if (isCbValid) {
                lh.asyncReadEntries(firstEntry, lastEntry, this, sync);

            } else {
                lh.asyncReadEntries(firstEntry, lastEntry, null, sync);
            }


        // aspetta fino all'ultima entry (lastConfirmed) -> lettura a buon fine
        synchronized (sync) {
            while (!sync.value) {
                try {
                    sync.wait();
                } catch (InterruptedException e) {
                    res = false;
                    assertEquals(expectedRes, res);
                    System.out.println("\nres= " + res + "      expectedRes= " + expectedRes);
                   // e.printStackTrace();
                }
            }

            if ( (sync.getReturnCode() ==  BKException.Code.IncorrectParameterException ) ||
                    (sync.getReturnCode() == BKException.Code.ReadException) ){

                    res = false;
            }

            assertEquals(expectedRes, res);
            System.out.println("\nres= " + res + "      expectedRes= " + expectedRes);

        }


        try {
            lh.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (BKException e) {
            e.printStackTrace();
        }

    }
}








package org.apache.bookkeeper.client;

import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;

import static org.junit.Assert.assertEquals;

@RunWith(value = Parameterized.class)
public class MyAsyncAddEntryTest extends BookKeeperClusterTestCase implements AsyncCallback.AddCallback {

    private static LedgerHandle lh;

    private byte[] data;
    private int offset;
    private int lenght;
    private AsyncCallback.AddCallback cb;   //bisogna implementare callbackinterface
    private boolean expectedRes;
    private static AsyncHelper.SyncObj sync;       //corrisponde a Object ctx, cioè un ogetto di CONTROLLO
                                            //SyncObj controlla che tutte le entry siano scritte nel Ledger andandole a contare


    private boolean isCbValid;

    public MyAsyncAddEntryTest(boolean expectedRes, byte[] data, int offset, int lenght, boolean isCbValid, AsyncHelper.SyncObj sync) {
        super(3);

        this.expectedRes=expectedRes;
        this.data=data;
        this.offset=offset;
        this.lenght=lenght;
        this.isCbValid=isCbValid;
        this.sync=sync;

    }

    @Before
    public void setup(){
        // creazione del ledger

        sync = new AsyncHelper.SyncObj();
        byte[] ledgerPassword = "pwd".getBytes();
        final BookKeeper.DigestType digestType = BookKeeper.DigestType.CRC32;

        try {
            lh = bkc.createLedger(digestType, ledgerPassword);
        } catch (BKException | InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("\n------------------------- Ledger ID: " + lh.getId());

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



    @Parameterized.Parameters
    public static Collection<?> getTestParameters() {
        byte[] data = {'h','e','l','l','o'};
        byte[] bytes2 = {'1','2','3'};

        return Arrays.asList(new Object[][] {

                /*FALSE se :
                1. offset <0
                2. lenght <0
                3. offset + lenght > len(data)
                 */

                //boolean expectedRes, byte[] data, int offset, int lenght, AsyncCallback.AddCallback cb, AsyncHelper.SyncObj sync


                {true, data, 0, data.length, true, sync},
                //{true, data, 2, data.length-2, true, sync},
                {false, data, -1, data.length, true, sync},
                {false, data, 2, -1, true, sync},
                //{false, data, -1, data.length+2, true, sync},
                {false, data, 2, 5, true, sync},
                //{false, data, 0, data.length, false, sync}, //fallisce perche non arriva notifica di completamento al SyncObj
                {true, data, 0, data.length, true, null},

        });
    }

    @After
    public void tearDown() throws Exception{

        lh.close();
        super.tearDown();
    }



    @Test
    public void myTest(){

        boolean res;

        try {
            res = true;

            if (isCbValid) {
                //System.out.println("\n--------isCbValid: "+isCbValid+"  -> this");
                lh.asyncAddEntry(data, offset, lenght, this, sync);




            }
            else{
                //System.out.println("\n--------isCbValid: "+isCbValid+"  -> null");
                lh.asyncAddEntry(data, offset, lenght, null, sync);

            }


        } catch (ArrayIndexOutOfBoundsException e) {
            res= false;
            e.printStackTrace();

        } catch (NullPointerException e){
            res= false;
            e.printStackTrace();
        }

        // aspetta che tutte le entries siano aggiunte -> scrittura a buon fine
        if (expectedRes){
            synchronized (sync) {
                while (sync.counter < 1) {

                    try {
                        sync.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                if (sync.getReturnCode()!=BKException.Code.OK){
                    res = false;
                }
            }
        }

        assertEquals(expectedRes,res);
        System.out.println("\nres= "+res+"      expectedRes= "+expectedRes);

        //se ho aggiunto un entry (quindi ho effettuato write)
        // voglio controllare che il contenuto della entry sia corretto, cioè
        // che nella entry ci sia effettivamente ciò che avevo inserito prima

        if (expectedRes){

            int numEntries = 1;
            String actualEntry, expectedEntry;

            actualEntry=null;
            expectedEntry=null;

            Enumeration<LedgerEntry> entries = null;

            try {
                entries = lh.readEntries(0, numEntries - 1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (BKException e) {
                e.printStackTrace();
            }

            while (entries.hasMoreElements()) {


                byte[] expectedTemp = null;
                byte[] entryTemp = entries.nextElement().getEntry();

                expectedTemp = Arrays.copyOfRange(data, 0, data.length);

                try {
                    actualEntry = new String(entryTemp, "UTF-8") ;
                    expectedEntry = new String(expectedTemp, "UTF-8");
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                }

                System.out.println("\n----- entry: " + actualEntry+"   expected: "+expectedEntry);
                assertEquals(actualEntry, expectedEntry);

            }


        }
    }



}
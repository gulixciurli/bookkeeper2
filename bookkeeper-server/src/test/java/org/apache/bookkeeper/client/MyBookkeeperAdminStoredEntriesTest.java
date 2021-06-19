package org.apache.bookkeeper.client;


import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.*;


//uso extends perche classe BookkeeperAdmin rappresenta l'admin client per un cluster

@RunWith(value = Parameterized.class)
public class MyBookkeeperAdminStoredEntriesTest extends BookKeeperClusterTestCase {

    private BookKeeper.DigestType digestType = BookKeeper.DigestType.CRC32;
    private static final String PASSWORD = "testPasswd";
    private static final int numBookies = 2;

    private boolean bookieSocketAddressIsValid;
    private long ledgerId;
    private boolean ledgerMetadataIsValid;
    private int invalidLedgerCase;
    private int invalidAddressCase;
    private boolean expectedResult;

    private BookieSocketAddress bookie0, bookie1, bookie2, bookie3 = null;
    List<BookieSocketAddress> ensembleOfSegment1, ensembleOfSegment2;
    private int lastEntryId;

    public MyBookkeeperAdminStoredEntriesTest(boolean expectedResult, long ledgerId, boolean bookieSocketAddressIsValid, boolean ledgerManagerIsValid, int invalidLedgerCase, int invalidAddressCase) {
        super(numBookies);
        setAutoRecoveryEnabled(true);

        this.expectedResult=expectedResult;
        this.bookieSocketAddressIsValid=bookieSocketAddressIsValid;
        this.ledgerId=ledgerId;
        this.ledgerMetadataIsValid=ledgerManagerIsValid;
        this.invalidLedgerCase = invalidLedgerCase;
        this.invalidAddressCase = invalidAddressCase;

    }

    @Before
    public void setup(){

        try {
            bookie0 = new BookieSocketAddress("bookie0:3181");
            bookie1 = new BookieSocketAddress("bookie1:3181");
            bookie2 = new BookieSocketAddress("bookie2:3181");
            bookie3 = new BookieSocketAddress("bookie3:3181");

        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        ensembleOfSegment1 = new ArrayList<BookieSocketAddress>();
        ensembleOfSegment1.add(bookie0);
        ensembleOfSegment1.add(bookie1);
        ensembleOfSegment1.add(bookie2);


        ensembleOfSegment2 = new ArrayList<BookieSocketAddress>();
        ensembleOfSegment2.add(bookie3);
        ensembleOfSegment2.add(bookie1);
        ensembleOfSegment2.add(bookie2);


    }

    @After
    public void tearDown() throws Exception{
        super.tearDown();
    }


    //boolean expectedResult, long ledgerId, boolean bookieSocketAddressIsValid,boolean ledgerMetadataIsValid, int invalidLedgerCase, int invalidAddressCase
    @Parameterized.Parameters
    public static Collection<?> getTestParameters() {
        return Arrays.asList(new Object[][] {


                {true, 100L, true, true,0,0},
                {false, 100L, true, false,1,0},
                {false, 100L, false, false,2,1},
                {false, 100L, true, false,3,0},
                {false, 100L, false, true,0,2},


                // il parametro long ledgerId effettivamente è inutilizzato nel metodo,
                // quindi lo lascio costante
        });
    }

    @Test
    public void areEntriesOfLedgerStoredInTheBookieTest() throws Exception {

        int lastEntryId =10;
        int firstEntry=0;
        int ensembleSize = 3;
        int writeQuorumSize = 3;
        int ackQuorumSize = 2;

        LedgerMetadata meta;
        BookieSocketAddress myBookie = null;

        boolean res;

        System.out.println("\n========================INIZIO PRINT========================== \n");
        System.out.println("\nledgerMetadataIsValid= "+ledgerMetadataIsValid);

        try {

            if (!ledgerMetadataIsValid) {     //ledgerMetadataIsValid = true, quindi ledgerMetadata
                //è un oggetto valido

                System.out.println("\ninvalid case : "+ invalidLedgerCase);

                if (invalidLedgerCase == 1) {
                    //caso 1: lestEntryId non è valido
                    //così first Entry > lastEntry -> deve generare eccezione

                    lastEntryId = -10;
                    firstEntry = lastEntryId + 1;
                    System.out.println("\nlastEntryId= "+lastEntryId);

                }else if (invalidLedgerCase ==2) {
                    //caso 2: le Size sono sbagliate

                    ensembleSize = 3;
                    writeQuorumSize = ensembleSize + 1;
                    ackQuorumSize = writeQuorumSize + 1;
                    System.out.println("\nensembleSize= "+ensembleSize+"    writeQuorumSize= "+writeQuorumSize+"   ackQuorumSize= "+ackQuorumSize);


                }


            }


            LedgerMetadataBuilder builder = LedgerMetadataBuilder.create();
            builder.withEnsembleSize(ensembleSize)
                    .withWriteQuorumSize(writeQuorumSize)
                    .withAckQuorumSize(ackQuorumSize)
                    .withDigestType(digestType.toApiDigestType())
                    .withPassword(PASSWORD.getBytes())
                    .newEnsembleEntry(firstEntry, ensembleOfSegment1)
                    .newEnsembleEntry(lastEntryId + 1, ensembleOfSegment2)
                    .withLastEntryId(lastEntryId).withLength(65576).withClosedState();
            meta = builder.build();

            if (!ledgerMetadataIsValid && invalidLedgerCase ==3) {
                //caso 3: ledgerMetadata = null

                meta = null;
                System.out.println("\nmeta is null");

            }


        System.out.println("\nbookieSocketAddressIsValid= "+bookieSocketAddressIsValid);

        if (bookieSocketAddressIsValid){
            myBookie = bookie2;
        }
        else{
            if (invalidAddressCase==1){
                myBookie = bookie3;
            }
            else if (invalidAddressCase==2)
            myBookie= null;

        }

        System.out.println("\nbookieSocketAddress= "+myBookie);




        System.out.println("\n----- id= "+ledgerId+"    address= "+myBookie+"    meta= "+meta);

        res = BookKeeperAdmin.areEntriesOfLedgerStoredInTheBookie(ledgerId, myBookie, meta);

        }catch (Exception e){
            res = false;
            e.printStackTrace();
        }
        System.out.println("\nres= "+res);
        assertEquals(res,expectedResult);

        System.out.println("\n========================FINE PRINT========================== \n");

    }
}

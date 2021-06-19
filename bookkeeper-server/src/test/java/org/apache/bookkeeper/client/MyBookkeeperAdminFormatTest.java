package org.apache.bookkeeper.client;


import com.google.common.util.concurrent.UncheckedExecutionException;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;


//uso extends perche classe BookkeeperAdmin rappresenta l'admin client per un cluster

@RunWith(value = Parameterized.class)
public class MyBookkeeperAdminFormatTest extends BookKeeperClusterTestCase {

    private BookKeeper.DigestType digest = BookKeeper.DigestType.CRC32;

    //format -> metodo che formatta (inizializza) i metadati dell'oggetto Bookkeeper in zook.

    private boolean isInteractive;
    private boolean force;
    private boolean serverConfIsValid;
    private boolean expectedRes;
    private int countTest;

    private static final int numOfLedgers = 2;
    private static final int numBookies = 2;




    //public MyBookkeeperAdminTest(boolean expectedRes, ServerConfiguration serverConf, boolean isInteractive, boolean force) {
    public MyBookkeeperAdminFormatTest(boolean expectedRes, boolean serverConfIsValid, boolean isInteractive, boolean force, int countTest) {

        super(numBookies);

        setAutoRecoveryEnabled(true);

        this.expectedRes=expectedRes;
        this.serverConfIsValid=serverConfIsValid;

        this.isInteractive=isInteractive;
        this.force=force;
        this.countTest=countTest;

    }


    @Parameterized.Parameters
    public static Collection<?> getTestParameters() {
        return Arrays.asList(new Object[][] {

                {true, true, false, true,0},        //expectedRes, serverConf, isInteractive, force, countTest
                {false, true, false, false,1},
                //ora non so quanto expectedRes valga, può essere TRUE O FALSE
                {true, true, true, true,2},         //expectedRes = TRUE
                {false, true, true, true,3},         //expectedRes = FALSE
                {true, true, true, false,4},         //expectedRes = TRUE
                {false, true, true, false,5},         //expectedRes = FALSE

                {false, false, false, false,6},         //serverConf = null , per ogni force e isInteractive
                // mi aspetto sempre expectedRes = FALSE


        });
    }


    //vedi pure num di ledgers già scritti che devono essere cancellati quando bookieAdmin è formattato
    //ledger id devono ripartire da 0


    @Before
    public void setup() throws InterruptedException, BKException, IOException {

        ClientConfiguration clientConf = new ClientConfiguration();
        clientConf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());

        try (BookKeeper bk = new BookKeeper(clientConf)){
            Set<Long> ledgersId = new HashSet<>();


            for (int i=0;i<numBookies;i++){

                try (LedgerHandle lh = bk.createLedger(numOfLedgers,numOfLedgers,digest,"L".getBytes())) {
                    ledgersId.add(lh.getId());
                    lh.addEntry("000".getBytes());
                }
            }

        }


    }

    @After
    public void tearDown() throws Exception{
        super.tearDown();
    }

    @Test
    public void formatTest() throws InterruptedException, BKException, IOException {
        boolean res;

        if (isInteractive){
            //deve rispondere una volta si e una no
            if ( (countTest%2)==0 ) {

                System.setIn(new ByteArrayInputStream("y\n".getBytes(),0,2));
            }
            else{
                System.setIn(new ByteArrayInputStream("n\n".getBytes(),0,2));
            }

            System.out.println("countTest= " + countTest+"    resto= "+countTest%2);
        }

        try {

            if (serverConfIsValid){
                res = BookKeeperAdmin.format( baseConf, isInteractive, force);

            }
            else{
                res = BookKeeperAdmin.format( null, isInteractive, force);

            }


        } catch( UncheckedExecutionException e){
            res = false;
            e.printStackTrace();

        } catch (Exception e) {
            res= false;
            e.printStackTrace();
        }

        System.out.println("res= " + res + "   expectedRes= " + expectedRes);
        assertEquals(expectedRes, res);


    }


}


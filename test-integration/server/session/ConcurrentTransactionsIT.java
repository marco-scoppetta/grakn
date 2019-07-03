package grakn.core.server.session;

import grakn.core.concept.Label;
import grakn.core.concept.type.SchemaConcept;
import grakn.core.rule.GraknTestServer;
import graql.lang.Graql;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static junit.framework.TestCase.assertEquals;

public class ConcurrentTransactionsIT {
    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();
    private TransactionOLTP tx;
    private SessionImpl session;

    @Before
    public void setUp() {
        session = server.sessionWithNewKeyspace();
        tx = session.transaction().write();
    }

    @After
    public void tearDown() {
        tx.close();
        session.close();
    }


    /**
     *
     *              WARNING:::::::!!!!!!!
     *
     *              PROBABLY THE READ COMMITTED ISOLATION IS COMPROSIMED BY INDEX CACHE WHICH CACHES ALL THE VERTICES FETCHED USING ANY INDEXED PROPERTY
     *              FOR EXAMPLE ATTRIBUTES OR ALMOST ANYTHING ELSE (THIS IS THE REASON WHY WE NEED ATTRIBUTE CACHE!!!!)
     *
     *
     */

    /**
     * Read Uncommitted – Read Uncommitted is the lowest isolation level.
     * In this level, one transaction may read not yet committed changes made by other transaction, thereby allowing dirty reads.
     * In this level, transactions are not isolated from each other.
     * <p>
     * In this test we:
     * - open tx1 -> wait on tx2 to be open
     * - open tx2 -> unlock tx1 -> wait for tx1 to insert a new person
     * - insert a new person without committing -> unlock tx2
     * - tx2 count how many persons are in the graph
     * The count should be equal to 0
     * <p>
     * We guarantee that uncommitted data is not visible to other open transactions.
     */
    @Test
    public void transactionsIsolation_ReadUncommitted() throws ExecutionException, InterruptedException {
        tx.execute(Graql.parse("define " +
                "person sub entity;").asDefine());
        tx.commit();


        CountDownLatch latch1 = new CountDownLatch(1);
        CountDownLatch latch2 = new CountDownLatch(1);
        CountDownLatch latch3 = new CountDownLatch(1);

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        Future<?> future1 = executorService.submit(() -> {
            TransactionOLTP tx1 = session.transaction().write();
            try {
                latch1.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            tx1.execute(Graql.parse("insert $x isa person;").asInsert());
            latch2.countDown();
            try {
                latch3.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            tx1.close();
        });

        Future<?> future2 = executorService.submit(() -> {
            TransactionOLTP tx2 = session.transaction().write();
            latch1.countDown();
            try {
                latch2.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            int personNum = tx2.execute(Graql.parse("match $x isa person; get; count;").asGetAggregate()).get(0).number().intValue();
            tx2.close();
            assertEquals(0, personNum);
            latch3.countDown();
        });
        future1.get();
        future2.get();
    }

    /**
     * Read Committed: This isolation level guarantees that any data read is committed at the moment it is read.
     * Thus it does not allows dirty read.
     * The transaction holds a read or write lock on the current row, and thus prevent other transactions from reading, updating or deleting it.
     * <p>
     * In this test we:
     * - open tx1 -> insert a new person -> wait on tx2
     * - open tx2 -> unlock tx1 -> wait on tx1 to commit
     * - tx1 commits -> unlock tx2
     * - tx2 count how many persons are in the graph
     * The count should be equal to 1
     * <p>
     * We guarantee that all readable data is already committed to the graph.
     * <p>
     * There is no isolation on committed data,
     * all the transactions that were already open can immediately see committed data.
     */
    @Test
    public void transactionsIsolation_ReadCommitted() throws ExecutionException, InterruptedException {
        tx.execute(Graql.parse("define " +
                "person sub entity;").asDefine());
        tx.commit();


        CountDownLatch commitLatch = new CountDownLatch(1);
        CountDownLatch insertLatch = new CountDownLatch(1);
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        Future<?> future1 = executorService.submit(() -> {
            TransactionOLTP tx1 = session.transaction().write();
            tx1.execute(Graql.parse("insert $x isa person;").asInsert());
            try {
                commitLatch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            tx1.commit();
            insertLatch.countDown();
        });

        Future<?> future2 = executorService.submit(() -> {
            TransactionOLTP tx2 = session.transaction().write();
//            int personNumBefore = tx2.execute(Graql.parse("match $x isa person; get; count;").asGetAggregate()).get(0).number().intValue();
//            System.out.println(personNumBefore);
            commitLatch.countDown();
            try {
                insertLatch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            int personNum = tx2.execute(Graql.parse("match $x isa person; get; count;").asGetAggregate()).get(0).number().intValue();
            tx2.close();
            assertEquals(1, personNum);
        });
        future1.get();
        future2.get();
    }

    /**
     * Given that we are at isolation level: Read Committed
     * This test shows how transactions are susceptible to Non Repeatable read.
     * Non Repeatable read occurs when a transaction reads same row twice, and get a different value each time
     */
    @Test
    public void concurrentTransactions_areSusceptibleToNonRepeatableReads() throws ExecutionException, InterruptedException {
        tx.execute(Graql.parse("define " +
                "person sub entity, has name; " +
                "name sub attribute, datatype string;").asDefine());
        tx.commit();


        CountDownLatch commitLatch = new CountDownLatch(1);
        CountDownLatch insertLatch = new CountDownLatch(1);
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        Future<?> future1 = executorService.submit(() -> {
            TransactionOLTP tx1 = session.transaction().write();
            tx1.execute(Graql.parse("insert $x isa person, has name 'Aretha';").asInsert());
            try {
                commitLatch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            tx1.commit();
            insertLatch.countDown();
        });

        Future<?> future2 = executorService.submit(() -> {
            TransactionOLTP tx2 = session.transaction().write();
            int aretha = tx2.getAttributesByValue("Aretha").size();
            System.out.println("aretha before commit " + aretha);
            commitLatch.countDown();
            try {
                insertLatch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            int aretha2 = tx2.getAttributesByValue("Aretha").size();
            System.out.println("aretha after commit " + aretha2);
            // When executing the following statement we should see an entry for Aretha in the attributeMap of
            // Session, but it won't be usable in this session, due to Txs isolation.
            tx2.execute(Graql.parse("insert $x isa person, has name 'Aretha';").asInsert());
            tx2.commit();
        });
        future1.get();
        future2.get();
    }
}

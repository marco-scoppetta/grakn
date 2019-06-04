/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package grakn.core.server.session;

import grakn.core.concept.Label;
import grakn.core.concept.type.SchemaConcept;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.exception.SessionException;
import grakn.core.server.exception.TransactionException;
import grakn.core.server.keyspace.KeyspaceImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class SessionIT {
    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    private SessionImpl session;
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setUp() {
        session = server.sessionWithNewKeyspace();
    }

    @After
    public void closeSession() {
        session.close();
    }

    /**
     * When requesting 2 transactions from the same SessionImpl we expect to receive
     * 2 different objects
     */
    @Test
    public void sessionProducesDifferentTransactionObjects() {
        TransactionOLTP tx1 = session.transaction().write();
        tx1.close();
        TransactionOLTP tx2 = session.transaction().write();
        assertNotEquals(tx1, tx2);
    }

    /**
     * It is not possible to have multiple transactions per thread.
     */
    @Test
    public void tryingToOpenTwoWriteTransactionsInSameThread_throwsException() {
        TransactionOLTP tx1 = session.transaction().write();
        expectedException.expect(TransactionException.class);
        expectedException.expectMessage("A transaction is already open on this thread for graph [" + session.keyspace() + "]. Close the current transaction before opening a new one in the same thread.");
        TransactionOLTP tx2 = session.transaction().write();
    }

    @Test
    public void tryingToOpenTwoReadTransactionsInSameThread_throwsException() {
        TransactionOLTP tx1 = session.transaction().read();
        expectedException.expect(TransactionException.class);
        expectedException.expectMessage("A transaction is already open on this thread for graph [" + session.keyspace() + "]. Close the current transaction before opening a new one in the same thread.");
        TransactionOLTP tx2 = session.transaction().read();
    }


    /**
     * Transactions are thread bound, it's not possible to share the same transaction between multiple threads
     */
    @Test
    public void sharingSameTransactionInDifferentThread_transactionIsNotUsable() throws InterruptedException {
        TransactionOLTP tx1 = session.transaction().write();
        ExecutorService executor = Executors.newSingleThreadExecutor();

        try {
            executor.submit(() -> {
                SchemaConcept concept = tx1.getSchemaConcept(Label.of("thing"));
            }).get();
        } catch (ExecutionException e) {
            Throwable transactionException = e.getCause();
            assertThat(transactionException, instanceOf(TransactionException.class));
            assertEquals("The transaction for keyspace [" + session.keyspace() + "] is closed. Use the session to get a new transaction for the graph.", transactionException.getMessage());
        }
    }

    /**
     * A session can be shared between multiple threads so that each thread can use its own local transaction.
     */
    @Test
    public void sessionOpeningTransactionsInDifferentThreads_transactionsAreUsable() throws ExecutionException, InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        executor.submit(() -> {
            TransactionOLTP tx1 = session.transaction().write();
            SchemaConcept concept = tx1.getSchemaConcept(Label.of("thing"));
            assertEquals("thing", concept.label().toString());
            tx1.close();
        }).get();
        executor.submit(() -> {
            TransactionOLTP tx1 = session.transaction().write();
            SchemaConcept concept = tx1.getSchemaConcept(Label.of("thing"));
            assertEquals("thing", concept.label().toString());
            tx1.close();
        }).get();
    }


    /**
     * Using different sessions in different threads it should be possible to access the same keyspace.
     */
    @Test
    public void sessionsInDifferentThreadsShouldBeAbleToAccessSameKeyspace() throws ExecutionException, InterruptedException {
        TransactionOLTP tx1 = session.transaction().write();
        tx1.putEntityType("person");
        tx1.commit();
        ExecutorService executor = Executors.newFixedThreadPool(2);

        executor.submit(() -> {
            SessionImpl localSession = server.sessionFactory().session(session.keyspace());
            TransactionOLTP tx2 = localSession.transaction().write();
            SchemaConcept concept = tx2.getSchemaConcept(Label.of("person"));
            assertEquals("person", concept.label().toString());
            tx2.close();
            localSession.close();
        }).get();

        executor.submit(() -> {
            SessionImpl localSession = server.sessionFactory().session(session.keyspace());
            TransactionOLTP tx2 = localSession.transaction().write();
            SchemaConcept concept = tx2.getSchemaConcept(Label.of("person"));
            assertEquals("person", concept.label().toString());
            tx2.close();
            localSession.close();
        }).get();
    }


    @Test
    public void whenClosingSession_transactionIsAlsoClosed() {
        SessionImpl localSession = server.sessionFactory().session(KeyspaceImpl.of("test"));
        TransactionOLTP tx1 = localSession.transaction().write();
        assertFalse(tx1.isClosed());
        localSession.close();
        assertTrue(tx1.isClosed());
    }

    @Test
    public void whenClosingSession_tryingToUseTransactionThrowsException() {
        SessionImpl localSession = server.sessionFactory().session(KeyspaceImpl.of("test"));
        TransactionOLTP tx1 = localSession.transaction().write();
        assertFalse(tx1.isClosed());
        localSession.close();
        expectedException.expect(TransactionException.class);
        expectedException.expectMessage("The session for graph [test] is closed. Create a new session to interact with the graph.");
        SchemaConcept thing = tx1.getSchemaConcept(Label.of("thing"));
    }

    /**
     * Once a session it's closed it should not be possible to use it to get new transactions.
     */
    @Test
    public void whenSessionIsClosed_itIsNotPossibleToCreateNewTransactions(){
        session.close();
        expectedException.expect(SessionException.class);
        expectedException.expectMessage("The session for graph [" + session.keyspace() + "] is closed. Create a new session to interact with the graph.");
        TransactionOLTP tx1 = session.transaction().write();

        SchemaConcept concept = tx1.getSchemaConcept(Label.of("thing"));
        assertEquals("thing", concept.label().toString());
    }
}

/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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

package grakn.core.graph.graphdb.database.idassigner;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import grakn.core.graph.core.JanusGraphException;
import grakn.core.graph.diskstorage.BackendException;
import grakn.core.graph.diskstorage.IDAuthority;
import grakn.core.graph.diskstorage.IDBlock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


public class StandardIDPool implements IDPool {
    private static final Logger LOG = LoggerFactory.getLogger(StandardIDPool.class);
    private static final int RENEW_ID_COUNT = 100;


    private static final IDBlock ID_POOL_EXHAUSTION = new IDBlock() {
        @Override
        public long numIds() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getId(long index) {
            throw new UnsupportedOperationException();
        }
    };

    private static final IDBlock UNINITIALIZED_BLOCK = new IDBlock() {
        @Override
        public long numIds() {
            return 0;
        }

        @Override
        public long getId(long index) {
            throw new ArrayIndexOutOfBoundsException(0);
        }
    };

    private final IDAuthority idAuthority;
    private final long idUpperBound; //exclusive
    private final int partition;
    private final int idNamespace;

    private final Duration renewTimeout;
    private final double renewBufferPercentage;

    private IDBlock currentBlock;
    private long currentIndex;
    private long renewBlockIndex;

    private volatile IDBlock nextBlock;
    private Future<IDBlock> idBlockFuture;
    private IDBlockGetter idBlockGetter;
    private final ThreadPoolExecutor exec;

    private volatile boolean closed;

    private final Queue<Future<?>> closeBlockers;

    public StandardIDPool(IDAuthority idAuthority, int partition, int idNamespace, long idUpperBound, Duration renewTimeout, double renewBufferPercentage) {
        Preconditions.checkArgument(idUpperBound > 0);
        this.idAuthority = idAuthority;
        Preconditions.checkArgument(partition >= 0);
        this.partition = partition;
        Preconditions.checkArgument(idNamespace >= 0);
        this.idNamespace = idNamespace;
        this.idUpperBound = idUpperBound;
        Preconditions.checkArgument(!renewTimeout.isZero(), "Renew-timeout must be positive");
        this.renewTimeout = renewTimeout;
        Preconditions.checkArgument(renewBufferPercentage > 0.0 && renewBufferPercentage <= 1.0, "Renew-buffer percentage must be in (0.0,1.0]");
        this.renewBufferPercentage = renewBufferPercentage;

        currentBlock = UNINITIALIZED_BLOCK;
        currentIndex = 0;
        renewBlockIndex = 0;
        nextBlock = null;

        // daemon=true would probably be fine too
        exec = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(), new ThreadFactoryBuilder()
                .setDaemon(false)
                .setNameFormat("JanusGraphID(" + partition + ")(" + idNamespace + ")[%d]")
                .build());
        idBlockFuture = null;
        closeBlockers = new ArrayDeque<>(4);
        closed = false;
    }

    private synchronized void waitForIDBlockGetter() throws InterruptedException {
        Stopwatch sw = Stopwatch.createStarted();
        if (null != idBlockFuture) {
            try {
                nextBlock = idBlockFuture.get(renewTimeout.toMillis(), TimeUnit.MILLISECONDS);
            } catch (ExecutionException e) {
                String msg = String.format("ID block allocation on partition(%d)-namespace(%d) failed with an exception in %s", partition, idNamespace, sw.stop());
                throw new JanusGraphException(msg, e);
            } catch (TimeoutException e) {
                String msg = String.format("ID block allocation on partition(%d)-namespace(%d) timed out in %s", partition, idNamespace, sw.stop());
                // Attempt to cancel the renewer
                idBlockGetter.stopRequested();
                if (idAuthority.supportsInterruption()) {
                    idBlockFuture.cancel(true);
                } else {
                    // Attempt to clean one dead element out of closeBlockers every time we append to it
                    if (!closeBlockers.isEmpty()) {
                        Future<?> f = closeBlockers.peek();
                        if (null != f && f.isDone())
                            closeBlockers.remove();
                    }
                    closeBlockers.add(idBlockFuture);
                }
                throw new JanusGraphException(msg, e);
            } catch (CancellationException e) {
                String msg = String.format("ID block allocation on partition(%d)-namespace(%d) was cancelled after %s", partition, idNamespace, sw.stop());
                throw new JanusGraphException(msg, e);
            } finally {
                idBlockFuture = null;
            }
            // Allow InterruptedException to propagate up the stack
        }
    }

    private synchronized void nextBlock() throws InterruptedException {
        Preconditions.checkState(!closed, "ID Pool has been closed for partition(%s)-namespace(%s) - cannot apply for new id block", partition, idNamespace);

        if (null == nextBlock && null == idBlockFuture) {
            startIDBlockGetter();
        }

        if (null == nextBlock) {
            waitForIDBlockGetter();
        }

        if (nextBlock == ID_POOL_EXHAUSTION) {
            throw new IDPoolExhaustedException("Exhausted ID Pool for partition(" + partition + ")-namespace(" + idNamespace + ")");
        }

        currentBlock = nextBlock;
        currentIndex = 0;

        LOG.debug("ID partition({})-namespace({}) acquired block: [{}]", partition, idNamespace, currentBlock);

        nextBlock = null;
        renewBlockIndex = Math.max(0, currentBlock.numIds() - Math.max(RENEW_ID_COUNT, Math.round(currentBlock.numIds() * renewBufferPercentage)));
    }

    @Override
    public synchronized long nextID() {
        if (currentIndex == currentBlock.numIds()) {
            try {
                nextBlock();
            } catch (InterruptedException e) {
                throw new JanusGraphException("Could not renew id block due to interruption", e);
            }
        }

        if (currentIndex == renewBlockIndex) {
            startIDBlockGetter();
        }

        long returnId = currentBlock.getId(currentIndex);
        currentIndex++;
        if (returnId >= idUpperBound) throw new IDPoolExhaustedException("Reached id upper bound of " + idUpperBound);
        LOG.trace("partition({})-namespace({}) Returned id: {}", partition, idNamespace, returnId);
        return returnId;
    }

    @Override
    public synchronized void close() {
        closed = true;
        try {
            waitForIDBlockGetter();
        } catch (InterruptedException e) {
            throw new JanusGraphException("Interrupted while waiting for id renewer thread to finish", e);
        }

        for (Future<?> closeBlocker : closeBlockers) {
            try {
                closeBlocker.get();
            } catch (InterruptedException e) {
                throw new JanusGraphException("Interrupted while waiting for runaway ID renewer task " + closeBlocker, e);
            } catch (ExecutionException e) {
                LOG.debug("Runaway ID renewer task completed with exception", e);
            }
        }
        exec.shutdownNow();
    }

    private synchronized void startIDBlockGetter() {
        Preconditions.checkArgument(idBlockFuture == null, idBlockFuture);
        if (closed) return; //Don't renew anymore if closed
        //Renew buffer
        LOG.debug("Starting id block renewal thread upon {}", currentIndex);
        idBlockGetter = new IDBlockGetter(idAuthority, partition, idNamespace, renewTimeout);
        idBlockFuture = exec.submit(idBlockGetter);
    }

    private static class IDBlockGetter implements Callable<IDBlock> {
        private final Stopwatch alive;
        private final IDAuthority idAuthority;
        private final int partition;
        private final int idNamespace;
        private final Duration renewTimeout;
        private volatile boolean stopRequested;

        IDBlockGetter(IDAuthority idAuthority, int partition, int idNamespace, Duration renewTimeout) {
            this.idAuthority = idAuthority;
            this.partition = partition;
            this.idNamespace = idNamespace;
            this.renewTimeout = renewTimeout;
            this.alive = Stopwatch.createStarted();
        }

        private void stopRequested() {
            this.stopRequested = true;
        }

        @Override
        public IDBlock call() {
            Stopwatch running = Stopwatch.createStarted();

            try {
                if (stopRequested) {
                    LOG.debug("Aborting ID block retrieval on partition({})-namespace({}) after graceful shutdown was requested, exec time {}, exec+q time {}",
                            partition, idNamespace, running.stop(), alive.stop());
                    throw new JanusGraphException("ID block retrieval aborted by caller");
                }
                IDBlock idBlock = idAuthority.getIDBlock(partition, idNamespace, renewTimeout);
                LOG.debug("Retrieved ID block from authority on partition({})-namespace({}), " +
                                "exec time {}, exec+q time {}",
                        partition, idNamespace, running.stop(), alive.stop());
                Preconditions.checkArgument(idBlock != null && idBlock.numIds() > 0);
                return idBlock;
            } catch (BackendException e) {
                throw new JanusGraphException("Could not acquire new ID block from storage", e);
            } catch (IDPoolExhaustedException e) {
                return ID_POOL_EXHAUSTION;
            }
        }
    }
}
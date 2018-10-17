/*
 * Copyright (C) 2014 Per Lundqvist
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.github.java.rsync.internal.session;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.github.java.rsync.RsyncException;
import com.github.java.rsync.internal.channels.ChannelException;

public final class RsyncTaskExecutor {
    private static final Logger LOG = Logger.getLogger(RsyncTaskExecutor.class.getName());
    
    public static void throwUnwrappedException(Throwable thrown) throws InterruptedException, RsyncException {
        Throwable cause;
        if (thrown instanceof ExecutionException) {
            cause = thrown.getCause();
        } else {
            cause = thrown;
        }
        
        if (cause instanceof InterruptedException) {
            throw (InterruptedException) cause;
        } else if (cause instanceof RsyncException) {
            throw (RsyncException) cause;
        } else if (cause instanceof RuntimeException) {
            // e.g. CancellationException
            throw (RuntimeException) cause;
        } else if (cause instanceof Error) {
            throw (Error) cause;
        }
        throw new AssertionError("BUG - missing statement for " + cause);
    }
    
    private final Executor executor;
    
    public RsyncTaskExecutor(Executor executor) {
        assert executor != null;
        this.executor = executor;
    }
    
    public boolean exec(RsyncTask... tasks) throws RsyncException, InterruptedException {
        CompletionService<Boolean> ecs = new ExecutorCompletionService<>(this.executor);
        
        List<Future<Boolean>> futures = new LinkedList<>();
        for (RsyncTask task : tasks) {
            futures.add(ecs.submit(task));
        }
        
        Throwable thrown = null;
        boolean isOK = true;
        
        for (int i = 0; i < futures.size(); i++) {
            try {
                if (LOG.isLoggable(Level.FINER)) {
                    LOG.finer(String.format("waiting for result from task " + "%d/%d", i + 1, futures.size()));
                }
                // take throws InterruptedException,
                // get throws CancellationException
                boolean isThreadOK = ecs.take().get();
                isOK = isOK && isThreadOK;
                if (LOG.isLoggable(Level.FINER)) {
                    LOG.finer(String.format("task %d/%d finished %s", i + 1, futures.size(), isThreadOK ? "OK" : "ERROR"));
                }
            } catch (Throwable t) {
                if (LOG.isLoggable(Level.FINER)) {
                    LOG.finer(String.format("deferring exception raised by task %d/%d: %s", i + 1, futures.size(), t));
                }
                if (thrown == null) {
                    thrown = t;
                    for (Future<Boolean> future : futures) {
                        future.cancel(true);
                    }
                    for (RsyncTask task : tasks) {
                        if (!task.isInterruptible()) {
                            try {
                                task.closeChannel();
                            } catch (ChannelException e) {
                                t.addSuppressed(e);
                            }
                        }
                    }
                } else {
                    thrown.addSuppressed(t);
                }
            }
        }
        
        boolean isException = thrown != null;
        if (isException) {
            throwUnwrappedException(thrown);
        }
        
        if (LOG.isLoggable(Level.FINE)) {
            LOG.fine("exit " + (isOK ? "OK" : "ERROR"));
        }
        
        return isOK;
    }
}

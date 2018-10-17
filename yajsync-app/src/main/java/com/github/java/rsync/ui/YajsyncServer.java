/*
 * A simple rsync command line server implementation
 *
 * Copyright (C) 1996-2011 by Andrew Tridgell, Wayne Davison, and others
 * Copyright (C) 2013-2015 Per Lundqvist
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
package com.github.java.rsync.ui;

import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.UnsupportedCharsetException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.github.java.rsync.RsyncServer;
import com.github.java.rsync.internal.channels.ChannelException;
import com.github.java.rsync.internal.util.ArgumentParser;
import com.github.java.rsync.internal.util.ArgumentParsingError;
import com.github.java.rsync.internal.util.Environment;
import com.github.java.rsync.internal.util.Option;
import com.github.java.rsync.internal.util.Util;
import com.github.java.rsync.net.DuplexByteChannel;
import com.github.java.rsync.net.SSLServerChannelFactory;
import com.github.java.rsync.net.ServerChannel;
import com.github.java.rsync.net.ServerChannelFactory;
import com.github.java.rsync.net.StandardServerChannelFactory;
import com.github.java.rsync.server.module.ModuleException;
import com.github.java.rsync.server.module.ModuleProvider;
import com.github.java.rsync.server.module.Modules;

public final class YajsyncServer {
    private static final Logger LOG = Logger.getLogger(YajsyncServer.class.getName());
    private static final int THREAD_FACTOR = 4;
    private InetAddress address = InetAddress.getLoopbackAddress();
    private PrintStream err = System.err;
    private CountDownLatch listeningLatch;
    private ModuleProvider moduleProvider = ModuleProvider.getDefault();
    private int numThreads = Runtime.getRuntime().availableProcessors() * THREAD_FACTOR;
    private PrintStream out = System.out;
    private int port = RsyncServer.DEFAULT_LISTEN_PORT;
    private final RsyncServer.Builder serverBuilder = new RsyncServer.Builder();
    private int timeout = 0;
    private boolean useTLS;
    private int verbosity = 100;

    public YajsyncServer() {
    }

    private Callable<Boolean> createCallable(final RsyncServer server, final DuplexByteChannel sock, final boolean isInterruptible) {
        return new Callable<Boolean>() {
            @Override
            public Boolean call() {
                boolean isOK = false;
                try {
                    Modules modules;
                    if (sock.getPeerPrincipal().isPresent()) {
                        if (LOG.isLoggable(Level.FINE)) {
                            LOG.fine(String.format("%s connected from %s", sock.getPeerPrincipal().get(), sock.getPeerAddress()));
                        }
                        modules = YajsyncServer.this.moduleProvider.newAuthenticated(sock.getPeerAddress(), sock.getPeerPrincipal().get());
                    } else {
                        if (LOG.isLoggable(Level.FINE)) {
                            LOG.fine("got anonymous connection from " + sock.getPeerAddress());
                        }
                        modules = YajsyncServer.this.moduleProvider.newAnonymous(sock.getPeerAddress());
                    }
                    isOK = server.serve(modules, sock, sock, isInterruptible);
                } catch (ModuleException e) {
                    if (LOG.isLoggable(Level.SEVERE)) {
                        LOG.severe(String.format("Error: failed to initialise modules for " + "principal %s using ModuleProvider %s: %s%n", sock.getPeerPrincipal().get(),
                                YajsyncServer.this.moduleProvider, e));
                    }
                } catch (ChannelException e) {
                    if (LOG.isLoggable(Level.SEVERE)) {
                        LOG.severe("Error: communication closed with peer: " + e.getMessage());
                    }
                } catch (Throwable t) {
                    if (LOG.isLoggable(Level.SEVERE)) {
                        LOG.log(Level.SEVERE, "", t);
                    }
                } finally {
                    try {
                        sock.close();
                    } catch (IOException e) {
                        if (LOG.isLoggable(Level.SEVERE)) {
                            LOG.severe(String.format("Got error during close of socket %s: %s", sock, e.getMessage()));
                        }
                    }
                }

                if (LOG.isLoggable(Level.FINE)) {
                    LOG.fine("Thread exit status: " + (isOK ? "OK" : "ERROR"));
                }
                return isOK;
            }
        };
    }

    private Iterable<Option> options() {
        List<Option> options = new LinkedList<>();
        options.add(Option.newStringOption(Option.Policy.OPTIONAL, "charset", "", "which charset to use (default UTF-8)", option -> {
            String charsetName = (String) option.getValue();
            try {
                Charset charset = Charset.forName(charsetName);
                this.serverBuilder.charset(charset);
                return ArgumentParser.Status.CONTINUE;
            } catch (IllegalCharsetNameException | UnsupportedCharsetException e) {
                throw new ArgumentParsingError(String.format("failed to set character set to %s: %s", charsetName, e));
            }
        }));

        options.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "verbose", "v", String.format("output verbosity (default %d)", this.verbosity), option -> {
            this.verbosity++;
            return ArgumentParser.Status.CONTINUE;
        }));

        options.add(Option.newStringOption(Option.Policy.OPTIONAL, "address", "", String.format("address to bind to (default %s)", this.address), option -> {
            try {
                String name = (String) option.getValue();
                this.address = InetAddress.getByName(name);
                return ArgumentParser.Status.CONTINUE;
            } catch (UnknownHostException e) {
                throw new ArgumentParsingError(e);
            }
        }));

        options.add(Option.newIntegerOption(Option.Policy.OPTIONAL, "port", "", String.format("port number to listen on (default %d)", this.port), option -> {
            this.port = (int) option.getValue();
            return ArgumentParser.Status.CONTINUE;
        }));

        options.add(Option.newIntegerOption(Option.Policy.OPTIONAL, "threads", "", String.format("size of thread pool (default %d)", this.numThreads), option -> {
            this.numThreads = (int) option.getValue();
            return ArgumentParser.Status.CONTINUE;
        }));

        String deferredWriteHelp = "receiver defers writing into target tempfile as long as " + "possible to reduce I/O, at the cost of highly increased risk "
                + "of the file being modified by a process already having it " + "opened (default false)";

        options.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "defer-write", "", deferredWriteHelp, option -> {
            this.serverBuilder.isDeferWrite(true);
            return ArgumentParser.Status.CONTINUE;
        }));

        options.add(Option.newIntegerOption(Option.Policy.OPTIONAL, "timeout", "", "set I/O timeout in seconds", option -> {
            int timeout = (int) option.getValue();
            if (timeout < 0) {
                throw new ArgumentParsingError(String.format("invalid timeout %d - must be " + "greater than or equal to 0", timeout));
            }
            this.timeout = timeout * 1000;
            // Timeout socket operations depend on
            // ByteBuffer.array and ByteBuffer.arrayOffset.
            // Disable direct allocation if the resulting
            // ByteBuffer won't have an array.
            if (timeout > 0 && !Environment.hasAllocateDirectArray()) {
                Environment.setAllocateDirect(false);
            }
            return ArgumentParser.Status.CONTINUE;
        }));

        options.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "tls", "", String.format("tunnel all data over TLS/SSL " + "(default %s)", this.useTLS), option -> {
            this.useTLS = true;
            // SSLChannel.read and SSLChannel.write depends on
            // ByteBuffer.array and ByteBuffer.arrayOffset.
            // Disable direct allocation if the resulting
            // ByteBuffer won't have an array.
            if (!Environment.hasAllocateDirectArray()) {
                Environment.setAllocateDirect(false);
            }
            return ArgumentParser.Status.CONTINUE;
        }));

        return options;
    }

    public YajsyncServer setIsListeningLatch(CountDownLatch isListeningLatch) {
        this.listeningLatch = isListeningLatch;
        return this;
    }

    public void setModuleProvider(ModuleProvider moduleProvider) {
        this.moduleProvider = moduleProvider;
    }

    public YajsyncServer setStandardErr(PrintStream err) {
        this.err = err;
        return this;
    }

    public YajsyncServer setStandardOut(PrintStream out) {
        this.out = out;
        return this;
    }

    public int start(String[] args) throws IOException, InterruptedException {
        ArgumentParser argsParser = ArgumentParser.newNoUnnamed(this.getClass().getSimpleName());
        try {
            argsParser.addHelpTextDestination(this.out);
            for (Option o : this.options()) {
                argsParser.add(o);
            }
            for (Option o : this.moduleProvider.options()) {
                argsParser.add(o);
            }
            ArgumentParser.Status rc = argsParser.parse(Arrays.asList(args)); // throws ArgumentParsingError
            if (rc != ArgumentParser.Status.CONTINUE) {
                return rc == ArgumentParser.Status.EXIT_OK ? 0 : 1;
            }
        } catch (ArgumentParsingError e) {
            this.err.println(e.getMessage());
            this.err.println(argsParser.toUsageString());
            return -1;
        }

        Level logLevel = Util.getLogLevelForNumber(Util.WARNING_LOG_LEVEL_NUM + this.verbosity);
        Util.setRootLogLevel(logLevel);

        ServerChannelFactory socketFactory = this.useTLS ? new SSLServerChannelFactory().setWantClientAuth(true) : new StandardServerChannelFactory();

        socketFactory.setReuseAddress(true);
        // socketFactory.setKeepAlive(true);
        boolean isInterruptible = !this.useTLS;
        ExecutorService executor = Executors.newFixedThreadPool(this.numThreads);
        RsyncServer server = this.serverBuilder.build(executor);

        try (ServerChannel listenSock = socketFactory.open(this.address, this.port, this.timeout)) { // throws IOException
            if (this.listeningLatch != null) {
                this.listeningLatch.countDown();
            }
            while (true) {
                System.err.println("Got connection....");
                DuplexByteChannel sock = listenSock.accept(); // throws IOException
                Callable<Boolean> c = this.createCallable(server, sock, isInterruptible);
                executor.submit(c); // NOTE: result discarded
            }
        } finally {
            if (LOG.isLoggable(Level.INFO)) {
                LOG.info("shutting down...");
            }
            executor.shutdown();
            this.moduleProvider.close();
            while (!executor.awaitTermination(5, TimeUnit.MINUTES)) {
                LOG.info("some sessions are still running, waiting for them " + "to finish before exiting");
            }
            if (LOG.isLoggable(Level.INFO)) {
                LOG.info("done");
            }
        }
    }
}

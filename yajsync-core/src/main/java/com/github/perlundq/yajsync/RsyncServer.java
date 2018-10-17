/*
 * Rsync server -> client session creation
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
package com.github.perlundq.yajsync;

import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.UnsupportedCharsetException;
import java.util.concurrent.ExecutorService;

import com.github.perlundq.yajsync.internal.session.FilterMode;
import com.github.perlundq.yajsync.internal.session.Generator;
import com.github.perlundq.yajsync.internal.session.Receiver;
import com.github.perlundq.yajsync.internal.session.RsyncTaskExecutor;
import com.github.perlundq.yajsync.internal.session.Sender;
import com.github.perlundq.yajsync.internal.session.ServerSessionConfig;
import com.github.perlundq.yajsync.internal.session.SessionStatus;
import com.github.perlundq.yajsync.internal.text.Text;
import com.github.perlundq.yajsync.internal.util.Util;
import com.github.perlundq.yajsync.server.module.Modules;

public class RsyncServer {
    public static class Builder {
        private Charset charset = Charset.forName(Text.UTF8_NAME);
        private ExecutorService executorService;
        private boolean deferWrite;
        
        public RsyncServer build(ExecutorService executorService) {
            assert executorService != null;
            this.executorService = executorService;
            return new RsyncServer(this);
        }
        
        /**
         *
         * @throws UnsupportedCharsetException if charset is not supported
         */
        public Builder charset(Charset charset) {
            assert charset != null;
            Util.validateCharset(charset);
            this.charset = charset;
            return this;
        }
        
        public Builder isDeferWrite(boolean isDeferWrite) {
            this.deferWrite = isDeferWrite;
            return this;
        }
    }
    
    public static final int DEFAULT_LISTEN_PORT = 2873;
    
    private final Charset charset;
    private final boolean deferWrite;
    private final RsyncTaskExecutor rsyncTaskExecutor;
    
    private RsyncServer(Builder builder) {
        this.deferWrite = builder.deferWrite;
        this.charset = builder.charset;
        this.rsyncTaskExecutor = new RsyncTaskExecutor(builder.executorService);
    }
    
    public boolean serve(Modules modules, ReadableByteChannel in, WritableByteChannel out, boolean isChannelsInterruptible) throws RsyncException, InterruptedException {
        assert modules != null;
        assert in != null;
        assert out != null;
        // throws IllegalArgumentException if charset is not supported
        ServerSessionConfig cfg = ServerSessionConfig.handshake(this.charset, in, out, modules);
        if (cfg.getStatus() == SessionStatus.ERROR) {
            return false;
        } else if (cfg.getStatus() == SessionStatus.EXIT) {
            return true;
        }
        
        if (cfg.isSender()) {
            Sender sender = Sender.Builder.newServer(in, out, cfg.getSourceFiles(), cfg.getChecksumSeed())//
                    .filterMode(FilterMode.RECEIVE)//
                    .charset(cfg.getCharset())//
                    .fileSelection(cfg.fileSelection())//
                    .isPreserveDevices(cfg.isPreserveDevices())//
                    .isPreserveSpecials(cfg.isPreserveSpecials())//
                    .isPreserveLinks(cfg.isPreserveLinks())//
                    .isPreserveUser(cfg.isPreserveUser())//
                    .isPreserveGroup(cfg.isPreserveGroup())//
                    .isNumericIds(cfg.isNumericIds())//
                    .isInterruptible(isChannelsInterruptible)//
                    .isSafeFileList(cfg.isSafeFileList())//
                    .build();
            return this.rsyncTaskExecutor.exec(sender);
        } else {
            Generator generator = new Generator.Builder(out, cfg.getChecksumSeed())//
                    .charset(cfg.getCharset())//
                    .fileSelection(cfg.fileSelection())//
                    .isDelete(cfg.isDelete())//
                    .isPreserveDevices(cfg.isPreserveDevices())//
                    .isPreserveSpecials(cfg.isPreserveSpecials())//
                    .isPreserveLinks(cfg.isPreserveLinks())//
                    .isPreservePermissions(cfg.isPreservePermissions())//
                    .isPreserveTimes(cfg.isPreserveTimes()).isPreserveUser(cfg.isPreserveUser())//
                    .isPreserveGroup(cfg.isPreserveGroup())//
                    .isNumericIds(cfg.isNumericIds())//
                    .isIgnoreTimes(cfg.isIgnoreTimes())//
                    .isAlwaysItemize(cfg.getVerbosity() > 1)//
                    .isInterruptible(isChannelsInterruptible)//
                    .build();
            
            Receiver receiver = Receiver.Builder.newServer(generator, in, cfg.getReceiverDestination())//
                    .filterMode(cfg.isDelete() ? FilterMode.RECEIVE : FilterMode.NONE)//
                    .isDeferWrite(this.deferWrite)//
                    .isSafeFileList(cfg.isSafeFileList())//
                    .build();
            return this.rsyncTaskExecutor.exec(generator, receiver);
        }
    }
}

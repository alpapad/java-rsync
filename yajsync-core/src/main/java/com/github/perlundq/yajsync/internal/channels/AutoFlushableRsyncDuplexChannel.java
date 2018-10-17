/*
 * Automatically flush output channel for read operations
 *
 * Copyright (C) 2013, 2014 Per Lundqvist
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
package com.github.perlundq.yajsync.internal.channels;

public class AutoFlushableRsyncDuplexChannel extends AutoFlushableDuplexChannel implements Taggable, IndexDecoder, IndexEncoder {
    private final RsyncInChannel inChannel;
    private final RsyncOutChannel outChannel;
    
    public AutoFlushableRsyncDuplexChannel(RsyncInChannel inChannel, RsyncOutChannel outChannel) {
        super(inChannel, outChannel);
        this.inChannel = inChannel;
        this.outChannel = outChannel;
    }
    
    public void close() throws ChannelException {
        try {
            this.inChannel.close();
        } finally {
            this.outChannel.close();
        }
    }
    
    @Override
    public int decodeIndex() throws ChannelException {
        this.flush();
        return this.inChannel.decodeIndex();
    }
    
    @Override
    public void encodeIndex(int index) throws ChannelException {
        this.outChannel.encodeIndex(index);
    }
    
    @Override
    public void flush() throws ChannelException {
        if (this.inChannel.getNumBytesAvailable() == 0) {
            super.flush();
        }
    }
    
    public int numBytesAvailable() {
        return this.inChannel.getNumBytesAvailable();
    }
    
    public long numBytesRead() {
        return this.inChannel.getNumBytesRead();
    }
    
    public long numBytesWritten() {
        return this.outChannel.getNumBytesWritten();
    }
    
    @Override
    public void putMessage(Message message) throws ChannelException {
        this.outChannel.putMessage(message);
    }
}

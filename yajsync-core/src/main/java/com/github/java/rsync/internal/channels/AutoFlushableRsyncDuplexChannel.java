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
package com.github.java.rsync.internal.channels;

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
            inChannel.close();
        } finally {
            outChannel.close();
        }
    }

    @Override
    public int decodeIndex() throws ChannelException {
        flush();
        return inChannel.decodeIndex();
    }

    @Override
    public void encodeIndex(int index) throws ChannelException {
        outChannel.encodeIndex(index);
    }

    @Override
    public void flush() throws ChannelException {
        if (inChannel.getNumBytesAvailable() == 0) {
            super.flush();
        }
    }

    public int numBytesAvailable() {
        return inChannel.getNumBytesAvailable();
    }

    public long numBytesRead() {
        return inChannel.getNumBytesRead();
    }

    public long numBytesWritten() {
        return outChannel.getNumBytesWritten();
    }

    @Override
    public void putMessage(Message message) throws ChannelException {
        outChannel.putMessage(message);
    }
}

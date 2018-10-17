/*
 * Rsync Channel with support for handling tagged rsync Messages sent
 * from peer
 *
 * Copyright (C) 1996-2011 by Andrew Tridgell, Wayne Davison, and others
 * Copyright (C) 2013, 2014, 2016 Per Lundqvist
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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.github.java.rsync.RsyncProtocolException;
import com.github.java.rsync.internal.text.Text;
import com.github.java.rsync.internal.util.Util;

public class TaggedInputChannel extends SimpleInputChannel {
    private static final Logger LOG = Logger.getLogger(TaggedInputChannel.class.getName());
    
    private final SimpleInputChannel inputChannel;
    private final MessageHandler msgHandler;
    private int readAmountAvailable = 0;
    
    public TaggedInputChannel(ReadableByteChannel sock, MessageHandler handler) {
        super(sock);
        this.inputChannel = new SimpleInputChannel(sock);
        this.msgHandler = handler;
    }
    
    @Override
    protected void get(ByteBuffer dst) throws ChannelException {
        while (dst.hasRemaining()) {
            this.readNextAvailable(dst);
        }
    }
    
    public int getNumBytesAvailable() {
        return this.readAmountAvailable;
    }
    
    @Override
    public long getNumBytesRead() {
        return super.getNumBytesRead() + this.inputChannel.getNumBytesRead();
    }
    
    /**
     * @throws RsyncProtocolException if peer sends an invalid message
     */
    protected void readNextAvailable(ByteBuffer dst) throws ChannelException {
        while (this.readAmountAvailable == 0) {
            this.readAmountAvailable = this.readNextMessage();
        }
        int chunkLength = Math.min(this.readAmountAvailable, dst.remaining());
        ByteBuffer slice = Util.slice(dst, dst.position(), dst.position() + chunkLength);
        super.get(slice);
        if (LOG.isLoggable(Level.FINEST)) {
            ByteBuffer tmp = Util.slice(dst, dst.position(), dst.position() + Math.min(chunkLength, 64));
            LOG.finest(Text.byteBufferToString(tmp));
        }
        dst.position(slice.position());
        this.readAmountAvailable -= chunkLength;
    }
    
    private int readNextMessage() throws ChannelException {
        try {
            // throws IllegalArgumentException
            MessageHeader hdr = MessageHeader.fromTag(this.inputChannel.getInt());
            if (hdr.getMessageType() == MessageCode.DATA) {
                if (LOG.isLoggable(Level.FINER)) {
                    LOG.finer("< " + hdr);
                }
                return hdr.getLength();
            }
            ByteBuffer payload = this.inputChannel.get(hdr.getLength()).order(ByteOrder.LITTLE_ENDIAN);
            // throws IllegalArgumentException, IllegalStateException
            Message message = new Message(hdr, payload);
            if (LOG.isLoggable(Level.FINER)) {
                LOG.finer("< " + message);
            }
            this.msgHandler.handleMessage(message);
            return 0;
        } catch (RsyncProtocolException | IllegalStateException | IllegalArgumentException e) {
            throw new ChannelException(e);
        }
    }
}

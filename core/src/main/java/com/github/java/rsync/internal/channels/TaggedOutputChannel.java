/*
 * Rsync Channel with support for sending tagged rsync messages to
 * peer
 *
 * Copyright (C) 1996-2011 by Andrew Tridgell, Wayne Davison, and others
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

import java.nio.channels.WritableByteChannel;

import com.github.java.rsync.internal.util.Consts;

public class TaggedOutputChannel extends BufferedOutputChannel implements Taggable {
    private static final int DEFAULT_TAG_OFFSET = 0;
    private static final int TAG_SIZE = Consts.SIZE_INT;
    private int tagOffset;

    public TaggedOutputChannel(WritableByteChannel sock) {
        super(sock);
        updateTagOffsetAndBufPos(DEFAULT_TAG_OFFSET);
    }

    public TaggedOutputChannel(WritableByteChannel sock, int bufferSize) {
        super(sock, bufferSize);
        updateTagOffsetAndBufPos(DEFAULT_TAG_OFFSET);
    }

    @Override
    public void flush() throws ChannelException {
        if (getNumBytesBuffered() > 0) {
            if (getNumBytesUntagged() > 0) {
                tagCurrentData();
            } else {
                // reset buffer position
                assert buffer.position() == tagOffset + TAG_SIZE;
                buffer.position(buffer.position() - TAG_SIZE);
            }
            super.flush();
            updateTagOffsetAndBufPos(DEFAULT_TAG_OFFSET);
        }
    }

    @Override
    public int getNumBytesBuffered() {
        return buffer.position() - TAG_SIZE;
    }

    private int getNumBytesUntagged() {
        int dataStartOffset = tagOffset + TAG_SIZE;
        int numBytesUntagged = buffer.position() - dataStartOffset;
        assert numBytesUntagged >= 0;
        return numBytesUntagged;
    }

    @Override
    public void putMessage(Message message) throws ChannelException {
        assert message.getHeader().getLength() == message.getPayload().remaining();

        int numBytesRequired = message.getHeader().getLength() + TAG_SIZE;
        int minMessageSize = TAG_SIZE + 1;

        if (numBytesRequired + minMessageSize > buffer.remaining()) {
            flush();
        } else if (getNumBytesUntagged() > 0) {
            tagCurrentData();
            updateTagOffsetAndBufPos(buffer.position());
        }

        putMessageHeader(tagOffset, message.getHeader());
        assert buffer.remaining() >= message.getPayload().remaining();
        this.put(message.getPayload());
        updateTagOffsetAndBufPos(buffer.position());
    }

    private void putMessageHeader(int offset, MessageHeader header) {
        buffer.putInt(offset, header.toTag());
    }

    private void tagCurrentData() {
        putMessageHeader(tagOffset, new MessageHeader(MessageCode.DATA, getNumBytesUntagged()));
    }

    private void updateTagOffsetAndBufPos(int position) {
        assert position >= 0 && position < buffer.limit() - TAG_SIZE;
        tagOffset = position;
        buffer.position(tagOffset + TAG_SIZE);
    }
}

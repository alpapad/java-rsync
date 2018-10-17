/*
 * Rsync channel integration tests
 *
 * Copyright (C) 2016 Per Lundqvist
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
package com.github.java.rsync.channels;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.LinkedList;
import java.util.Queue;

import org.junit.Before;
import org.junit.Test;

import com.github.java.rsync.internal.channels.ChannelEOFException;
import com.github.java.rsync.internal.channels.ChannelException;
import com.github.java.rsync.internal.channels.Message;
import com.github.java.rsync.internal.channels.MessageCode;
import com.github.java.rsync.internal.channels.MessageHandler;
import com.github.java.rsync.internal.channels.RsyncInChannel;
import com.github.java.rsync.internal.channels.RsyncOutChannel;
import com.github.java.rsync.internal.util.Environment;

public class ChannelTest implements MessageHandler {
    private static Message toMessage(MessageCode code, String text) {
        ByteBuffer payload = ByteBuffer.wrap(text.getBytes());
        Message msg = new Message(code, payload);
        return msg;
    }
    
    private final Queue<Message> messages = new LinkedList<>();
    
    @Override
    public void handleMessage(Message message) {
        this.messages.add(message);
    }
    
    @Before
    public void initializeTest() {
        this.messages.clear();
    }
    
    @Test(expected = ChannelEOFException.class)
    public void testEOFRead() throws ChannelException {
        ByteBuffer b = ByteBuffer.allocate(0);
        ReadableByteChannel r = new ReadableByteBufferChannel(b);
        RsyncInChannel in = new RsyncInChannel(r, this);
        in.getByte();
    }
    
    @Test
    public void testManyMessagesManyData() throws ChannelException {
        ByteBuffer wb = ByteBuffer.allocate(1024);
        WritableByteBufferChannel w = new WritableByteBufferChannel(wb);
        RsyncOutChannel _out = new RsyncOutChannel(w);
        Message[] msgs = { toMessage(MessageCode.INFO, "INFO message"), toMessage(MessageCode.WARNING, "WARNING message"), toMessage(MessageCode.ERROR, "ERROR message"),
                toMessage(MessageCode.ERROR_XFER, "XFER message"), new Message(MessageCode.NO_SEND, ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(0, 127)),
                new Message(MessageCode.IO_ERROR, ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(0, 31)), };
        
        byte b = 0;
        for (Message msg : msgs) {
            msg.getPayload().mark();
            _out.putMessage(msg);
            _out.putByte(b++);
            msg.getPayload().reset();
        }
        _out.flush();
        
        wb.flip();
        ReadableByteChannel r = new ReadableByteBufferChannel(wb);
        RsyncInChannel _in = new RsyncInChannel(r, this);
        
        b = 0;
        for (Message msg : msgs) {
            byte result_b = _in.getByte();
            assertTrue(result_b == b++);
            Message resultMessage = this.messages.poll();
            assertEquals(msg, resultMessage);
        }
        assertTrue(this.messages.isEmpty());
    }
    
    @Test
    public void testManyMessagesNoData() throws ChannelException {
        ByteBuffer wb = ByteBuffer.allocate(1024);
        WritableByteBufferChannel w = new WritableByteBufferChannel(wb);
        RsyncOutChannel _out = new RsyncOutChannel(w);
        Message[] msgs = { toMessage(MessageCode.INFO, "INFO message"), toMessage(MessageCode.WARNING, "WARNING message"), toMessage(MessageCode.ERROR, "ERROR message"),
                toMessage(MessageCode.ERROR_XFER, "XFER message"), new Message(MessageCode.NO_SEND, ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(0, 127)),
                new Message(MessageCode.IO_ERROR, ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(0, 31)), };
        for (Message msg : msgs) {
            msg.getPayload().mark();
            _out.putMessage(msg);
            msg.getPayload().reset();
        }
        _out.flush();
        
        wb.flip();
        ReadableByteChannel r = new ReadableByteBufferChannel(wb);
        RsyncInChannel _in = new RsyncInChannel(r, this);
        
        try {
            _in.getByte();
            fail();
        } catch (ChannelEOFException e) {
            // expected
        }
        
        for (Message msg : msgs) {
            Message resultMessage = this.messages.poll();
            assertEquals(msg, resultMessage);
        }
        assertTrue(this.messages.isEmpty());
    }
    
    @Test
    public void testSingleMessageAndSingleData() throws ChannelException {
        ByteBuffer wb = ByteBuffer.allocate(128);
        WritableByteBufferChannel w = new WritableByteBufferChannel(wb);
        RsyncOutChannel _out = new RsyncOutChannel(w);
        Message testMessage = toMessage(MessageCode.INFO, "test message");
        testMessage.getPayload().mark();
        int testInt = 4;
        _out.putInt(testInt);
        _out.putMessage(testMessage);
        testMessage.getPayload().reset();
        _out.flush();
        
        wb.flip();
        ReadableByteChannel r = new ReadableByteBufferChannel(wb);
        RsyncInChannel _in = new RsyncInChannel(r, this);
        
        int resultInt = _in.getInt();
        assertTrue(resultInt == testInt);
        try {
            _in.getByte();
            fail();
        } catch (ChannelEOFException e) {
            // expected
        }
        Message resultMessage = this.messages.poll();
        assertEquals(testMessage, resultMessage);
        assertTrue(this.messages.isEmpty());
    }
    
    @Test
    public void testSingleMessageNoData() throws ChannelException {
        ByteBuffer wb = ByteBuffer.allocate(128);
        WritableByteBufferChannel w = new WritableByteBufferChannel(wb);
        RsyncOutChannel _out = new RsyncOutChannel(w);
        Message testMessage = toMessage(MessageCode.INFO, "test message");
        testMessage.getPayload().mark();
        _out.putMessage(testMessage);
        testMessage.getPayload().reset();
        _out.flush();
        
        wb.flip();
        ReadableByteChannel r = new ReadableByteBufferChannel(wb);
        RsyncInChannel _in = new RsyncInChannel(r, this);
        
        try {
            _in.getByte();
            fail();
        } catch (ChannelEOFException e) {
            // expected
        }
        Message resultMessage = this.messages.poll();
        assertEquals(testMessage, resultMessage);
        assertTrue(this.messages.isEmpty());
    }
    
    @Test
    public void testTransferSingleMaxByte() throws ChannelException {
        ByteBuffer wb = ByteBuffer.allocate(8);
        WritableByteBufferChannel w = new WritableByteBufferChannel(wb);
        RsyncOutChannel _out = new RsyncOutChannel(w);
        
        byte testByte = Byte.MAX_VALUE;
        _out.putByte(testByte);
        _out.flush();
        assertTrue(wb.position() > 0);
        
        wb.flip();
        ReadableByteChannel r = new ReadableByteBufferChannel(wb);
        RsyncInChannel _in = new RsyncInChannel(r, this);
        
        byte resultByte = _in.getByte();
        assertTrue(wb.position() > 0);
        assertTrue(resultByte == testByte);
    }
    
    @Test
    public void testTransferSingleMaxInt() throws ChannelException {
        ByteBuffer wb = ByteBuffer.allocate(8);
        WritableByteBufferChannel w = new WritableByteBufferChannel(wb);
        RsyncOutChannel _out = new RsyncOutChannel(w);
        
        int testInt = Integer.MAX_VALUE;
        _out.putInt(testInt);
        _out.flush();
        assertTrue(wb.position() > 0);
        
        wb.flip();
        ReadableByteChannel r = new ReadableByteBufferChannel(wb);
        RsyncInChannel _in = new RsyncInChannel(r, this);
        
        int resultInt = _in.getInt();
        assertTrue(wb.position() > 0);
        assertTrue(resultInt == testInt);
    }
    
    @Test
    public void testTransferSingleMinByte() throws ChannelException {
        ByteBuffer wb = ByteBuffer.allocate(8);
        WritableByteBufferChannel w = new WritableByteBufferChannel(wb);
        RsyncOutChannel _out = new RsyncOutChannel(w);
        
        byte testByte = Byte.MIN_VALUE;
        _out.putByte(testByte);
        _out.flush();
        assertTrue(wb.position() > 0);
        
        wb.flip();
        ReadableByteChannel r = new ReadableByteBufferChannel(wb);
        RsyncInChannel _in = new RsyncInChannel(r, this);
        
        byte resultByte = _in.getByte();
        assertTrue(wb.position() > 0);
        assertTrue(resultByte == testByte);
    }
    
    @Test
    public void testTransferSingleMinInt() throws ChannelException {
        ByteBuffer wb = ByteBuffer.allocate(8);
        WritableByteBufferChannel w = new WritableByteBufferChannel(wb);
        RsyncOutChannel _out = new RsyncOutChannel(w);
        
        int testInt = Integer.MIN_VALUE;
        _out.putInt(testInt);
        _out.flush();
        assertTrue(wb.position() > 0);
        
        wb.flip();
        ReadableByteChannel r = new ReadableByteBufferChannel(wb);
        RsyncInChannel _in = new RsyncInChannel(r, this);
        
        int resultInt = _in.getInt();
        assertTrue(wb.position() > 0);
        assertTrue(resultInt == testInt);
    }
    
    @Test
    public void testTransferString() throws ChannelException {
        Environment.setAllocateDirect(false);
        
        ByteBuffer wb = ByteBuffer.allocate(32);
        WritableByteBufferChannel w = new WritableByteBufferChannel(wb);
        RsyncOutChannel _out = new RsyncOutChannel(w);
        
        String testString = "abcdefghijklm ö\\";
        _out.put(ByteBuffer.wrap(testString.getBytes()));
        _out.flush();
        assertTrue(wb.position() > 0);
        
        wb.flip();
        ReadableByteChannel r = new ReadableByteBufferChannel(wb);
        RsyncInChannel _in = new RsyncInChannel(r, this);
        
        ByteBuffer res = _in.get(testString.getBytes().length);
        assertTrue(wb.position() > 0);
        assertTrue(res.position() == 0);
        
        String resultString = new String(res.array(), 0, res.remaining());
        assertEquals(testString, resultString);
    }
}

class ReadableByteBufferChannel implements ReadableByteChannel {
    private final ByteBuffer _buf;
    private boolean _isOpen = true;
    
    ReadableByteBufferChannel(ByteBuffer buf) {
        this._buf = buf;
    }
    
    @Override
    public void close() {
        this._isOpen = false;
    }
    
    @Override
    public boolean isOpen() {
        return this._isOpen;
    }
    
    @Override
    public int read(ByteBuffer dst) {
        int n = 0;
        while (dst.hasRemaining()) {
            if (this._buf.hasRemaining()) {
                byte b = this._buf.get();
                dst.put(b);
                n++;
            } else if (n == 0) {
                return -1;
            } else {
                return n;
            }
        }
        return n;
    }
}

class WritableByteBufferChannel implements WritableByteChannel {
    private final ByteBuffer _buf;
    private boolean _isOpen = true;
    
    WritableByteBufferChannel(ByteBuffer buf) {
        this._buf = buf;
    }
    
    @Override
    public void close() {
        this._isOpen = false;
    }
    
    @Override
    public boolean isOpen() {
        return this._isOpen;
    }
    
    @Override
    public int write(ByteBuffer src) {
        int n = 0;
        while (src.hasRemaining()) {
            if (this._buf.hasRemaining()) {
                byte b = src.get();
                this._buf.put(b);
                n++;
            } else if (n == 0) {
                return -1;
            } else {
                return n;
            }
        }
        return n;
    }
}

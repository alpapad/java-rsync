/*
 * Character decoding
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
package com.github.java.rsync.internal.text;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;

import com.github.java.rsync.internal.util.Consts;
import com.github.java.rsync.internal.util.ErrorPolicy;
import com.github.java.rsync.internal.util.MemoryPolicy;
import com.github.java.rsync.internal.util.OverflowException;
import com.github.java.rsync.internal.util.Util;

public class TextDecoder {
    public static TextDecoder newFallback(Charset charset) {
        CharsetDecoder encoder = charset.newDecoder().onMalformedInput(CodingErrorAction.REPLACE).onUnmappableCharacter(CodingErrorAction.REPLACE);
        TextDecoder instance = new TextDecoder(encoder);
        return instance;
    }

    public static TextDecoder newStrict(Charset charset) {
        CharsetDecoder encoder = charset.newDecoder().onMalformedInput(CodingErrorAction.REPORT).onUnmappableCharacter(CodingErrorAction.REPORT);
        TextDecoder instance = new TextDecoder(encoder);
        return instance;
    }

    private final CharsetDecoder decoder;

    private TextDecoder(CharsetDecoder decoder) {
        this.decoder = decoder;
    }

    /**
     * @throws TextConversionException
     */
    private String _decode(ByteBuffer input, ErrorPolicy errorPolicy, MemoryPolicy memoryPolicy) {
        decoder.reset();
        CharBuffer output = CharBuffer.allocate((int) Math.ceil(input.capacity() * decoder.averageCharsPerByte()));
        try {
            CoderResult result;
            while (true) {
                result = decoder.decode(input, output, true);
                if (result.isOverflow()) {
                    output = Util.enlargeCharBuffer(output, memoryPolicy, Consts.MAX_BUF_SIZE / 2); // throws OverflowException
                } else {
                    break;
                }
            }

            while (!result.isError()) {
                result = decoder.flush(output);
                if (result.isOverflow()) {
                    output = Util.enlargeCharBuffer(output, memoryPolicy, Consts.MAX_BUF_SIZE / 2); // throws OverflowException
                } else {
                    break;
                }
            }

            if (result.isUnderflow()) {
                return output.flip().toString();
            }

            if (errorPolicy == ErrorPolicy.THROW) {
                input.limit(input.position() + result.length());
                throw new TextConversionException(
                        String.format("failed to decode %d bytes after %s (using %s): %s -> %s", result.length(), output.flip().toString(), decoder.charset(), Text.byteBufferToString(input), result));
            }
            return null;
        } catch (OverflowException e) {
            if (errorPolicy == ErrorPolicy.THROW) {
                throw new TextConversionException(e);
            }
            return null;
        } finally {
            if (memoryPolicy == MemoryPolicy.ZERO) {
                Util.zeroCharBuffer(output);
            }
        }
    }

    public Charset charset() {
        return decoder.charset();
    }

    /**
     * @throws TextConversionException
     */
    public String decode(ByteBuffer input) {
        return _decode(input, ErrorPolicy.THROW, MemoryPolicy.IGNORE);
    }

    public String decodeOrNull(byte[] bytes) {
        ByteBuffer input = ByteBuffer.wrap(bytes);
        return this.decodeOrNull(input);
    }

    public String decodeOrNull(ByteBuffer input) {
        return _decode(input, ErrorPolicy.RETURN_NULL, MemoryPolicy.IGNORE);
    }

    /**
     * @throws TextConversionException
     */
    public String secureDecode(ByteBuffer input) {
        return _decode(input, ErrorPolicy.THROW, MemoryPolicy.ZERO);
    }
}

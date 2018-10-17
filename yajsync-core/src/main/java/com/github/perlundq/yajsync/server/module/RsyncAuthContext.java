/*
 * rsync challenge response MD5 authentication
 *
 * Copyright (C) 1996-2011 by Andrew Tridgell, Wayne Davison, and others
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
package com.github.perlundq.yajsync.server.module;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;

import com.github.perlundq.yajsync.internal.text.TextConversionException;
import com.github.perlundq.yajsync.internal.text.TextEncoder;
import com.github.perlundq.yajsync.internal.util.Consts;
import com.github.perlundq.yajsync.internal.util.MD5;

public class RsyncAuthContext {
    public static RsyncAuthContext fromChallenge(TextEncoder characterEncoder, String challenge) {
        return new RsyncAuthContext(characterEncoder, challenge);
    }
    
    private final String challenge;
    
    private final TextEncoder characterEncoder;
    
    public RsyncAuthContext(TextEncoder characterEncoder) {
        this.challenge = this.newChallenge();
        this.characterEncoder = characterEncoder;
    }
    
    private RsyncAuthContext(TextEncoder characterEncoder, String challenge) {
        this.challenge = challenge;
        this.characterEncoder = characterEncoder;
    }
    
    public String getChallenge() {
        return this.challenge;
    }
    
    /**
     * @throws TextConversionException if challenge cannot be encoded
     */
    private byte[] hash(char[] password) {
        byte[] passwordBytes = null;
        try {
            passwordBytes = this.characterEncoder.secureEncodeOrNull(password);
            if (passwordBytes == null) {
                throw new RuntimeException("Unable to encode characters in " + "password");
            }
            byte[] challengeBytes = this.characterEncoder.encode(this.challenge); // throws TextConversionException
            MessageDigest md = MD5.newInstance();
            md.update(passwordBytes);
            md.update(challengeBytes);
            return md.digest();
        } finally {
            if (passwordBytes != null) {
                Arrays.fill(passwordBytes, (byte) 0);
            }
        }
    }
    
    private String newChallenge() {
        long rand = new SecureRandom().nextLong();
        byte[] randBytes = ByteBuffer.allocate(Consts.SIZE_LONG).putLong(rand).array();
        return Base64.getEncoder().withoutPadding().encodeToString(randBytes);
    }
    
    /**
     * @throws TextConversionException if challenge cannot be encoded
     */
    public String response(char[] password) {
        byte[] hashedBytes = this.hash(password);
        return Base64.getEncoder().withoutPadding().encodeToString(hashedBytes);
    }
}

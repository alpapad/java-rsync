/*
 * rsync peer handshake session status
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
package com.github.java.rsync.internal.session;

public enum SessionStatus {
    AUTHREQ("@RSYNCD: AUTHREQD "), ERROR("@ERROR"), EXIT("@RSYNCD: EXIT"), OK("@RSYNCD: OK");
    
    private final String repr;
    
    SessionStatus(String s) {
        this.repr = s;
    }
    
    boolean matches(String s) {
        return s.startsWith(this.repr);
    }
    
    @Override
    public String toString() {
        return this.repr;
    }
}
/*
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
package com.github.perlundq.yajsync.ui;

import java.util.Objects;

import com.github.perlundq.yajsync.RsyncServer;
import com.github.perlundq.yajsync.internal.util.Environment;

final class ConnInfo {
    public static class Builder {
        private final String _address;
        private int _portNumber = RsyncServer.DEFAULT_LISTEN_PORT;
        private String _userName = Environment.getUserName();
        
        public Builder(String address) throws IllegalUrlException {
            assert address != null;
            if (address.isEmpty()) {
                throw new IllegalUrlException("address is empty");
            }
            this._address = address;
        }
        
        public ConnInfo build() {
            return new ConnInfo(this);
        }
        
        public Builder portNumber(int portNumber) throws IllegalUrlException {
            if (!isValidPortNumber(portNumber)) {
                throw new IllegalUrlException(String.format("illegal port %d - must be within the range [%d, %d]", portNumber, PORT_MIN, PORT_MAX));
            }
            this._portNumber = portNumber;
            return this;
        }
        
        public Builder userName(String userName) {
            assert userName != null;
            this._userName = userName;
            return this;
        }
    }
    
    public static final int PORT_MAX = 65535;
    public static final int PORT_MIN = 1;
    
    public static boolean isValidPortNumber(int portNumber) {
        return portNumber >= PORT_MIN && portNumber <= PORT_MAX;
    }
    
    private final String _address;
    
    private final int _portNumber;
    
    private final String _userName;
    
    private ConnInfo(Builder builder) {
        this._userName = builder._userName;
        this._address = builder._address;
        this._portNumber = builder._portNumber;
    }
    
    public String address() {
        return this._address;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj != null && this.getClass() == obj.getClass()) {
            ConnInfo other = (ConnInfo) obj;
            return this._userName.equals(other._userName) && this._address.equals(other._address) && this._portNumber == other._portNumber;
        }
        return false;
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(this._address, this._userName, this._portNumber);
    }
    
    public int portNumber() {
        return this._portNumber;
    }
    
    @Override
    public String toString() {
        return String.format("rsync://%s%s:%d", this._userName.isEmpty() ? "" : this._userName + "@", this._address, this._portNumber);
    }
    
    public String userName() {
        return this._userName;
    }
}

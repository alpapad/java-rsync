/*
 * Exception type for rsync security errors
 *
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
package com.github.perlundq.yajsync;

/**
 * Signals that an error which could compromise the integrity of yajsync has
 * occurred.
 */
public class RsyncSecurityException extends RsyncException {
    private static final long serialVersionUID = 1L;
    
    public RsyncSecurityException(Exception e) {
        super(e);
    }
    
    public RsyncSecurityException(String message) {
        super(message);
    }
}

/*
 * Automatically delete a file on close
 *
 * Copyright (C) 2015 Per Lundqvist
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
package com.github.java.rsync.internal.io;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class AutoDeletable implements AutoCloseable {
    private final Path path;
    
    public AutoDeletable(Path path) {
        this.path = path;
    }
    
    @Override
    public void close() throws IOException {
        Files.deleteIfExists(this.path);
    }
    
    public Path getPath() {
        return this.path;
    }
    
    @Override
    public String toString() {
        return this.path.toString();
    }
}

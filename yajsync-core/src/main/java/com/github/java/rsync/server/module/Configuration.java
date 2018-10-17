/*
 * Parsing of /etc/rsyncd.conf configuration files
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
package com.github.java.rsync.server.module;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.Principal;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.github.java.rsync.internal.text.Text;
import com.github.java.rsync.internal.util.ArgumentParser;
import com.github.java.rsync.internal.util.Environment;
import com.github.java.rsync.internal.util.Option;
import com.github.java.rsync.internal.util.PathOps;

public class Configuration implements Modules {
    private static class IllegalValueException extends Exception {
        private static final long serialVersionUID = 1L;
    }
    
    public static class Reader extends ModuleProvider {
        
        private static final Logger _log = Logger.getLogger(Reader.class.getName());
        private static final String DEFAULT_CONFIGURATION_FILE_NAME = "/home/alpapad/git/rsync/yajsync-orig/yajsync-app/yajsyncd.conf";
        private static final Pattern keyValuePattern = Pattern.compile("^([\\w ]+) *= *(\\S.*)$");
        private static final String MODULE_KEY_COMMENT = "comment";
        private static final String MODULE_KEY_FS = "fs";
        private static final String MODULE_KEY_IS_READABLE = "is_readable";
        private static final String MODULE_KEY_IS_WRITABLE = "is_writable";
        private static final String MODULE_KEY_PATH = "path";
        private static final Pattern modulePattern = Pattern.compile("^\\[\\s*([\\w ]+)\\s*\\]$");
        
        private static boolean isCommentLine(String line) {
            return line.startsWith("#") || line.startsWith(";");
        }
        
        private static Map<String, Map<String, String>> parse(BufferedReader reader) throws IOException {
            String prevLine = "";
            Map<String, Map<String, String>> modules = new TreeMap<>(); // { 'moduleName1' : { 'key1' : 'val1', ..., 'keyN' : 'valN'}, ... }
            Map<String, String> currentModule = new TreeMap<>(); // { 'key1' : 'val1', ..., 'keyN' : 'valN'}
            modules.put("", currentModule); // { 'key1' : 'val1', ..., 'keyN' : 'valN'}
            boolean isEOF = false;
            
            while (!isEOF) {
                String line = reader.readLine();
                isEOF = line == null;
                if (line == null) {
                    line = "";
                }
                
                String trimmedLine = prevLine + line.trim(); // prevLine is non-empty only if previous line ended with a backslash
                if (trimmedLine.isEmpty() || isCommentLine(trimmedLine)) {
                    continue;
                }
                
                String sep = FileSystems.getDefault().getSeparator();
                if (!sep.equals(Text.BACK_SLASH) && trimmedLine.endsWith(Text.BACK_SLASH)) {
                    prevLine = Text.stripLast(trimmedLine);
                } else {
                    prevLine = "";
                }
                
                Matcher moduleMatcher = modulePattern.matcher(trimmedLine);
                if (moduleMatcher.matches()) {
                    String moduleName = moduleMatcher.group(1).trim(); // TODO: remove consecutive white space in module name
                    currentModule = modules.get(moduleName);
                    if (currentModule == null) {
                        currentModule = new TreeMap<>();
                        modules.put(moduleName, currentModule);
                    }
                } else {
                    Matcher keyValueMatcher = keyValuePattern.matcher(trimmedLine);
                    if (keyValueMatcher.matches()) {
                        String key = keyValueMatcher.group(1).trim();
                        String val = keyValueMatcher.group(2).trim();
                        currentModule.put(key, val);
                    }
                }
            }
            return modules;
        }
        
        private static boolean toBoolean(String val) throws IllegalValueException {
            if (val == null) {
                throw new IllegalValueException();
            } else if (val.equalsIgnoreCase("true") || val.equalsIgnoreCase("yes")) {
                return true;
            } else if (val.equalsIgnoreCase("false") || val.equalsIgnoreCase("no")) {
                return false;
            }
            throw new IllegalValueException();
        }
        
        private String cfgFileName = Environment.getServerConfig(DEFAULT_CONFIGURATION_FILE_NAME);
        
        public Reader() {
        }
        
        @Override
        public void close() {
            // NOP
        }
        
        private Map<String, Module> getModules(String fileName) throws ModuleException {
            Map<String, Map<String, String>> modules;
            try (BufferedReader reader = Files.newBufferedReader(Paths.get(fileName), Charset.defaultCharset())) {
                modules = parse(reader);
            } catch (IOException e) {
                throw new ModuleException(e);
            }
            
            Map<String, Module> result = new TreeMap<>();
            
            for (Map.Entry<String, Map<String, String>> keyVal : modules.entrySet()) {
                
                String moduleName = keyVal.getKey();
                Map<String, String> moduleContent = keyVal.getValue();
                
                boolean isGlobalModule = moduleName.isEmpty();
                if (isGlobalModule) {
                    continue; // Not currently used
                }
                
                String pathValue = moduleContent.get(MODULE_KEY_PATH);
                boolean isValidModule = pathValue != null;
                if (!isValidModule) {
                    if (_log.isLoggable(Level.WARNING)) {
                        _log.warning(String.format("skipping incomplete " + "module %s - lacking path", moduleName));
                    }
                    continue;
                }
                
                try {
                    String fsValue = moduleContent.get(MODULE_KEY_FS);
                    FileSystem fs;
                    if (fsValue != null) {
                        fs = PathOps.fileSystemOf(fsValue);
                    } else {
                        fs = FileSystems.getDefault();
                    }
                    
                    Path p = PathOps.get(fs, pathValue);
                    RestrictedPath vp = new RestrictedPath(moduleName, p);
                    SimpleModule m = new SimpleModule(moduleName, vp);
                    String comment = Text.nullToEmptyStr(moduleContent.get(MODULE_KEY_COMMENT));
                    m.comment = comment;
                    if (moduleContent.containsKey(MODULE_KEY_IS_READABLE)) {
                        boolean isReadable = toBoolean(moduleContent.get(MODULE_KEY_IS_READABLE));
                        m.isReadable = isReadable;
                    }
                    if (moduleContent.containsKey(MODULE_KEY_IS_WRITABLE)) {
                        boolean isWritable = toBoolean(moduleContent.get(MODULE_KEY_IS_WRITABLE));
                        m.isWritable = isWritable;
                    }
                    result.put(moduleName, m);
                } catch (InvalidPathException | IllegalValueException | IOException | URISyntaxException e) {
                    if (_log.isLoggable(Level.WARNING)) {
                        _log.warning(String.format("skipping module %s: %s", moduleName, e.getMessage()));
                    }
                }
            }
            return result;
        }
        
        @Override
        public Configuration newAnonymous(InetAddress address) throws ModuleException {
            Map<String, Module> modules = this.getModules(this.cfgFileName);
            Configuration cfg = new Configuration(modules);
            return cfg;
        }
        
        @Override
        public Configuration newAuthenticated(InetAddress address, Principal principal) throws ModuleException {
            return this.newAnonymous(address);
        }
        
        @Override
        public Collection<Option> options() {
            List<Option> options = new LinkedList<>();
            options.add(Option.newStringOption(Option.Policy.OPTIONAL, "config", "", String.format("path to configuration file (default " + "%s)", this.cfgFileName), option -> {
                this.cfgFileName = (String) option.getValue();
                return ArgumentParser.Status.CONTINUE;
            }));
            return options;
        }
    }
    
    private static class SimpleModule implements Module {
        private String comment = "";
        private boolean isReadable = true;
        private boolean isWritable = false;
        private final String name;
        private final RestrictedPath restrictedPath;
        
        public SimpleModule(String name, RestrictedPath restrictedPath) {
            assert name != null;
            assert restrictedPath != null;
            this.name = name;
            this.restrictedPath = restrictedPath;
        }
        
        @Override
        public String getComment() {
            return this.comment;
        }
        
        @Override
        public boolean isReadable() {
            return this.isReadable;
        }
        
        @Override
        public boolean isWritable() {
            return this.isWritable;
        }
        
        @Override
        public String getName() {
            return this.name;
        }
        
        @Override
        public RestrictedPath getRestrictedPath() {
            return this.restrictedPath;
        }
    }
    
    private final Map<String, Module> modules;
    
    public Configuration(Map<String, Module> modules) {
        this.modules = modules;
    }
    
    @Override
    public Iterable<Module> all() {
        return this.modules.values();
    }
    
    @Override
    public Module get(String moduleName) throws ModuleException {
        Module m = this.modules.get(moduleName);
        if (m == null) {
            throw new ModuleNotFoundException(String.format("module %s does not exist", moduleName));
        }
        return m;
    }
}

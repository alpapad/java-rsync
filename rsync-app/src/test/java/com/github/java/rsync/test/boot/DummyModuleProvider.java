package com.github.java.rsync.test.boot;

import java.net.InetAddress;
import java.security.Principal;
import java.util.Collection;
import java.util.Collections;

import com.github.java.rsync.internal.util.Option;
import com.github.java.rsync.server.module.Module;
import com.github.java.rsync.server.module.ModuleException;
import com.github.java.rsync.server.module.ModuleProvider;
import com.github.java.rsync.server.module.Modules;
import com.github.java.rsync.server.module.RestrictedPath;

public class DummyModuleProvider extends ModuleProvider {
    private final String destination;
    public DummyModuleProvider(final String destination) {
        super();
        this.destination = destination;
    }

    @Override
    public void close() {
        
    }
    
    @Override
    public Modules newAnonymous(InetAddress address) throws ModuleException {
        return new SimpleModules(destination);
    }
    
    @Override
    public Modules newAuthenticated(InetAddress address, Principal principal) throws ModuleException {
        return new SimpleModules(destination);
    }
    
    @Override
    public Collection<Option> options() {
        return Collections.emptyList();
    }
    private static class SimpleModules implements Modules {
        SimpleModule module;
        
        public SimpleModules(final String destination) {
            super();
            RestrictedPath vp = new RestrictedPath("test", "boot:file://" + destination, "/");
            module = new SimpleModule("test", vp);
        }

        @Override
        public Iterable<Module> all() {
            return Collections.singleton(module);
        }

        @Override
        public Module get(String moduleName) throws ModuleException {
            assert "test".equals(moduleName);
            return module;
        }
        
    }
    private static class SimpleModule implements Module {
        private String comment = "";
        private boolean isReadable = true;
        private boolean isWritable = true;
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
            return comment;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public RestrictedPath getRestrictedPath() {
            return restrictedPath;
        }

        @Override
        public boolean isReadable() {
            return isReadable;
        }

        @Override
        public boolean isWritable() {
            return isWritable;
        }
    }
}

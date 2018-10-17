package com.github.perlundq.yajsync.internal.util;

public class StatusResult<T> {
    private final boolean isOK;
    private final T value;
    
    public StatusResult(boolean isOK, T value) {
        this.isOK = isOK;
        this.value = value;
    }
    
    public boolean isOK() {
        return this.isOK;
    }
    
    public T getValue() {
        return this.value;
    }
}
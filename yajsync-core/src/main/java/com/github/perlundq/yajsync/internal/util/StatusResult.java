package com.github.perlundq.yajsync.internal.util;

public class StatusResult<T> {
    private final boolean _isOK;
    private final T _value;
    
    public StatusResult(boolean isOK, T value) {
        this._isOK = isOK;
        this._value = value;
    }
    
    public boolean isOK() {
        return this._isOK;
    }
    
    public T value() {
        return this._value;
    }
}
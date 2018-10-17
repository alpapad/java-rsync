/*
 * Argument parsing option type
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
package com.github.perlundq.yajsync.internal.util;

public class Option {
    @FunctionalInterface
    public interface Handler {
        ArgumentParser.Status handle(Option option) throws ArgumentParsingError;
    }
    
    public enum Policy {
        OPTIONAL, REQUIRED
    }
    
    public static Option newHelpOption(Handler handler) {
        return Option.newWithoutArgument(Option.Policy.OPTIONAL, "help", "h", "show this help text", handler);
    }
    
    public static Option newIntegerOption(Policy policy, String longName, String shortName, String shortHelp, Handler handler) {
        return new Option(Integer.class, policy, longName, shortName, shortHelp, handler);
    }
    
    public static Option newStringOption(Policy policy, String longName, String shortName, String shortHelp, Handler handler) {
        return new Option(String.class, policy, longName, shortName, shortHelp, handler);
    }
    
    public static Option newWithoutArgument(Policy policy, String longName, String shortName, String shortHelp, Handler handler) {
        return new Option(Void.class, policy, longName, shortName, shortHelp, handler);
    }
    
    private final Handler handler;
    private final String longName;
    private int numInstances = 0;
    private final Policy policy;
    
    private final String shortHelp;
    
    private final String shortName;
    
    private final Class<?> type;
    
    private Object value;
    
    private Option(Class<?> type, Policy policy, String longName, String shortName, String shortHelp, Handler handler) {
        assert type != null;
        assert policy != null;
        assert longName != null;
        assert shortName != null;
        assert shortHelp != null;
        assert handler != null || type == Void.class && policy == Policy.REQUIRED;
        assert !(longName.isEmpty() && shortName.isEmpty());
        assert longName.length() > 1 || shortName.length() == 1 : "An option must have either a long name (>=2 characters) and/or a" + " one character long short name associated with it";
        this.type = type;
        this.policy = policy;
        this.longName = longName;
        this.shortName = shortName;
        this.shortHelp = shortHelp;
        this.handler = handler;
    }
    
    public String exampleUsageToString() {
        String shortName = this.usageShortToString();
        String longName = this.usageLongToString();
        StringBuilder sb = new StringBuilder();
        if (shortName.length() > 0 && longName.length() > 0) {
            return sb.append(shortName).append(" or ").append(longName).toString();
        } else if (shortName.length() > 0) {
            return sb.append(shortName).toString();
        } else {
            return sb.append(longName).toString();
        }
    }
    
    public boolean expectsValue() {
        return this.type != Void.class;
    }
    
    public Object getValue() {
        if (!this.isSet()) {
            throw new IllegalStateException(String.format("%s has not been " + "parsed yet", this));
        }
        return this.value;
    }
    
    public boolean hasLongName() {
        return this.longName.length() > 0;
    }
    
    public boolean hasShortName() {
        return this.shortName.length() > 0;
    }
    
    public boolean isRequired() {
        return this.policy == Policy.REQUIRED;
    }
    
    public boolean isSet() {
        return this.numInstances > 0;
    }
    
    public String longName() {
        return this.longName;
    }
    
    public String name() {
        if (this.hasLongName()) {
            return String.format("--%s", this.longName);
        } else {
            return String.format("-%s", this.shortName);
        }
    }
    
    public ArgumentParser.Status setValue(String str) throws ArgumentParsingError {
        try {
            if (this.type == Void.class) {
                if (!str.isEmpty()) {
                    throw new ArgumentParsingError(String.format("%s expects no argument - remove %s%nExample: %s", this.name(), str, this.exampleUsageToString()));
                }
            } else if (this.type == Integer.class) {
                this.value = Integer.valueOf(str);
            } else if (this.type == String.class) {
                if (str.isEmpty()) {
                    throw new ArgumentParsingError(String.format("%s expects an argument%nExample: %s", this.name(), this.exampleUsageToString()));
                }
                this.value = str;
            } else {
                throw new IllegalStateException(String.format("BUG: %s is of an unsupported type to %s%nExample: %s", str, this.name(), this.exampleUsageToString()));
            }
            this.numInstances++;
            if (this.handler != null) {
                return this.handler.handle(this);
            }
            return ArgumentParser.Status.CONTINUE;
        } catch (NumberFormatException e) {
            throw new ArgumentParsingError(String.format("%s - invalid value" + " %s%n%s%nExample: %s", this.name(), str, this.exampleUsageToString(), e));
        }
    }
    
    public String shortHelp() {
        return this.shortHelp;
    }
    
    public String shortName() {
        return this.shortName;
    }
    
    @Override
    public String toString() {
        return String.format("%s (type=%s policy=%s longName=%s shortName=%s)" + " { value=%s }", this.getClass().getSimpleName(), this.type, this.policy, this.longName, this.shortName,
                this.value);
    }
    
    public String usageLongToString() {
        if (this.longName.isEmpty()) {
            return "";
        } else if (this.expectsValue()) {
            return String.format("--%s=<%s>", this.longName, this.type.getSimpleName());
        } else {
            return String.format("--%s", this.longName);
        }
    }
    
    public String usageShortToString() {
        if (this.shortName.isEmpty()) {
            return "";
        } else if (this.expectsValue()) {
            return String.format("-%s <%s>", this.shortName, this.type.getSimpleName());
        } else {
            return String.format("-%s", this.shortName);
        }
    }
}
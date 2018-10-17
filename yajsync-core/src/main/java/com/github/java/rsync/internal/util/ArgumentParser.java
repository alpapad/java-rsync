/*
 * getopt style argument parser
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
package com.github.java.rsync.internal.util;

import java.io.PrintStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ArgumentParser {
    public enum Status {
        CONTINUE, EXIT_ERROR, EXIT_OK
    }
    
    public static ArgumentParser newNoUnnamed(String programName) {
        ArgumentParser instance = new ArgumentParser(programName, null);
        return instance;
    }
    
    public static ArgumentParser newWithUnnamed(String programName, String unnamedHelpText) {
        assert unnamedHelpText != null;
        ArgumentParser instance = new ArgumentParser(programName, unnamedHelpText);
        return instance;
    }
    
    private static String optionToColumnString(Option option) {
        String shortName = option.usageShortToString();
        String longName = option.usageLongToString();
        StringBuilder sb = new StringBuilder();
        if (shortName.isEmpty()) {
            sb.append("    ");
        } else {
            sb.append(shortName).append(longName.isEmpty() ? "  " : ", ");
        }
        sb.append(longName);
        return sb.toString();
    }
    
    private static String optionToUsageString(Option option) {
        String beginGroup;
        String endGroup;
        if (option.isRequired()) {
            beginGroup = "{";
            endGroup = "}";
        } else {
            beginGroup = "[";
            endGroup = "]";
            
        }
        StringBuilder sb = new StringBuilder();
        sb.append(beginGroup);
        if (option.hasShortName()) {
            sb.append(option.usageShortToString());
        }
        if (option.hasLongName()) {
            if (option.hasShortName()) {
                sb.append("|");
            }
            sb.append(option.usageLongToString());
        }
        sb.append(endGroup);
        return sb.toString();
    }
    
    private final Map<String, Option> longOptions = new HashMap<>();
    private final List<Option> optional = new LinkedList<>();
    private final List<Option> parsed = new LinkedList<>();
    private final String programName;
    
    private final List<Option> required = new LinkedList<>();
    
    private final Map<String, Option> shortOptions = new HashMap<>();
    
    private final List<String> unnamedArguments = new LinkedList<>();
    
    private final String unnamedHelpText;
    
    public ArgumentParser(String programName, String unnamedHelpText) {
        assert programName != null;
        this.programName = programName;
        this.unnamedHelpText = unnamedHelpText;
    }
    
    /**
     * @throws IllegalArgumentException
     */
    public void add(Option option) {
        if (this.longOptions.containsKey(option.longName())) {
            throw new IllegalArgumentException(String.format("long option %s already exists (%s)", option.longName(), this.longOptions.get(option.longName())));
        }
        if (this.shortOptions.containsKey(option.shortName())) {
            throw new IllegalArgumentException(String.format("short option %s already exists (%s)", option.shortName(), this.shortOptions.get(option.shortName())));
        }
        if (option.hasLongName()) {
            this.longOptions.put(option.longName(), option);
        }
        if (option.hasShortName()) {
            this.shortOptions.put(option.shortName(), option);
        }
        if (option.isRequired()) {
            this.required.add(option);
        } else {
            this.optional.add(option);
        }
    }
    
    public void addHelpTextDestination(final PrintStream stream) {
        Option.Handler h = new Option.Handler() {
            @Override
            public Status handle(Option option) {
                stream.println(ArgumentParser.this.toUsageString());
                return Status.EXIT_OK;
            }
        };
        this.add(Option.newHelpOption(h));
    }
    
    public void addUnnamed(String arg) throws ArgumentParsingError {
        if (this.isUnnamedArgsAllowed()) {
            this.unnamedArguments.add(arg);
        } else {
            throw new ArgumentParsingError(arg + " - unknown unnamed argument");
        }
    }
    
    public List<Option> getAllOptions() {
        Set<Option> all = new LinkedHashSet<>();
        all.addAll(this.longOptions.values());
        all.addAll(this.shortOptions.values());
        return new LinkedList<>(all);
    }
    
    private Option getOptionForName(String name) throws ArgumentParsingError {
        Option result;
        if (name.length() == 1) {
            result = this.shortOptions.get(name);
        } else {
            result = this.longOptions.get(name);
        }
        if (result == null) {
            throw new ArgumentParsingError(name + " - unknown option");
        }
        return result;
    }
    
    public List<Option> getParsedOptions() {
        return this.parsed;
    }
    
    public List<String> getUnnamedArguments() {
        return this.unnamedArguments;
    }
    
    // make configurable?
    private boolean isEndOfOptionMarker(String arg) {
        return arg.equals("--") || arg.equals("-");
    }
    
    private boolean isLongOption(String arg) {
        return arg.length() > 2 && arg.startsWith("--");
    }
    
    private boolean isShortOption(String arg) {
        return arg.length() > 1 && arg.startsWith("-") && !this.isLongOption(arg);
    }
    
    private boolean isUnnamedArgsAllowed() {
        return this.unnamedHelpText != null;
    }
    
    public Status parse(Iterable<String> args) throws ArgumentParsingError {
        Option currentOption = null;
        boolean isRemainingUnnamed = false;
        
        for (String arg : args) {
            if (isRemainingUnnamed) {
                assert currentOption == null;
                this.addUnnamed(arg);
            } else if (currentOption != null) { // from previous short option
                Status rc = this.setValue(currentOption, arg);
                if (rc != Status.CONTINUE) {
                    return rc;
                }
                currentOption = null;
            } else if (this.isEndOfOptionMarker(arg)) {
                isRemainingUnnamed = true;
            } else if (this.isLongOption(arg)) {
                String[] result = this.splitLongOption(arg);
                Option opt = this.getOptionForName(result[0]);
                Status rc = this.setValue(opt, result[1]);
                if (rc != Status.CONTINUE) {
                    return rc;
                }
            } else if (this.isShortOption(arg)) {
                assert currentOption == null;
                char[] charArray = arg.toCharArray();
                
                for (int i = 1; i < charArray.length; i++) {
                    String shortName = Character.toString(charArray[i]);
                    Option opt = this.getOptionForName(shortName);
                    if (opt.expectsValue()) { // munge all the remaining chars
                        int offset = i + 1;
                        int length = charArray.length - offset;
                        if (offset < charArray.length && length > 0) {
                            String value = new String(charArray, offset, length);
                            Status rc = this.setValue(opt, value);
                            if (rc != Status.CONTINUE) {
                                return rc;
                            }
                        } else { // else set value from next arg
                            currentOption = opt;
                        }
                        break;
                    } else {
                        Status rc = this.setValue(opt, "");
                        if (rc != Status.CONTINUE) {
                            return rc;
                        }
                    }
                }
            } else {
                isRemainingUnnamed = true;
                this.addUnnamed(arg);
            }
        }
        
        if (currentOption != null) {
            throw new ArgumentParsingError(String.format("%s expects an argument%nExample: %s", currentOption.name(), currentOption.exampleUsageToString()));
        }
        
        for (Option o : this.required) {
            if (!o.isSet()) {
                throw new ArgumentParsingError(String.format("%s is a required option", o.name()));
            }
        }
        return Status.CONTINUE;
    }
    
    private Status setValue(Option opt, String value) throws ArgumentParsingError {
        Status rc = opt.setValue(value);
        this.parsed.add(opt);
        return rc;
    }
    
    private String[] splitLongOption(String arg) throws ArgumentParsingError {
        String longName;
        String value;
        int indexOfEqualChar = arg.indexOf('=');
        if (indexOfEqualChar == arg.length() - 1) {
            throw new ArgumentParsingError("syntax error: parameter " + "key without a value: " + arg);
        }
        if (indexOfEqualChar >= 0) {
            longName = arg.substring(2, indexOfEqualChar);
            value = arg.substring(indexOfEqualChar + 1);
        } else {
            longName = arg.substring(2);
            value = "";
        }
        String[] result = { longName, value };
        return result;
    }
    
    public String toUsageString() {
        final String ls = System.lineSeparator();
        final StringBuilder sb = new StringBuilder();
        sb.append("Usage: ").append(this.programName).append(" ");
        
        for (Iterator<Option> it = this.required.iterator(); it.hasNext();) {
            Option opt = it.next();
            sb.append(optionToUsageString(opt));
            if (it.hasNext()) {
                sb.append(" ");
            }
        }
        
        if (!this.required.isEmpty()) {
            sb.append(" ");
        }
        
        for (Iterator<Option> it = this.optional.iterator(); it.hasNext();) {
            Option opt = it.next();
            sb.append(optionToUsageString(opt));
            if (it.hasNext()) {
                sb.append(" ");
            }
        }
        
        if (this.unnamedHelpText != null) {
            sb.append(" ").append(this.unnamedHelpText);
        }
        
        if (this.required.size() + this.optional.size() > 0) {
            sb.append(ls).append(ls).append("Options:").append(ls);
        }
        
        int maxWidth = 0;
        for (Option opt : this.required) {
            int width = optionToColumnString(opt).length();
            if (width > maxWidth) {
                maxWidth = width;
            }
        }
        for (Option opt : this.optional) {
            int width = optionToColumnString(opt).length();
            if (width > maxWidth) {
                maxWidth = width;
            }
        }
        maxWidth += 3;
        
        // TODO: automatically split shortHelp to 80 characters
        for (Option opt : this.optional) {
            sb.append(String.format("%-" + maxWidth + "s", optionToColumnString(opt))).append(opt.shortHelp()).append(ls);
        }
        
        for (Option opt : this.required) {
            sb.append(String.format("%-" + maxWidth + "s", optionToColumnString(opt))).append(opt.shortHelp()).append(ls);
        }
        
        return sb.toString();
    }
}

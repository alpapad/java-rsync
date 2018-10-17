/*
 * Rsync filter rules
 *
 * Copyright (C) 2013, 2014 Per Lundqvist
 * Copyright (C) 2014 Florian Sager
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

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import com.github.java.rsync.internal.util.ArgumentParsingError;

public class FilterRuleList {
    
    /*
     * see http://rsync.samba.org/ftp/rsync/rsync.html --> FILTER RULES
     */
    public class FilterRule {
        
        private final boolean absoluteMatching;
        private final boolean deletionRule;
        private final boolean directoryOnly;
        private final boolean hidingRule;
        private final boolean inclusion;
        private final boolean negateMatching;
        private String path;
        private Pattern pattern;
        private final boolean patternMatching;
        
        /*
         * @formatter:off
         *
         * Input samples: + /some/path/this-file-is-found + *.csv - * + !/.svn/
         *
         * @formatter:on
         */
        
        public FilterRule(String plainRule) throws ArgumentParsingError {
            
            String[] splittedRule = plainRule.split("\\s+");
            if (splittedRule.length != 2) {
                throw new ArgumentParsingError(String.format("failed to parse filter rule '%s', invalid format: should be '<+|-|P|R|H|S> <modifier><path-expression>'", plainRule));
            }
            
            if ("P".equals(splittedRule[0])) {
                splittedRule[0] = "-";
                deletionRule = true;
            } else if ("R".equals(splittedRule[0])) {
                splittedRule[0] = "+";
                deletionRule = true;
            } else {
                deletionRule = false;
            }
            
            if ("H".equals(splittedRule[0])) {
                splittedRule[0] = "-";
                hidingRule = true;
            } else if ("S".equals(splittedRule[0])) {
                splittedRule[0] = "+";
                hidingRule = true;
            } else {
                hidingRule = false;
            }
            
            if (!"+".equals(splittedRule[0]) && !"-".equals(splittedRule[0])) {
                throw new ArgumentParsingError(String.format("failed to parse filter rule '%s': must start with + (inclusion) or - (exclusion)", plainRule));
            }
            
            inclusion = "+".equals(splittedRule[0]);
            
            directoryOnly = splittedRule[1].endsWith("/");
            
            negateMatching = splittedRule[1].startsWith("!");
            
            path = splittedRule[1].substring(negateMatching ? 1 : 0, directoryOnly ? splittedRule[1].length() - 1 : splittedRule[1].length());
            
            absoluteMatching = path.startsWith("/");
            
            // add . for absolute matching to conform to rsync paths
            if (absoluteMatching) {
                path = "." + path;
            }
            
            // check if string or pattern matching is required
            // patternMatching = path.contains("*") || path.contains("?") ||
            // path.contains("[");
            patternMatching = path.matches(".*[\\*\\?\\[].*");
            
            if (patternMatching) {
                
                StringBuilder b = new StringBuilder();
                
                if (absoluteMatching) {
                    b.append("^");
                }
                
                for (int i = 0; i < path.length(); i++) {
                    
                    char c = path.charAt(i);
                    
                    if (c == '?') {
                        b.append("[^/]");
                    } else if (c == '*' && i + 1 < path.length() && path.charAt(i + 1) == '*') {
                        b.append(".*");
                    } else if (c == '*') {
                        b.append("[^/].*");
                    } else {
                        b.append(c);
                    }
                }
                
                pattern = Pattern.compile(b.toString());
            }
        }
        
        public boolean isDirectoryOnly() {
            return directoryOnly;
        }
        
        public boolean isInclusion() {
            return inclusion;
        }
        
        public boolean matches(String filename) {
            
            boolean _result;
            
            if (patternMatching) {
                _result = pattern.matcher(filename).matches();
            } else {
                
                String path = this.path + (directoryOnly ? "/" : "");
                
                // string matching
                if (absoluteMatching) {
                    if (filename.length() < path.length()) {
                        // no matching if filename is shorter than path
                        _result = false;
                    } else if (filename.length() == path.length()) {
                        // matching if filename equals path
                        _result = filename.startsWith(path);
                    } else if (filename.charAt(path.length()) == '/') {
                        // matching if filename is contained in path
                        _result = filename.startsWith(path);
                    } else {
                        _result = false;
                    }
                } else {
                    // tail matching
                    if (path.length() < filename.length()) {
                        _result = filename.endsWith("/" + path);
                    } else {
                        _result = filename.equals(path);
                    }
                }
            }
            
            return negateMatching ? !_result : _result;
        }
        
        @Override
        public String toString() {
            StringBuilder buf = new StringBuilder();
            if (deletionRule) {
                buf.append(inclusion ? "R" : "P").append(" ");
            } else if (hidingRule) {
                buf.append(inclusion ? "S" : "H").append(" ");
            } else {
                buf.append(inclusion ? "+" : "-").append(" ");
            }
            buf.append(negateMatching ? "!" : "");
            /*
             * if (patternMatching) { buf.append(pattern.toString()); } else {
             */
            buf.append(path);
            // }
            if (directoryOnly) {
                buf.append("/");
            }
            
            return buf.toString();
        }
    }
    
    public enum Result {
        EXCLUDED /* PROTECTED, HIDE */, INCLUDED /* RISK, SHOW */ , NEUTRAL
    }
    
    public List<FilterRule> _rules = new ArrayList<>();
    
    public FilterRuleList addList(FilterRuleList list) {
        _rules.addAll(list._rules);
        return this;
    }
    
    public void addRule(String rule) throws ArgumentParsingError {
        _rules.add(new FilterRule(rule));
    }
    
    public Result check(String filename, boolean isDirectory) {
        
        for (FilterRule rule : _rules) {
            
            if (!isDirectory && rule.isDirectoryOnly()) {
                continue;
            }
            
            boolean matches = rule.matches(filename);
            
            if (matches) {
                /*
                 * first matching rule matters
                 */
                if (rule.isInclusion()) {
                    return Result.INCLUDED;
                } else {
                    return Result.EXCLUDED;
                }
            }
        }
        
        return Result.NEUTRAL;
    }
    
    @Override
    public String toString() {
        
        StringBuilder buf = new StringBuilder();
        for (FilterRule rule : _rules) {
            buf.append(rule).append("; ");
        }
        return buf.toString();
    }
}

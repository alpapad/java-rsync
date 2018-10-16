/*
 * Rsync filter rules
 *
 * Copyright (C) 1996-2011 by Andrew Tridgell, Wayne Davison, and others
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
package com.github.perlundq.yajsync.internal.session;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import com.github.perlundq.yajsync.internal.session.FilterRuleList.Result;
import com.github.perlundq.yajsync.internal.util.ArgumentParsingError;

public class FilterRuleConfiguration {
    
    private class Modifier {
        boolean _dirMerge;
        boolean _exclude;
        boolean _excludeMergeFilename;
        boolean _hide;
        boolean _include;
        boolean _merge;
        boolean _noInheritanceOfRules;
        boolean _protect;
        boolean _risk;
        boolean _show;
        
        public void checkValidity(String plainRule) throws ArgumentParsingError {
            if (this._merge && this._dirMerge || this._include && this._exclude || this._protect && this._risk) {
                throw new ArgumentParsingError(String.format("invalid combination of modifiers in rule %s (processing %s)", plainRule, FilterRuleConfiguration.this._dirname));
            }
        }
    }
    
    public enum RuleType {
        DELETION, EXCLUSION, HIDING
    }
    
    private final FilterRuleList _deletionRuleList = new FilterRuleList();
    private String _dirMergeFilename = null;
    private String _dirname = null;
    private final FilterRuleList _hidingRuleList = new FilterRuleList();
    private boolean _inheritance = true;
    
    // FSTODO: CustomFileSystem.getConfigPath( ... )
    
    private FilterRuleList _localRuleList = new FilterRuleList();
    
    private FilterRuleConfiguration _parentRuleConfiguration = null;
    
    public FilterRuleConfiguration(FilterRuleConfiguration parentRuleConfiguration, Path directory) throws ArgumentParsingError {
        
        this._parentRuleConfiguration = parentRuleConfiguration;
        if (this._parentRuleConfiguration != null) {
            this._inheritance = this._parentRuleConfiguration.isInheritance();
            this._dirMergeFilename = this._parentRuleConfiguration.getDirMergeFilename();
        }
        this._dirname = directory.toString();
        
        if (this._dirMergeFilename != null && new File(this._dirname + "/" + this._dirMergeFilename).exists()) {
            // merge local filter rule file
            this.readRule(". " + this._dirname + "/" + this._dirMergeFilename);
        }
    }
    
    public FilterRuleConfiguration() {
        
    }
    
    public FilterRuleConfiguration(List<String> inputFilterRules) throws ArgumentParsingError {
        for (String inputFilterRule : inputFilterRules) {
            this.readRule(inputFilterRule);
        }
    }
    
    private String assureDirectoryPathname(String filename, boolean isDirectory) {
        
        if (!isDirectory) {
            return filename;
        }
        if (isDirectory && !filename.endsWith("/")) {
            return filename + "/";
        }
        return filename;
    }
    
    public Result check(String filename, boolean isDirectory, RuleType ruleType) {
        
        this.assureDirectoryPathname(filename, isDirectory);
        
        Result result;
        
        switch (ruleType) {
            case EXCLUSION:
                result = this._localRuleList.check(filename, isDirectory);
                break;
            case DELETION:
                result = this._deletionRuleList.check(filename, isDirectory);
                break;
            case HIDING:
                result = this._hidingRuleList.check(filename, isDirectory);
                break;
            default:
                throw new RuntimeException("ruleType " + ruleType + " not implemented");
        }
        
        if (result != Result.NEUTRAL) {
            return result;
        }
        
        // search root and check against root only
        FilterRuleConfiguration parent = this;
        while (parent.getParentRuleConfiguration() != null) {
            parent = parent.getParentRuleConfiguration();
            if (parent.isInheritance()) {
                result = parent.check(filename, isDirectory, ruleType);
                if (result != Result.NEUTRAL) {
                    return result;
                }
            }
        }
        
        return Result.NEUTRAL;
    }
    
    public boolean exclude(String filename, boolean isDirectory) {
        Result result = this.check(filename, isDirectory, RuleType.EXCLUSION);
        if (result == Result.EXCLUDED) {
            return true;
        }
        
        return false;
    }
    
    public String getDirMergeFilename() {
        return this._dirMergeFilename;
    }
    
    public FilterRuleList getFilterRuleListForSending() {
        return new FilterRuleList().addList(this._localRuleList).addList(this._deletionRuleList);
    }
    
    public FilterRuleConfiguration getParentRuleConfiguration() {
        return this._parentRuleConfiguration;
    }
    
    public boolean hide(String filename, boolean isDirectory) {
        Result result = this.check(filename, isDirectory, RuleType.HIDING);
        if (result == Result.EXCLUDED) {
            return true;
        }
        
        return false;
    }
    
    public boolean include(String filename, boolean isDirectory) {
        Result result = this.check(filename, isDirectory, RuleType.EXCLUSION);
        if (result == Result.EXCLUDED) {
            return false;
        }
        
        return true;
    }
    
    public boolean isFilterAvailable() {
        boolean result = this._localRuleList._rules.size() > 0 || this._deletionRuleList._rules.size() > 0 || this._hidingRuleList._rules.size() > 0;
        if (!result && this._inheritance && this._parentRuleConfiguration != null) {
            result = this._parentRuleConfiguration.isFilterAvailable();
        }
        return result;
    }
    
    public boolean isInheritance() {
        return this._inheritance;
    }
    
    public boolean protect(String filename, boolean isDirectory) {
        Result result = this.check(filename, isDirectory, RuleType.DELETION);
        if (result == Result.EXCLUDED) {
            return true;
        }
        
        return false;
    }
    
    /*
     * public List<MergeRule> getMergeRuleList() { return _mergeRuleList; }
     */
    
    // see http://rsync.samba.org/ftp/rsync/rsync.html --> MERGE-FILE FILTER
    // RULES
    private Modifier readModifiers(String modifier, String plainRule) throws ArgumentParsingError {
        
        Modifier m = new Modifier();
        
        int i = 0;
        while (i < modifier.length()) {
            
            char c = modifier.charAt(i);
            
            if (c == '-') {
                // exclude rule
                m._exclude = true;
                i++;
                continue;
            } else if (c == '+') {
                // include rule
                m._include = true;
                i++;
                continue;
            }
            
            if (i > 0) {
                if (c == 'e') {
                    // exclude the merge-file name from the transfer
                    m._excludeMergeFilename = true;
                    i++;
                    continue;
                } else if (c == 'n') {
                    // don't inherit rules
                    m._noInheritanceOfRules = true;
                    i++;
                    continue;
                } else if (c == 'w') {
                    // A w specifies that the rules are word-split on whitespace
                    // instead of the normal line-splitting
                    throw new ArgumentParsingError(String.format("the modifier 'w' is not implemented, see rule '%s'", plainRule));
                } else if (c == ',') {
                    i++;
                    continue;
                }
            }
            
            if (c == '.') {
                // merge
                m._merge = true;
                i++;
                continue;
            } else if (c == ':') {
                // dir-merge
                m._dirMerge = true;
                i++;
                continue;
            } else if (c == 'm' && i + 5 <= modifier.length() && "merge".equals(modifier.substring(i, i + 5))) {
                // merge
                m._merge = true;
                i += 5;
                continue;
            } else if (c == 'd' && i + 9 <= modifier.length() && "dir-merge".equals(modifier.substring(i, i + 9))) {
                // dir-merge
                m._dirMerge = true;
                i += 9;
                continue;
            } else if (c == 'P') {
                // protect
                m._protect = true;
                i++;
                continue;
            } else if (c == 'p' && i + 7 <= modifier.length() && "protect".equals(modifier.substring(i, i + 7))) {
                // protect
                m._protect = true;
                i += 7;
                continue;
            } else if (c == 'R') {
                // risk
                m._risk = true;
                i++;
                continue;
            } else if (c == 'r' && i + 4 <= modifier.length() && "risk".equals(modifier.substring(i, i + 4))) {
                // risk
                m._risk = true;
                i += 4;
                continue;
            } else if (c == 'H') {
                // hide
                m._hide = true;
                i++;
                continue;
            } else if (c == 'h' && i + 4 <= modifier.length() && "hide".equals(modifier.substring(i, i + 4))) {
                // hide
                m._hide = true;
                i += 4;
                continue;
            } else if (c == 'S') {
                // show
                m._show = true;
                i++;
                continue;
            } else if (c == 's' && i + 4 <= modifier.length() && "show".equals(modifier.substring(i, i + 4))) {
                // show
                m._show = true;
                i += 4;
                continue;
            }
            
            throw new ArgumentParsingError(String.format("unknown modifier '%c' in rule %s", c, plainRule));
        }
        
        return m;
    }
    
    public void readRule(String plainRule) throws ArgumentParsingError {
        
        String[] splittedRule = plainRule.split("\\s+");
        
        if (splittedRule.length == 1 && (splittedRule[0].startsWith("!") || splittedRule[0].startsWith("clear"))) {
            // LIST-CLEARING FILTER RULE
            this._localRuleList = new FilterRuleList();
            // clearing refers to inclusion/exclusion lists only
            // _deletionRuleList = new FilterRuleList();
            // _hidingRuleList = new FilterRuleList();
            this._parentRuleConfiguration = null;
            return;
        }
        
        if (splittedRule.length != 2) {
            throw new ArgumentParsingError(String.format("failed to parse filter rule '%s', invalid format: should be '<+|-|merge|dir-merge>,<modifier> <filename|path-expression>'", plainRule));
        }
        
        Modifier m = this.readModifiers(splittedRule[0].trim(), plainRule);
        m.checkValidity(plainRule);
        
        if (m._merge == true || m._dirMerge == true) {
            
            if (m._noInheritanceOfRules == true) {
                this._inheritance = false;
            }
            
            if (m._merge == true) {
                
                // _mergeRuleList.add(new MergeRule(m, splittedRule[1].trim()));
                
                // String _mergeFilename = splittedRule[1].trim();
                Path _mergeFilename = Paths.get(splittedRule[1].trim());
                Path _absoluteMergeFilename = _mergeFilename;
                if (!_absoluteMergeFilename.isAbsolute()) {
                    _absoluteMergeFilename = Paths.get(this._dirname, splittedRule[1].trim());
                }
                
                try (BufferedReader br = new BufferedReader(new FileReader(_absoluteMergeFilename.toString()))) {
                    String line = br.readLine();
                    while (line != null) {
                        line = line.trim();
                        // ignore empty lines or comments
                        if (line.length() != 0 && !line.startsWith("#")) {
                            
                            if (m._exclude == true) {
                                this._localRuleList.addRule("- " + line);
                            } else if (m._include == true) {
                                this._localRuleList.addRule("+ " + line);
                            } else {
                                this.readRule(line);
                            }
                        }
                        line = br.readLine();
                    }
                    
                    if (m._excludeMergeFilename && _mergeFilename != null) {
                        this._localRuleList.addRule("- " + _mergeFilename);
                    }
                    
                } catch (IOException e) {
                    throw new ArgumentParsingError(String.format("impossible to parse filter file '%s'", _mergeFilename));
                }
                
                return;
            }
            
            if (this._dirMergeFilename == null && m._dirMerge == true) {
                this._dirMergeFilename = splittedRule[1].trim();
            }
            
            if (m._excludeMergeFilename && this._dirMergeFilename != null) {
                this._localRuleList.addRule("- " + this._dirMergeFilename);
            }
            
            return;
        }
        
        if (m._exclude == true) {
            this._localRuleList.addRule("- " + splittedRule[1].trim());
            return;
        } else if (m._include == true) {
            this._localRuleList.addRule("+ " + splittedRule[1].trim());
            return;
        } else if (m._protect == true) {
            this._deletionRuleList.addRule("P " + splittedRule[1].trim());
            return;
        } else if (m._risk == true) {
            this._deletionRuleList.addRule("R " + splittedRule[1].trim());
            return;
        } else if (m._hide == true) {
            this._hidingRuleList.addRule("H " + splittedRule[1].trim());
            return;
        } else if (m._show == true) {
            this._hidingRuleList.addRule("S " + splittedRule[1].trim());
            return;
        }
        
        throw new ArgumentParsingError(String.format("invalid rule %s", plainRule));
    }
    
    public boolean risk(String filename, boolean isDirectory) {
        Result result = this.check(filename, isDirectory, RuleType.DELETION);
        if (result == Result.EXCLUDED) {
            return false;
        }
        
        return true;
    }
    
    public boolean show(String filename, boolean isDirectory) {
        Result result = this.check(filename, isDirectory, RuleType.HIDING);
        if (result == Result.EXCLUDED) {
            return false;
        }
        
        return true;
    }
    
    @Override
    public String toString() {
        
        StringBuilder buf = new StringBuilder();
        
        buf.append("dir=").append(this._dirname).append("; ");
        buf.append("rules=[").append(this._localRuleList.toString()).append("]; ");
        buf.append("deletion_rules=[").append(this._deletionRuleList.toString()).append("]; ");
        buf.append("hiding_rules=[").append(this._hidingRuleList.toString()).append("]; ");
        buf.append("inheritance=").append(new Boolean(this._inheritance).toString()).append("; ");
        if (this._dirMergeFilename != null) {
            buf.append("dirMergeFilename=").append(this._dirMergeFilename).append("; ");
        }
        
        if (this._parentRuleConfiguration != null) {
            buf.append("parent=").append(this._parentRuleConfiguration.toString());
        }
        
        return buf.toString();
    }
}

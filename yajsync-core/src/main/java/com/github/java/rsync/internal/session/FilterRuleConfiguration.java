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
package com.github.java.rsync.internal.session;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import com.github.java.rsync.internal.session.FilterRuleList.Result;
import com.github.java.rsync.internal.util.ArgumentParsingError;

public class FilterRuleConfiguration {

    private class Modifier {
        boolean dirMerge;
        boolean exclude;
        boolean excludeMergeFilename;
        boolean hide;
        boolean include;
        boolean merge;
        boolean noInheritanceOfRules;
        boolean protect;
        boolean risk;
        boolean show;

        public void checkValidity(String plainRule) throws ArgumentParsingError {
            if (merge && dirMerge || include && exclude || protect && risk) {
                throw new ArgumentParsingError(String.format("invalid combination of modifiers in rule %s (processing %s)", plainRule, dirname));
            }
        }
    }

    public enum RuleType {
        DELETION, EXCLUSION, HIDING
    }

    private final FilterRuleList deletionRuleList = new FilterRuleList();
    private String dirMergeFilename = null;
    private String dirname = null;
    private final FilterRuleList hidingRuleList = new FilterRuleList();
    private boolean inheritance = true;

    // FSTODO: CustomFileSystem.getConfigPath( ... )

    private FilterRuleList localRuleList = new FilterRuleList();

    private FilterRuleConfiguration parentRuleConfiguration = null;

    public FilterRuleConfiguration() {

    }

    public FilterRuleConfiguration(FilterRuleConfiguration parentRuleConfiguration, Path directory) throws ArgumentParsingError {

        this.parentRuleConfiguration = parentRuleConfiguration;
        if (this.parentRuleConfiguration != null) {
            inheritance = this.parentRuleConfiguration.isInheritance();
            dirMergeFilename = this.parentRuleConfiguration.getDirMergeFilename();
        }
        dirname = directory.toString();

        if (dirMergeFilename != null && new File(dirname + "/" + dirMergeFilename).exists()) {
            // merge local filter rule file
            readRule(". " + dirname + "/" + dirMergeFilename);
        }
    }

    public FilterRuleConfiguration(List<String> inputFilterRules) throws ArgumentParsingError {
        for (String inputFilterRule : inputFilterRules) {
            readRule(inputFilterRule);
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

        assureDirectoryPathname(filename, isDirectory);

        Result result;

        switch (ruleType) {
            case EXCLUSION:
                result = localRuleList.check(filename, isDirectory);
                break;
            case DELETION:
                result = deletionRuleList.check(filename, isDirectory);
                break;
            case HIDING:
                result = hidingRuleList.check(filename, isDirectory);
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
        Result result = check(filename, isDirectory, RuleType.EXCLUSION);
        if (result == Result.EXCLUDED) {
            return true;
        }

        return false;
    }

    public String getDirMergeFilename() {
        return dirMergeFilename;
    }

    public FilterRuleList getFilterRuleListForSending() {
        return new FilterRuleList().addList(localRuleList).addList(deletionRuleList);
    }

    public FilterRuleConfiguration getParentRuleConfiguration() {
        return parentRuleConfiguration;
    }

    public boolean hide(String filename, boolean isDirectory) {
        Result result = check(filename, isDirectory, RuleType.HIDING);
        if (result == Result.EXCLUDED) {
            return true;
        }

        return false;
    }

    public boolean include(String filename, boolean isDirectory) {
        Result result = check(filename, isDirectory, RuleType.EXCLUSION);
        if (result == Result.EXCLUDED) {
            return false;
        }

        return true;
    }

    public boolean isFilterAvailable() {
        boolean result = localRuleList._rules.size() > 0 || deletionRuleList._rules.size() > 0 || hidingRuleList._rules.size() > 0;
        if (!result && inheritance && parentRuleConfiguration != null) {
            result = parentRuleConfiguration.isFilterAvailable();
        }
        return result;
    }

    public boolean isInheritance() {
        return inheritance;
    }

    public boolean protect(String filename, boolean isDirectory) {
        Result result = check(filename, isDirectory, RuleType.DELETION);
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
                m.exclude = true;
                i++;
                continue;
            } else if (c == '+') {
                // include rule
                m.include = true;
                i++;
                continue;
            }

            if (i > 0) {
                if (c == 'e') {
                    // exclude the merge-file name from the transfer
                    m.excludeMergeFilename = true;
                    i++;
                    continue;
                } else if (c == 'n') {
                    // don't inherit rules
                    m.noInheritanceOfRules = true;
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
                m.merge = true;
                i++;
                continue;
            } else if (c == ':') {
                // dir-merge
                m.dirMerge = true;
                i++;
                continue;
            } else if (c == 'm' && i + 5 <= modifier.length() && "merge".equals(modifier.substring(i, i + 5))) {
                // merge
                m.merge = true;
                i += 5;
                continue;
            } else if (c == 'd' && i + 9 <= modifier.length() && "dir-merge".equals(modifier.substring(i, i + 9))) {
                // dir-merge
                m.dirMerge = true;
                i += 9;
                continue;
            } else if (c == 'P') {
                // protect
                m.protect = true;
                i++;
                continue;
            } else if (c == 'p' && i + 7 <= modifier.length() && "protect".equals(modifier.substring(i, i + 7))) {
                // protect
                m.protect = true;
                i += 7;
                continue;
            } else if (c == 'R') {
                // risk
                m.risk = true;
                i++;
                continue;
            } else if (c == 'r' && i + 4 <= modifier.length() && "risk".equals(modifier.substring(i, i + 4))) {
                // risk
                m.risk = true;
                i += 4;
                continue;
            } else if (c == 'H') {
                // hide
                m.hide = true;
                i++;
                continue;
            } else if (c == 'h' && i + 4 <= modifier.length() && "hide".equals(modifier.substring(i, i + 4))) {
                // hide
                m.hide = true;
                i += 4;
                continue;
            } else if (c == 'S') {
                // show
                m.show = true;
                i++;
                continue;
            } else if (c == 's' && i + 4 <= modifier.length() && "show".equals(modifier.substring(i, i + 4))) {
                // show
                m.show = true;
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
            localRuleList = new FilterRuleList();
            // clearing refers to inclusion/exclusion lists only
            // deletionRuleList = new FilterRuleList();
            // hidingRuleList = new FilterRuleList();
            parentRuleConfiguration = null;
            return;
        }

        if (splittedRule.length != 2) {
            throw new ArgumentParsingError(String.format("failed to parse filter rule '%s', invalid format: should be '<+|-|merge|dir-merge>,<modifier> <filename|path-expression>'", plainRule));
        }

        Modifier m = readModifiers(splittedRule[0].trim(), plainRule);
        m.checkValidity(plainRule);

        if (m.merge == true || m.dirMerge == true) {

            if (m.noInheritanceOfRules == true) {
                inheritance = false;
            }

            if (m.merge == true) {

                // _mergeRuleList.add(new MergeRule(m, splittedRule[1].trim()));

                // String _mergeFilename = splittedRule[1].trim();
                Path _mergeFilename = Paths.get(splittedRule[1].trim());
                Path _absoluteMergeFilename = _mergeFilename;
                if (!_absoluteMergeFilename.isAbsolute()) {
                    _absoluteMergeFilename = Paths.get(dirname, splittedRule[1].trim());
                }

                try (BufferedReader br = new BufferedReader(new FileReader(_absoluteMergeFilename.toString()))) {
                    String line = br.readLine();
                    while (line != null) {
                        line = line.trim();
                        // ignore empty lines or comments
                        if (line.length() != 0 && !line.startsWith("#")) {

                            if (m.exclude == true) {
                                localRuleList.addRule("- " + line);
                            } else if (m.include == true) {
                                localRuleList.addRule("+ " + line);
                            } else {
                                readRule(line);
                            }
                        }
                        line = br.readLine();
                    }

                    if (m.excludeMergeFilename && _mergeFilename != null) {
                        localRuleList.addRule("- " + _mergeFilename);
                    }

                } catch (IOException e) {
                    throw new ArgumentParsingError(String.format("impossible to parse filter file '%s'", _mergeFilename));
                }

                return;
            }

            if (dirMergeFilename == null && m.dirMerge == true) {
                dirMergeFilename = splittedRule[1].trim();
            }

            if (m.excludeMergeFilename && dirMergeFilename != null) {
                localRuleList.addRule("- " + dirMergeFilename);
            }

            return;
        }

        if (m.exclude == true) {
            localRuleList.addRule("- " + splittedRule[1].trim());
            return;
        } else if (m.include == true) {
            localRuleList.addRule("+ " + splittedRule[1].trim());
            return;
        } else if (m.protect == true) {
            deletionRuleList.addRule("P " + splittedRule[1].trim());
            return;
        } else if (m.risk == true) {
            deletionRuleList.addRule("R " + splittedRule[1].trim());
            return;
        } else if (m.hide == true) {
            hidingRuleList.addRule("H " + splittedRule[1].trim());
            return;
        } else if (m.show == true) {
            hidingRuleList.addRule("S " + splittedRule[1].trim());
            return;
        }

        throw new ArgumentParsingError(String.format("invalid rule %s", plainRule));
    }

    public boolean risk(String filename, boolean isDirectory) {
        Result result = check(filename, isDirectory, RuleType.DELETION);
        if (result == Result.EXCLUDED) {
            return false;
        }

        return true;
    }

    public boolean show(String filename, boolean isDirectory) {
        Result result = check(filename, isDirectory, RuleType.HIDING);
        if (result == Result.EXCLUDED) {
            return false;
        }

        return true;
    }

    @Override
    public String toString() {

        StringBuilder buf = new StringBuilder();

        buf.append("dir=").append(dirname).append("; ");
        buf.append("rules=[").append(localRuleList.toString()).append("]; ");
        buf.append("deletion_rules=[").append(deletionRuleList.toString()).append("]; ");
        buf.append("hiding_rules=[").append(hidingRuleList.toString()).append("]; ");
        buf.append("inheritance=").append(new Boolean(inheritance).toString()).append("; ");
        if (dirMergeFilename != null) {
            buf.append("dirMergeFilename=").append(dirMergeFilename).append("; ");
        }

        if (parentRuleConfiguration != null) {
            buf.append("parent=").append(parentRuleConfiguration.toString());
        }

        return buf.toString();
    }
}

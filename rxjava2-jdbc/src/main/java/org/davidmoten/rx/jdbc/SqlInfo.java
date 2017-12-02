package org.davidmoten.rx.jdbc;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

final class SqlInfo {
    private final String sql;
    private final List<String> names;
    private final int numQuestionMarks;

    SqlInfo(String sql, List<String> names) {
        this.sql = sql;
        this.names = names;
        if (names.isEmpty()) {
            numQuestionMarks = Util.countQuestionMarkParameters(sql);
        } else {
            numQuestionMarks = 0;
        }

    }

    String sql() {
        return sql;
    }

    int numParameters() {
        if (names.isEmpty()) {
            return numQuestionMarks;
        } else {
            return names.size();
        }
    }

    int numQuestionMarks() {
        return numQuestionMarks;
    }

    List<String> names() {
        return names;
    }

    static SqlInfo parse(String namedSql) {
        // was originally using regular expressions, but they didn't work well
        // for ignoring parameter-like strings inside quotes.
        List<String> names = new ArrayList<String>();
        int length = namedSql.length();
        StringBuilder parsedQuery = new StringBuilder(length);
        boolean inSingleQuote = false;
        boolean inDoubleQuote = false;
        for (int i = 0; i < length; i++) {
            char c = namedSql.charAt(i);
            if (inSingleQuote) {
                if (c == '\'') {
                    inSingleQuote = false;
                }
            } else if (inDoubleQuote) {
                if (c == '"') {
                    inDoubleQuote = false;
                }
            } else {
                if (c == '\'') {
                    inSingleQuote = true;
                } else if (c == '"') {
                    inDoubleQuote = true;
                } else if (c == ':' && i + 1 < length && !isFollowedOrPrefixedByColon(namedSql, i)
                        && Character.isJavaIdentifierStart(namedSql.charAt(i + 1))) {
                    int j = i + 2;
                    while (j < length && Character.isJavaIdentifierPart(namedSql.charAt(j))) {
                        j++;
                    }
                    String name = namedSql.substring(i + 1, j);
                    c = '?'; // replace the parameter with a question mark
                    i += name.length(); // skip past the end if the parameter
                    names.add(name);
                }
            }
            parsedQuery.append(c);
        }
        return new SqlInfo(parsedQuery.toString(), names);
    }

    static String expandQuestionMarks(String sql, List<Parameter> parameters) {
        if (!hasCollection(parameters)) {
            return sql;
        }
        int length = sql.length();
        StringBuilder parsedQuery = new StringBuilder(length);
        boolean inSingleQuote = false;
        boolean inDoubleQuote = false;
        int count = 0;
        for (int i = 0; i < length; i++) {
            char c = sql.charAt(i);
            String s = String.valueOf(c);
            if (inSingleQuote) {
                if (c == '\'') {
                    inSingleQuote = false;
                }
            } else if (inDoubleQuote) {
                if (c == '"') {
                    inDoubleQuote = false;
                }
            } else {
                if (c == '\'') {
                    inSingleQuote = true;
                } else if (c == '"') {
                    inDoubleQuote = true;
                } else if (c == '?') {
                    count++;
                    int size = parameters.get(count - 1).size();
                    if (size > 1) {
                        s = IntStream //
                                .range(0, size) //
                                .mapToObj(x -> "?") //
                                .collect(Collectors.joining(","));
                    }
                }
            }
            parsedQuery.append(s);
        }
        return parsedQuery.toString();
    }

    private static boolean hasCollection(List<Parameter> parameters) {
        for (Parameter p : parameters) {
            if (p.isCollection()) {
                return true;
            }
        }
        return false;
    }

    // Visible for testing
    static boolean isFollowedOrPrefixedByColon(String sql, int i) {
        return ':' == sql.charAt(i + 1) || (i > 0 && ':' == sql.charAt(i - 1));
    }

}

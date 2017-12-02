package org.davidmoten.rx.jdbc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

    static SqlInfo parse(String namedSql, List<Parameter> parameters) {
        // was originally using regular expressions, but they didn't work well
        // for ignoring parameter-like strings inside quotes.
        List<String> names = new ArrayList<String>();
        String sql = collectNamesAndConvertToQuestionMarks(namedSql, names, parameters);
        return new SqlInfo(sql, names);
    }

    static SqlInfo parse(String namedSql) {
        return parse(namedSql, Collections.emptyList());
    }

    private static String collectNamesAndConvertToQuestionMarks(String namedSql, List<String> names,
            List<Parameter> parameters) {

        Map<String, Parameter> map = new HashMap<>();
        for (Parameter p : parameters) {
            if (p.hasName()) {
                map.put(p.name(), p);
            }
        }
        int length = namedSql.length();
        StringBuilder parsedQuery = new StringBuilder(length);
        boolean inSingleQuote = false;
        boolean inDoubleQuote = false;
        int count = 0;
        for (int i = 0; i < length; i++) {
            char c = namedSql.charAt(i);
            StringBuilder s = new StringBuilder();
            if (inSingleQuote) {
                if (c == '\'') {
                    inSingleQuote = false;
                }
                s.append(c);
            } else if (inDoubleQuote) {
                if (c == '"') {
                    inDoubleQuote = false;
                }
                s.append(c);
            } else {
                if (c == '\'') {
                    inSingleQuote = true;
                    s.append(c);
                } else if (c == '"') {
                    inDoubleQuote = true;
                    s.append(c);
                } else if (c == ':' && i + 1 < length && !isFollowedOrPrefixedByColon(namedSql, i)
                        && Character.isJavaIdentifierStart(namedSql.charAt(i + 1))) {
                    count++;
                    int j = i + 2;
                    while (j < length && Character.isJavaIdentifierPart(namedSql.charAt(j))) {
                        j++;
                    }
                    String name = namedSql.substring(i + 1, j);
                    if (!parameters.isEmpty()) {
                        Parameter p = map.get(name);
                        s.append(IntStream.range(0, p.size()).mapToObj(x -> "?")
                                .collect(Collectors.joining(",")));
                    } else {
                        s.append("?"); // replace the parameter with a question mark
                    }
                    names.add(name);
                    i += name.length(); // skip past the end if the parameter
                } else if (c == '?') {
                    count++;
                    if (!parameters.isEmpty()) {
                        Parameter p = parameters.get(count - 1);
                        s.append(IntStream.range(0, p.size()).mapToObj(x -> "?")
                                .collect(Collectors.joining(",")));
                    } else {
                        s.append(c);
                    }
                } else {
                    s.append(c);
                }
            }
            parsedQuery.append(s.toString());
        }
        return parsedQuery.toString();
    }

    // Visible for testing
    static boolean isFollowedOrPrefixedByColon(String sql, int i) {
        return ':' == sql.charAt(i + 1) || (i > 0 && ':' == sql.charAt(i - 1));
    }

}

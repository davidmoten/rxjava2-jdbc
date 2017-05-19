package org.davidmoten.rx.jdbc;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public final class Sql {

    private Sql() {

    }

    private static final String UTF8 = "UTF-8";

    public static List<String> statements(InputStream is, String delimiter) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        byte[] b = new byte[8192];
        while (true) {
            try {
                int len = is.read(b);
                if (len == -1) {
                    break;
                } else {
                    bytes.write(b, 0, len);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        try {
            String s = bytes.toString(UTF8);
            //trim comment lines starting with --
            s = Arrays.stream(s.split("\n")).filter(line -> !line.startsWith("--")).collect(Collectors.joining("\n"));
            String[] statements = s.split(delimiter);
            return Arrays.asList(statements);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    public static List<String> statements(InputStream is) {
        return statements(is, ";");
    }
}

package com.distributedfs;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A small, strict JSON reader for the tests of the web console's endpoints (no library): objects
 * become {@code Map<String, Object>} in field order, arrays {@code List<Object>}, numbers
 * {@code Long}, and {@code true}, {@code false} and {@code null} themselves. Anything that is not
 * exactly one valid JSON value (with only the number forms the server writes: integers) fails the
 * test, so a test also checks that the server's answer is well-formed.
 */
final class TestJson {

    private final String text;
    private int pos;

    private TestJson(String text) {
        this.text = text;
    }

    static Object parse(String text) {
        TestJson reader = new TestJson(text);
        Object value = reader.value();
        reader.space();
        NodeTestKit.check(reader.pos == text.length(), "text after the JSON value at " + reader.pos + ": " + text);
        return value;
    }

    @SuppressWarnings("unchecked")
    static Map<String, Object> object(String text) {
        Object value = parse(text);
        NodeTestKit.check(value instanceof Map, "not a JSON object: " + text);
        return (Map<String, Object>) value;
    }

    @SuppressWarnings("unchecked")
    static Map<String, Object> map(Object value) {
        NodeTestKit.check(value instanceof Map, "not an object: " + value);
        return (Map<String, Object>) value;
    }

    @SuppressWarnings("unchecked")
    static List<Object> list(Object value) {
        NodeTestKit.check(value instanceof List, "not an array: " + value);
        return (List<Object>) value;
    }

    /** The objects of an array field. */
    static List<Map<String, Object>> objects(Map<String, Object> parent, String key) {
        List<Map<String, Object>> out = new ArrayList<>();
        for (Object item : list(parent.get(key))) {
            out.add(map(item));
        }
        return out;
    }

    static long number(Map<String, Object> parent, String key) {
        Object value = parent.get(key);
        NodeTestKit.check(value instanceof Long, "\"" + key + "\" is not a number: " + value);
        return (Long) value;
    }

    private Object value() {
        space();
        NodeTestKit.check(pos < text.length(), "the JSON text ends early: " + text);
        char c = text.charAt(pos);
        if (c == '{') {
            pos++;
            Map<String, Object> map = new LinkedHashMap<>();
            space();
            if (peek('}')) {
                return map;
            }
            do {
                space();
                String key = string();
                space();
                expect(':');
                NodeTestKit.check(!map.containsKey(key), "duplicate key \"" + key + "\" in " + text);
                map.put(key, value());
                space();
            } while (peek(','));
            expect('}');
            return map;
        }
        if (c == '[') {
            pos++;
            List<Object> list = new ArrayList<>();
            space();
            if (peek(']')) {
                return list;
            }
            do {
                list.add(value());
                space();
            } while (peek(','));
            expect(']');
            return list;
        }
        if (c == '"') {
            return string();
        }
        for (String word : new String[] {"true", "false", "null"}) {
            if (text.startsWith(word, pos)) {
                pos += word.length();
                return word.equals("null") ? null : Boolean.valueOf(word);
            }
        }
        int start = pos;
        if (c == '-') {
            pos++;
        }
        while (pos < text.length() && Character.isDigit(text.charAt(pos))) {
            pos++;
        }
        NodeTestKit.check(pos > start && Character.isDigit(text.charAt(pos - 1)), "bad value at " + start + ": " + text);
        return Long.parseLong(text.substring(start, pos));
    }

    private String string() {
        expect('"');
        StringBuilder out = new StringBuilder();
        while (true) {
            NodeTestKit.check(pos < text.length(), "unterminated string: " + text);
            char c = text.charAt(pos++);
            if (c == '"') {
                return out.toString();
            }
            NodeTestKit.check(c >= 0x20, "a control character inside a string: " + text);
            if (c != '\\') {
                out.append(c);
                continue;
            }
            char e = text.charAt(pos++);
            switch (e) {
                case '"', '\\', '/' -> out.append(e);
                case 'b' -> out.append('\b');
                case 'f' -> out.append('\f');
                case 'n' -> out.append('\n');
                case 'r' -> out.append('\r');
                case 't' -> out.append('\t');
                case 'u' -> {
                    out.append((char) Integer.parseInt(text.substring(pos, pos + 4), 16));
                    pos += 4;
                }
                default -> throw new AssertionError("bad escape \\" + e + " in " + text);
            }
        }
    }

    private void space() {
        while (pos < text.length() && " \t\r\n".indexOf(text.charAt(pos)) >= 0) {
            pos++;
        }
    }

    private boolean peek(char c) {
        if (pos < text.length() && text.charAt(pos) == c) {
            pos++;
            return true;
        }
        return false;
    }

    private void expect(char c) {
        NodeTestKit.check(pos < text.length() && text.charAt(pos) == c, "expected '" + c + "' at " + pos + ": " + text);
        pos++;
    }
}

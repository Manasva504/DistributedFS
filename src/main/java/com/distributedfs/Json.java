package com.distributedfs;

import java.util.HexFormat;
import java.util.Objects;

/**
 * The small amount of JSON writing the HTTP API needs, without a JSON library.
 *
 * <p>Every response body the API sends is a flat object whose values are numbers or strings, so
 * the only part that needs care is turning a Java string into a JSON string literal. That is
 * what {@link #quote(String)} does.
 */
final class Json {

    private static final HexFormat HEX = HexFormat.of();

    private Json() {
    }

    /**
     * Returns {@code value} as a JSON string literal, including the surrounding quotes.
     *
     * <p>The result is always printable ASCII. {@code "} and {@code \} are escaped with a
     * backslash, and every other character outside printable ASCII (control characters such as
     * line breaks, DEL, and all non-ASCII text) becomes a six-character {@code \}{@code uXXXX}
     * escape. A character outside the Basic Multilingual Plane, such as an emoji, is two UTF-16
     * units in a Java string, and each unit gets its own escape, which is exactly how JSON
     * (RFC 8259) writes such a character. Because the output is pure ASCII, no character
     * encoding along the way can damage it.
     *
     * <p>An unpaired surrogate is escaped like any other unit. RFC 8259's grammar allows that,
     * but some JSON readers reject it; the API only quotes text it controls, which has none.
     *
     * @throws NullPointerException if {@code value} is null
     */
    static String quote(String value) {
        Objects.requireNonNull(value, "value must not be null");
        StringBuilder out = new StringBuilder(value.length() + 2);
        out.append('"');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == '"' || c == '\\') {
                out.append('\\').append(c);
            } else if (c >= 0x20 && c <= 0x7E) {
                out.append(c);
            } else {
                out.append("\\u").append(HEX.toHexDigits(c));
            }
        }
        out.append('"');
        return out.toString();
    }
}

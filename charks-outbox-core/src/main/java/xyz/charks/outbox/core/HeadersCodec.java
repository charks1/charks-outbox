package xyz.charks.outbox.core;

import org.jspecify.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for serializing and deserializing event headers to/from JSON.
 *
 * <p>This codec provides a lightweight JSON encoding for string-to-string maps
 * without requiring external JSON libraries. It handles proper escaping of
 * special characters in keys and values.
 *
 * <p>Example usage:
 * <pre>{@code
 * Map<String, String> headers = Map.of("key", "value");
 * String json = HeadersCodec.serialize(headers);
 * Map<String, String> restored = HeadersCodec.deserialize(json);
 * }</pre>
 */
public final class HeadersCodec {

    private HeadersCodec() {
        // Utility class
    }

    /**
     * Serializes a map of headers to a JSON string.
     *
     * @param headers the headers map to serialize
     * @return JSON string representation of the headers
     */
    public static String serialize(Map<String, String> headers) {
        if (headers.isEmpty()) {
            return "{}";
        }
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            if (!first) {
                sb.append(",");
            }
            sb.append("\"").append(escapeJson(entry.getKey())).append("\":");
            sb.append("\"").append(escapeJson(entry.getValue())).append("\"");
            first = false;
        }
        sb.append("}");
        return sb.toString();
    }

    /**
     * Deserializes a JSON string to a map of headers.
     *
     * @param json the JSON string to deserialize, may be null
     * @return immutable map of headers, empty if json is null or empty
     */
    public static Map<String, String> deserialize(@Nullable String json) {
        if (json == null || json.isBlank() || json.equals("{}")) {
            return Map.of();
        }

        Map<String, String> headers = new HashMap<>();
        String content = json.trim();
        if (content.startsWith("{") && content.endsWith("}")) {
            content = content.substring(1, content.length() - 1);
        }

        if (content.isBlank()) {
            return Map.of();
        }

        int i = 0;
        while (i < content.length()) {
            while (i < content.length() && Character.isWhitespace(content.charAt(i))) i++;
            if (i >= content.length()) break;

            if (content.charAt(i) != '"') break;
            i++;

            StringBuilder key = new StringBuilder();
            while (i < content.length() && content.charAt(i) != '"') {
                if (content.charAt(i) == '\\' && i + 1 < content.length()) {
                    i++;
                    key.append(unescapeChar(content.charAt(i)));
                } else {
                    key.append(content.charAt(i));
                }
                i++;
            }
            i++;

            while (i < content.length() && (content.charAt(i) == ':' || Character.isWhitespace(content.charAt(i)))) i++;

            if (i >= content.length() || content.charAt(i) != '"') break;
            i++;

            StringBuilder value = new StringBuilder();
            while (i < content.length() && content.charAt(i) != '"') {
                if (content.charAt(i) == '\\' && i + 1 < content.length()) {
                    i++;
                    value.append(unescapeChar(content.charAt(i)));
                } else {
                    value.append(content.charAt(i));
                }
                i++;
            }
            i++;

            headers.put(key.toString(), value.toString());

            while (i < content.length() && (content.charAt(i) == ',' || Character.isWhitespace(content.charAt(i)))) i++;
        }

        return Map.copyOf(headers);
    }

    private static String escapeJson(String value) {
        return value
                .replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }

    private static char unescapeChar(char c) {
        return switch (c) {
            case 'n' -> '\n';
            case 'r' -> '\r';
            case 't' -> '\t';
            default -> c;
        };
    }
}

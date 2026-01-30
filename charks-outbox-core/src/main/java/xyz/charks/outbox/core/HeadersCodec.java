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

        int[] pos = {0};
        while (pos[0] < content.length()) {
            skipWhitespace(content, pos);
            if (pos[0] >= content.length() || content.charAt(pos[0]) != '"') break;

            String key = parseQuotedString(content, pos);
            if (key == null) break;

            skipColonAndWhitespace(content, pos);
            if (pos[0] >= content.length() || content.charAt(pos[0]) != '"') break;

            String value = parseQuotedString(content, pos);
            if (value == null) break;

            headers.put(key, value);

            skipCommaAndWhitespace(content, pos);
        }

        return Map.copyOf(headers);
    }

    private static void skipWhitespace(String content, int[] pos) {
        while (pos[0] < content.length() && Character.isWhitespace(content.charAt(pos[0]))) {
            pos[0]++;
        }
    }

    private static void skipColonAndWhitespace(String content, int[] pos) {
        while (pos[0] < content.length() &&
               (content.charAt(pos[0]) == ':' || Character.isWhitespace(content.charAt(pos[0])))) {
            pos[0]++;
        }
    }

    private static void skipCommaAndWhitespace(String content, int[] pos) {
        while (pos[0] < content.length() &&
               (content.charAt(pos[0]) == ',' || Character.isWhitespace(content.charAt(pos[0])))) {
            pos[0]++;
        }
    }

    private static @Nullable String parseQuotedString(String content, int[] pos) {
        if (pos[0] >= content.length() || content.charAt(pos[0]) != '"') {
            return null;
        }
        pos[0]++;

        StringBuilder result = new StringBuilder();
        while (pos[0] < content.length() && content.charAt(pos[0]) != '"') {
            if (content.charAt(pos[0]) == '\\' && pos[0] + 1 < content.length()) {
                pos[0]++;
                result.append(unescapeChar(content.charAt(pos[0])));
            } else {
                result.append(content.charAt(pos[0]));
            }
            pos[0]++;
        }
        pos[0]++;
        return result.toString();
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

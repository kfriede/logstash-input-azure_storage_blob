package com.azure.logstash.input.config;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses human-readable size strings (e.g., "500GB", "1TB") into byte counts.
 * Uses binary units (powers of 1024).
 */
public final class SizeParser {

    private static final Pattern SIZE_PATTERN =
            Pattern.compile("^\\s*([0-9]+(?:\\.[0-9]+)?)\\s*(MB|GB|TB)\\s*$",
                    Pattern.CASE_INSENSITIVE);

    private SizeParser() {
        // utility class
    }

    /**
     * Parses a human-readable size string into bytes.
     *
     * @param value size string like "500GB", "1TB", "100MB", or "0" (disabled)
     * @return the size in bytes, or 0 if disabled
     * @throws IllegalArgumentException if the string cannot be parsed
     */
    public static long parseBytes(String value) {
        if (value == null) {
            throw new IllegalArgumentException("Size value cannot be null");
        }

        String trimmed = value.trim();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException("Size value cannot be empty");
        }

        if ("0".equals(trimmed)) {
            return 0L;
        }

        Matcher matcher = SIZE_PATTERN.matcher(trimmed);
        if (!matcher.matches()) {
            throw new IllegalArgumentException(
                    "Invalid size value '" + value + "'. "
                            + "Expected format: <number><MB|GB|TB> (e.g., '500GB', '1TB') or '0' to disable");
        }

        double number = Double.parseDouble(matcher.group(1));
        if (number < 0) {
            throw new IllegalArgumentException("Size value cannot be negative: " + value);
        }

        String unit = matcher.group(2).toUpperCase();
        long multiplier;
        switch (unit) {
            case "MB": multiplier = 1024L * 1024; break;
            case "GB": multiplier = 1024L * 1024 * 1024; break;
            case "TB": multiplier = 1024L * 1024 * 1024 * 1024; break;
            default:
                throw new IllegalArgumentException("Unknown unit: " + unit);
        }

        return (long) (number * multiplier);
    }
}

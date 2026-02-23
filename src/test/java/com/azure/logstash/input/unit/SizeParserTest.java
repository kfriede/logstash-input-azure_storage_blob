package com.azure.logstash.input.unit;

import com.azure.logstash.input.config.SizeParser;
import org.junit.Test;
import static org.junit.Assert.*;

public class SizeParserTest {

    @Test
    public void testParseMB() {
        assertEquals(100L * 1024 * 1024, SizeParser.parseBytes("100MB"));
    }

    @Test
    public void testParseGB() {
        assertEquals(500L * 1024 * 1024 * 1024, SizeParser.parseBytes("500GB"));
    }

    @Test
    public void testParseTB() {
        assertEquals(1L * 1024 * 1024 * 1024 * 1024, SizeParser.parseBytes("1TB"));
    }

    @Test
    public void testParseCaseInsensitive() {
        assertEquals(500L * 1024 * 1024 * 1024, SizeParser.parseBytes("500gb"));
        assertEquals(100L * 1024 * 1024, SizeParser.parseBytes("100mb"));
        assertEquals(1L * 1024 * 1024 * 1024 * 1024, SizeParser.parseBytes("1tb"));
    }

    @Test
    public void testParseZeroDisabled() {
        assertEquals(0L, SizeParser.parseBytes("0"));
    }

    @Test
    public void testParseWithWhitespace() {
        assertEquals(500L * 1024 * 1024 * 1024, SizeParser.parseBytes("  500GB  "));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseNegative() {
        SizeParser.parseBytes("-500GB");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseEmptyString() {
        SizeParser.parseBytes("");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseGarbageString() {
        SizeParser.parseBytes("notasize");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseUnknownUnit() {
        SizeParser.parseBytes("500PB");
    }

    @Test
    public void testParseDecimal() {
        assertEquals((long) (1.5 * 1024 * 1024 * 1024), SizeParser.parseBytes("1.5GB"));
    }
}

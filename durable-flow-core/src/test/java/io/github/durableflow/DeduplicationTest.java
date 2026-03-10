package io.github.durableflow;

import io.github.durableflow.api.InboundMessage;
import io.github.durableflow.spi.DefaultMessagePreprocessor;
import io.github.durableflow.spi.PreprocessResult;
import net.openhft.hashing.LongHashFunction;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DeduplicationTest {

    private static final DefaultMessagePreprocessor PREPROCESSOR = DefaultMessagePreprocessor.INSTANCE;

    @Test
    void samePayload_producesIdenticalHash() {
        byte[] payload = "hello world".getBytes(StandardCharsets.UTF_8);
        String hash1 = hash(payload);
        String hash2 = hash(payload);
        assertEquals(hash1, hash2);
    }

    @Test
    void differentPayloads_produceDifferentHashes() {
        String h1 = hash("msg1".getBytes(StandardCharsets.UTF_8));
        String h2 = hash("msg2".getBytes(StandardCharsets.UTF_8));
        assertNotEquals(h1, h2);
    }

    @Test
    void hashIs32HexChars() {
        String h = hash("test".getBytes(StandardCharsets.UTF_8));
        // XXH3 128-bit = 32 hex chars
        assertEquals(32, h.length());
        assertTrue(h.matches("[0-9a-f]+"), "Hash should be lowercase hex");
    }

    @Test
    void emptyPayload_producesValidHash() {
        String h = hash(new byte[0]);
        assertNotNull(h);
        assertFalse(h.isBlank());
    }

    @Test
    void preprocessor_passesPayloadThrough() {
        byte[] payload = "data".getBytes(StandardCharsets.UTF_8);
        InboundMessage msg = new InboundMessage("src", payload, Map.of("k", "v"));
        PreprocessResult result = PREPROCESSOR.preprocess(msg);
        assertArrayEquals(payload, result.getCanonicalBytes());
        assertArrayEquals(payload, result.getStoredPayload());
    }

    @Test
    void preprocessor_preservesHeaders() {
        InboundMessage msg = new InboundMessage("src", new byte[]{1}, Map.of("header1", "value1"));
        PreprocessResult result = PREPROCESSOR.preprocess(msg);
        assertEquals("value1", result.getMetadata().get("header1"));
    }

    // -------------------------------------------------------------------------
    // Helper – mirrors the hash computation in DurableFlowEngine
    // -------------------------------------------------------------------------

    private static String hash(byte[] bytes) {
        long hi = LongHashFunction.xx3().hashBytes(bytes);
        long lo = LongHashFunction.xx3(0xDEAD_BEEF_CAFE_BABAL).hashBytes(bytes);
        return String.format("%016x%016x", hi, lo);
    }
}

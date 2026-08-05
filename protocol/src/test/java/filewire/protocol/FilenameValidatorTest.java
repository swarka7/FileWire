package filewire.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FilenameValidatorTest {
    @Test
    void acceptsPortableUtf8Filename() throws Exception {
        assertEquals("résumé-נתונים.txt", FilenameValidator.requireValid("résumé-נתונים.txt"));
    }

    @Test
    void acceptsMaximumUtf8ByteLength() throws Exception {
        String filename = "a".repeat(ProtocolConstants.MAX_FILENAME_BYTES);
        assertEquals(filename, FilenameValidator.requireValid(filename));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "../outside.txt",
            "../../outside.txt",
            "/etc/passwd",
            "C:\\Windows\\secret.txt",
            "C:/Windows/secret.txt",
            "\\\\server\\share\\file.txt",
            ".",
            "..",
            "folder/file.txt",
            "folder\\file.txt",
            "star*.txt",
            "question?.txt",
            "less<than.txt",
            "greater>than.txt",
            "pipe|name.txt",
            "NUL",
            "con.txt",
            "trailing.",
            "trailing "
    })
    void rejectsUnsafeOrNonPortableFilename(String filename) {
        ProtocolException exception = assertThrows(
                ProtocolException.class,
                () -> FilenameValidator.requireValid(filename));
        assertEquals(ErrorCode.INVALID_FILENAME, exception.errorCode());
    }

    @Test
    void rejectsBlankAndOversizedFilenames() {
        assertThrows(ProtocolException.class, () -> FilenameValidator.requireValid("  "));
        assertThrows(
                ProtocolException.class,
                () -> FilenameValidator.requireValid("quote" + (char) 34 + ".txt"));
        assertThrows(ProtocolException.class, () -> FilenameValidator.requireValid(
                "a".repeat(ProtocolConstants.MAX_FILENAME_BYTES + 1)));
        assertThrows(ProtocolException.class, () -> FilenameValidator.requireValid("bad\u0000name"));
    }

    @Test
    void countsUtf8BytesRatherThanCharacters() {
        String multiByte = "界".repeat(86); // 258 UTF-8 bytes
        assertThrows(ProtocolException.class, () -> FilenameValidator.requireValid(multiByte));
    }
}

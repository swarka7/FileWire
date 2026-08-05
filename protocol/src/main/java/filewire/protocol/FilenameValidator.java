package filewire.protocol;

import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Set;

/** Validates portable, single-component remote filenames at the protocol boundary. */
public final class FilenameValidator {
    private static final Set<String> WINDOWS_RESERVED_NAMES = Set.of(
            "CON", "PRN", "AUX", "NUL",
            "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9",
            "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9");

    private FilenameValidator() {
    }

    public static String requireValid(String filename) throws ProtocolException {
        if (filename == null || filename.isBlank()) {
            throw invalid("Filename must not be blank");
        }
        if (filename.equals(".") || filename.equals("..")) {
            throw invalid("Filename must identify a regular file");
        }
        if (filename.indexOf('/') >= 0 || filename.indexOf('\\') >= 0 || filename.indexOf(':') >= 0) {
            throw invalid("Filename must be a single path component");
        }
        if (filename.endsWith(" ") || filename.endsWith(".")) {
            throw invalid("Filename must not end with a space or period");
        }
        for (int offset = 0; offset < filename.length();) {
            int codePoint = filename.codePointAt(offset);
            if (Character.isISOControl(codePoint)) {
                throw invalid("Filename must not contain control characters");
            }
            if (codePoint == '<'
                    || codePoint == '>'
                    || codePoint == 34
                    || codePoint == '|'
                    || codePoint == '?'
                    || codePoint == '*') {
                throw invalid("Filename contains a character that is not portable");
            }
            offset += Character.charCount(codePoint);
        }

        String baseName = filename;
        int dot = baseName.indexOf('.');
        if (dot >= 0) {
            baseName = baseName.substring(0, dot);
        }
        if (WINDOWS_RESERVED_NAMES.contains(baseName.toUpperCase(Locale.ROOT))) {
            throw invalid("Filename is reserved by a supported platform");
        }

        try {
            Path path = Path.of(filename);
            if (path.isAbsolute() || path.getNameCount() != 1) {
                throw invalid("Filename must be a relative single path component");
            }
        } catch (InvalidPathException exception) {
            throw new ProtocolException(
                    ErrorCode.INVALID_FILENAME,
                    ProtocolException.UNKNOWN_CORRELATION_ID,
                    false,
                    "Filename is not a valid path component",
                    exception);
        }

        int byteLength;
        try {
            byteLength = StandardCharsets.UTF_8.newEncoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .encode(CharBuffer.wrap(filename))
                    .remaining();
        } catch (CharacterCodingException exception) {
            throw new ProtocolException(
                    ErrorCode.INVALID_FILENAME,
                    ProtocolException.UNKNOWN_CORRELATION_ID,
                    false,
                    "Filename is not valid Unicode",
                    exception);
        }
        if (byteLength > ProtocolConstants.MAX_FILENAME_BYTES) {
            throw invalid("Filename exceeds " + ProtocolConstants.MAX_FILENAME_BYTES + " UTF-8 bytes");
        }
        return filename;
    }

    private static ProtocolException invalid(String message) {
        return new ProtocolException(ErrorCode.INVALID_FILENAME, message);
    }
}

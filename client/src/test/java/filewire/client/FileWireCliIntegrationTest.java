package filewire.client;

import filewire.server.FileWireServer;
import filewire.server.ServerConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FileWireCliIntegrationTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void scriptedCliPerformsTheDocumentedEndToEndWorkflow() throws Exception {
        byte[] content = new byte[131_089];
        new Random(42_424L).nextBytes(content);
        content[65_536] = 0;
        Path source = Path.of("target", "cli-smoke-source.bin");
        Files.createDirectories(source.getParent());
        Files.write(source, content);

        Path storage = temporaryDirectory.resolve("server-files");
        Path downloads = temporaryDirectory.resolve("downloads");
        FileWireServer server = new FileWireServer(ServerConfig.defaults(storage, 0));
        StringWriter standardOutput = new StringWriter();
        StringWriter errorOutput = new StringWriter();
        try {
            server.start();
            String commands = String.join("\n",
                    "connect 127.0.0.1 " + server.port(),
                    "upload target/cli-smoke-source.bin cli-smoke.bin",
                    "list",
                    "download cli-smoke.bin copy.bin",
                    "delete cli-smoke.bin",
                    "disconnect",
                    "exit",
                    "");
            FileWireCli cli = new FileWireCli(
                    new BufferedReader(new StringReader(commands)),
                    new PrintWriter(standardOutput, true),
                    new PrintWriter(errorOutput, true),
                    downloads,
                    false);

            cli.run();
        } finally {
            server.close();
            Files.deleteIfExists(source);
        }

        String transcript = standardOutput.toString();
        assertTrue(transcript.contains("Connected to 127.0.0.1:"));
        assertTrue(transcript.contains("Uploaded cli-smoke.bin"));
        assertTrue(transcript.contains("Downloaded cli-smoke.bin"));
        assertTrue(transcript.contains("Deleted cli-smoke.bin"));
        assertTrue(transcript.contains("Disconnected"));
        assertEquals("", errorOutput.toString());
        assertArrayEquals(content, Files.readAllBytes(downloads.resolve("copy.bin")));
        assertFalse(Files.exists(storage.resolve("cli-smoke.bin")));
        try (Stream<Path> parts = Files.list(storage.resolve(".filewire-tmp"))) {
            assertTrue(parts.findAny().isEmpty());
        }
        assertEquals(0, server.activeSessionCount());
    }
}

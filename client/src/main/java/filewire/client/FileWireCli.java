package filewire.client;

import filewire.protocol.ProtocolException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;

/** Interactive command-line interface for a single FileWire connection. */
public final class FileWireCli {
    private static final String HELP = """
            Commands:
              connect <host> <port>
              list
              upload <local-path> [remote-name]
              download <remote-name> [local-path]
              delete <remote-name>
              disconnect
              help
              exit

            Paths and filenames containing whitespace are not supported by this command parser.
            """;

    private final BufferedReader input;
    private final PrintWriter output;
    private final PrintWriter errors;
    private final Path downloadRoot;
    private final boolean showPrompt;
    private FileWireClient client;

    FileWireCli(
            BufferedReader input,
            PrintWriter output,
            PrintWriter errors,
            Path downloadRoot,
            boolean showPrompt) {
        this.input = input;
        this.output = output;
        this.errors = errors;
        this.downloadRoot = downloadRoot;
        this.showPrompt = showPrompt;
    }

    public static void main(String[] args) {
        PrintWriter output = new PrintWriter(
                System.out, true, StandardCharsets.UTF_8);
        PrintWriter errors = new PrintWriter(
                System.err, true, StandardCharsets.UTF_8);
        if (args.length > 1) {
            errors.println("Usage: FileWireCli [download-root]");
            return;
        }

        try {
            Path downloadRoot = args.length == 1 ? Path.of(args[0]) : Path.of("downloads");
            BufferedReader input = new BufferedReader(
                    new InputStreamReader(System.in, StandardCharsets.UTF_8));
            FileWireCli cli = new FileWireCli(
                    input, output, errors, downloadRoot, System.console() != null);
            cli.run();
        } catch (IllegalArgumentException invalidPath) {
            errors.println("Could not start FileWire client: " + invalidPath.getMessage());
        }
    }

    void run() {
        output.println("FileWire client. Type 'help' for commands.");
        try {
            boolean running = true;
            while (running) {
                if (showPrompt) {
                    output.print("filewire> ");
                    output.flush();
                }
                String line = input.readLine();
                if (line == null) {
                    break;
                }
                if (line.isBlank()) {
                    continue;
                }
                try {
                    running = execute(line.trim().split("\\s+"));
                } catch (RemoteOperationException remoteError) {
                    errors.printf(
                            "Remote error [%s]: %s%n",
                            remoteError.errorCode(),
                            remoteError.getMessage());
                } catch (ProtocolException protocolError) {
                    errors.printf(
                            "Protocol error [%s]: %s%n",
                            protocolError.errorCode(),
                            protocolError.getMessage());
                } catch (IllegalArgumentException | IllegalStateException invalidInput) {
                    errors.println("Error: " + invalidInput.getMessage());
                } catch (IOException ioError) {
                    errors.println("I/O error: " + ioError.getMessage());
                }
                if (client != null && !client.isConnected()) {
                    client = null;
                }
            }
        } catch (IOException inputFailure) {
            errors.println("Could not read a command: " + inputFailure.getMessage());
        } finally {
            closeClient();
        }
    }

    private boolean execute(String[] arguments) throws IOException {
        String command = arguments[0].toLowerCase(Locale.ROOT);
        return switch (command) {
            case "connect" -> {
                requireArity(arguments, 3, 3, "connect <host> <port>");
                if (client != null && client.isConnected()) {
                    throw new IllegalStateException("disconnect before connecting again");
                }
                int port;
                try {
                    port = Integer.parseInt(arguments[2]);
                } catch (NumberFormatException invalidPort) {
                    throw new IllegalArgumentException("port must be a number", invalidPort);
                }
                client = FileWireClient.connect(arguments[1], port, downloadRoot);
                output.printf("Connected to %s:%d%n", arguments[1], port);
                yield true;
            }
            case "list" -> {
                requireArity(arguments, 1, 1, "list");
                List<String> filenames = requireClient().listFiles();
                if (filenames.isEmpty()) {
                    output.println("(no files)");
                } else {
                    filenames.forEach(output::println);
                }
                yield true;
            }
            case "upload" -> {
                requireArity(arguments, 2, 3, "upload <local-path> [remote-name]");
                Path source = Path.of(arguments[1]);
                TransferReceipt receipt = arguments.length == 3
                        ? requireClient().upload(source, arguments[2])
                        : requireClient().upload(source);
                printReceipt("Uploaded", receipt);
                yield true;
            }
            case "download" -> {
                requireArity(arguments, 2, 3, "download <remote-name> [local-path]");
                TransferReceipt receipt = arguments.length == 3
                        ? requireClient().download(arguments[1], Path.of(arguments[2]))
                        : requireClient().download(arguments[1]);
                printReceipt("Downloaded", receipt);
                yield true;
            }
            case "delete" -> {
                requireArity(arguments, 2, 2, "delete <remote-name>");
                requireClient().delete(arguments[1]);
                output.println("Deleted " + arguments[1]);
                yield true;
            }
            case "disconnect" -> {
                requireArity(arguments, 1, 1, "disconnect");
                FileWireClient current = requireClient();
                try {
                    current.disconnect();
                    output.println("Disconnected");
                } finally {
                    client = null;
                }
                yield true;
            }
            case "help" -> {
                requireArity(arguments, 1, 1, "help");
                output.print(HELP);
                yield true;
            }
            case "exit" -> {
                requireArity(arguments, 1, 1, "exit");
                yield false;
            }
            default -> throw new IllegalArgumentException(
                    "unknown command '" + arguments[0] + "'; type 'help'");
        };
    }

    private FileWireClient requireClient() {
        if (client == null || !client.isConnected()) {
            throw new IllegalStateException("connect to a server first");
        }
        return client;
    }

    private void printReceipt(String action, TransferReceipt receipt) {
        output.printf(
                "%s %s (%d bytes, SHA-256 %s)%n",
                action,
                receipt.filename(),
                receipt.bytesTransferred(),
                receipt.sha256());
    }

    private static void requireArity(
            String[] arguments, int minimum, int maximum, String usage) {
        if (arguments.length < minimum || arguments.length > maximum) {
            throw new IllegalArgumentException("usage: " + usage);
        }
    }

    private void closeClient() {
        FileWireClient current = client;
        client = null;
        if (current == null) {
            return;
        }
        try {
            current.close();
        } catch (IOException closeFailure) {
            errors.println("Could not disconnect cleanly: " + closeFailure.getMessage());
        }
    }
}

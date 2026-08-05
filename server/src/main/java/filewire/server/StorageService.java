package filewire.server;

import filewire.protocol.AtomicFilePublisher;
import filewire.protocol.ErrorCode;
import filewire.protocol.FilenameValidator;
import filewire.protocol.ProtocolException;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/** Owns all filesystem access and keeps completed and temporary files inside one storage root. */
final class StorageService implements AutoCloseable {
    private static final System.Logger LOGGER = System.getLogger(StorageService.class.getName());
    static final String TEMP_DIRECTORY_NAME = ".filewire-tmp";
    static final String REPOSITORY_MARKER_NAME = ".gitkeep";

    private final Path root;
    private final Path temporaryRoot;
    private final long maximumFileSize;

    StorageService(Path configuredRoot, long maximumFileSize) throws IOException {
        if (maximumFileSize < 0) {
            throw new IllegalArgumentException("maximumFileSize must not be negative");
        }
        Path normalizedRoot = configuredRoot.toAbsolutePath().normalize();
        if (Files.isSymbolicLink(normalizedRoot)) {
            throw new IOException("Storage root must not be a symbolic link");
        }
        Files.createDirectories(normalizedRoot);
        if (!Files.isDirectory(normalizedRoot, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("Storage root is not a directory: " + normalizedRoot);
        }

        root = normalizedRoot.toRealPath(LinkOption.NOFOLLOW_LINKS);
        this.maximumFileSize = maximumFileSize;
        temporaryRoot = root.resolve(TEMP_DIRECTORY_NAME).normalize();
        if (!temporaryRoot.startsWith(root)) {
            throw new IOException("Temporary directory escaped the storage root");
        }
        if (Files.isSymbolicLink(temporaryRoot)) {
            throw new IOException("Temporary directory must not be a symbolic link");
        }
        Files.createDirectories(temporaryRoot);
        if (!Files.isDirectory(temporaryRoot, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("Temporary path is not a directory: " + temporaryRoot);
        }
        cleanupTemporaryFiles();
    }

    Path root() {
        return root;
    }

    Path temporaryRoot() {
        return temporaryRoot;
    }

    long maximumFileSize() {
        return maximumFileSize;
    }

    Path resolveFile(String filename) throws RequestException {
        try {
            FilenameValidator.requireValid(filename);
        } catch (filewire.protocol.ProtocolException exception) {
            throw new RequestException(ErrorCode.INVALID_FILENAME, exception.getMessage(), exception);
        }
        if (REPOSITORY_MARKER_NAME.equalsIgnoreCase(filename)) {
            throw new RequestException(ErrorCode.INVALID_FILENAME, "Filename is reserved by the storage service");
        }

        Path resolved = root.resolve(filename).normalize();
        if (!resolved.startsWith(root) || !root.equals(resolved.getParent())) {
            throw new RequestException(ErrorCode.INVALID_FILENAME, "Filename escapes the storage root");
        }
        return resolved;
    }

    List<String> listFiles() throws IOException {
        List<String> names = new ArrayList<>();
        try (DirectoryStream<Path> entries = Files.newDirectoryStream(root)) {
            for (Path entry : entries) {
                String filename = entry.getFileName().toString();
                if (!REPOSITORY_MARKER_NAME.equalsIgnoreCase(filename)
                        && Files.isRegularFile(entry, LinkOption.NOFOLLOW_LINKS)
                        && !Files.isSymbolicLink(entry)) {
                    try {
                        names.add(FilenameValidator.requireValid(filename));
                    } catch (ProtocolException invalidName) {
                        LOGGER.log(
                                System.Logger.Level.WARNING,
                                "Ignoring a storage entry with a non-portable filename");
                    }
                }
            }
        }
        names.sort(Comparator.naturalOrder());
        return List.copyOf(names);
    }

    void requireUploadTargetAvailable(Path target) throws RequestException {
        requireDirectChild(target);
        if (Files.exists(target, LinkOption.NOFOLLOW_LINKS)) {
            throw new RequestException(ErrorCode.FILE_ALREADY_EXISTS, "A file with that name already exists");
        }
    }

    DownloadSource openDownload(String filename) throws IOException {
        return openDownload(filename, () -> {
        });
    }

    DownloadSource openDownload(String filename, DownloadSource.Progress progress) throws IOException {
        Path path = resolveFile(filename);
        requireRegularFile(path);
        return DownloadSource.open(filename, path, maximumFileSize, progress);
    }

    void delete(String filename) throws IOException {
        Path path = resolveFile(filename);
        requireRegularFile(path);
        try {
            Files.delete(path);
        } catch (NoSuchFileException exception) {
            throw new RequestException(ErrorCode.FILE_NOT_FOUND, "File not found", exception);
        }
    }

    Path createTemporaryUpload(long transferId) throws IOException {
        if (transferId <= 0) {
            throw new IllegalArgumentException("transferId must be positive");
        }
        Path temporaryFile = Files.createTempFile(temporaryRoot, "upload-" + transferId + "-", ".part");
        if (!temporaryRoot.equals(temporaryFile.normalize().getParent())) {
            Files.deleteIfExists(temporaryFile);
            throw new IOException("Temporary upload escaped its dedicated directory");
        }
        return temporaryFile;
    }

    void commitTemporaryUpload(Path temporaryFile, Path target) throws IOException {
        requireTemporaryFile(temporaryFile);
        requireDirectChild(target);
        if (Files.exists(target, LinkOption.NOFOLLOW_LINKS)) {
            throw new RequestException(ErrorCode.FILE_ALREADY_EXISTS, "A file with that name already exists");
        }

        try {
            AtomicFilePublisher.publishCreateOnly(temporaryFile, target);
        } catch (FileAlreadyExistsException exception) {
            throw new RequestException(
                    ErrorCode.FILE_ALREADY_EXISTS,
                    "A file with that name already exists",
                    exception);
        }
    }

    void deleteTemporaryFile(Path temporaryFile) throws IOException {
        requireTemporaryPath(temporaryFile);
        Files.deleteIfExists(temporaryFile);
    }

    int temporaryFileCount() throws IOException {
        int count = 0;
        try (DirectoryStream<Path> entries = Files.newDirectoryStream(temporaryRoot)) {
            for (Path ignored : entries) {
                count++;
            }
        }
        return count;
    }

    void cleanupTemporaryFiles() throws IOException {
        IOException failure = null;
        try (DirectoryStream<Path> entries = Files.newDirectoryStream(temporaryRoot)) {
            for (Path entry : entries) {
                if (Files.isDirectory(entry, LinkOption.NOFOLLOW_LINKS) && !Files.isSymbolicLink(entry)) {
                    continue;
                }
                try {
                    Files.deleteIfExists(entry);
                } catch (IOException exception) {
                    if (failure == null) {
                        failure = exception;
                    } else {
                        failure.addSuppressed(exception);
                    }
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    @Override
    public void close() throws IOException {
        cleanupTemporaryFiles();
    }

    private void requireRegularFile(Path path) throws RequestException {
        requireDirectChild(path);
        if (!Files.exists(path, LinkOption.NOFOLLOW_LINKS)) {
            throw new RequestException(ErrorCode.FILE_NOT_FOUND, "File not found");
        }
        if (Files.isSymbolicLink(path) || !Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
            throw new RequestException(ErrorCode.INVALID_FILENAME, "Target is not a regular stored file");
        }
    }

    private void requireDirectChild(Path path) throws RequestException {
        Path normalized = path.toAbsolutePath().normalize();
        if (!normalized.startsWith(root) || !root.equals(normalized.getParent())) {
            throw new RequestException(ErrorCode.INVALID_FILENAME, "Path escapes the storage root");
        }
    }

    private void requireTemporaryPath(Path path) throws IOException {
        Path normalized = path.toAbsolutePath().normalize();
        if (!temporaryRoot.equals(normalized.getParent())) {
            throw new IOException("Refusing to access a temporary file outside the temporary directory");
        }
    }

    private void requireTemporaryFile(Path path) throws IOException {
        requireTemporaryPath(path);
        if (Files.isSymbolicLink(path) || !Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("Upload temporary path is not a regular file");
        }
    }

}

package bgu.spl.net.impl.tftp;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

public class FileStore {
    private final Path baseDir;

    public FileStore(Path baseDir) {
        this.baseDir = baseDir;
        ensureBaseDir();
    }

    public boolean exists(String filename) {
        ensureBaseDir();
        return baseDir.resolve(filename).toFile().exists();
    }

    public byte[] read(String filename) {
        ensureBaseDir();
        File file = baseDir.resolve(filename).toFile();
        byte[] data = new byte[(int) file.length()];
        try (FileInputStream fis = new FileInputStream(file)) {
            int bytesRead;
            int index = 0;
            while ((bytesRead = fis.read()) != -1) {
                data[index++] = (byte) bytesRead;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return data;
    }

    public void write(String filename, byte[] data) throws IOException {
        ensureBaseDir();
        Path path = baseDir.resolve(filename);
        Files.write(path, data, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    }

    public boolean delete(String filename) {
        ensureBaseDir();
        return baseDir.resolve(filename).toFile().delete();
    }

    public List<String> list() {
        ensureBaseDir();
        File[] files = baseDir.toFile().listFiles();
        List<String> names = new ArrayList<>();
        if (files != null) {
            for (File file : files) {
                names.add(file.getName());
            }
        }
        return names;
    }

    private void ensureBaseDir() {
        File dir = baseDir.toFile();
        if (!dir.exists()) {
            dir.mkdirs();
        }
    }
}

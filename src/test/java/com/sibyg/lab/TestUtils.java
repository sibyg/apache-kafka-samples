package com.sibyg.lab;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

@Slf4j
public class TestUtils {
    public static File tempDirectory() {
        return tempDirectory((String) null);

    }

    public static File tempDirectory(String prefix) {
        return tempDirectory((Path) null, prefix);
    }

    public static File tempDirectory(Path parent, String prefix) {
        prefix = prefix == null ? "kafka-" : prefix;

        File file;
        try {
            file = parent == null ? Files.createTempDirectory(prefix).toFile() : Files.createTempDirectory(parent, prefix).toFile();
        } catch (IOException var4) {
            throw new RuntimeException("Failed to create a temp dir", var4);
        }

        file.deleteOnExit();
        Exit.addShutdownHook("delete-temp-file-shutdown-hook", () -> {
            try {
                Utils.delete(file);
            } catch (IOException var2) {
                log.error("Error deleting {}", file.getAbsolutePath(), var2);
            }

        });
        return file;
    }
}

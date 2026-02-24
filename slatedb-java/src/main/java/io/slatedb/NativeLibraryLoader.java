package io.slatedb;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/// Loads the SlateDB native library from classpath resources.
///
/// The Gradle build stages platform-specific files under `native/<os>-<arch>/`.
public final class NativeLibraryLoader {
    private static final Object LOAD_LOCK = new Object();
    private static volatile boolean loaded;

    private static final Map<String, String> RESOURCE_BY_PLATFORM = Map.of(
        "linux-x86_64", "native/linux-x86_64/libslatedb_c.so",
        "linux-aarch64", "native/linux-aarch64/libslatedb_c.so",
        "macos-x86_64", "native/macos-x86_64/libslatedb_c.dylib",
        "macos-aarch64", "native/macos-aarch64/libslatedb_c.dylib",
        "windows-x86_64", "native/windows-x86_64/slatedb_c.dll",
        "windows-aarch64", "native/windows-aarch64/slatedb_c.dll"
    );

    private NativeLibraryLoader() {
    }

    /// Loads the current platform's native SlateDB library from the classpath once.
    ///
    /// This is called from generated jextract code. During the build,
    /// `patchJextractNativeLoader` rewrites the generated file in `Native.java` so
    /// its static initializer calls this method instead of
    /// `System.loadLibrary("slatedb_c")`.
    public static void loadFromClasspath() {
        // Fast path check to avoid synchronization after the library is loaded.
        if (loaded) {
            return;
        }

        synchronized (LOAD_LOCK) {
            if (loaded) {
                return;
            }

            String platformId = currentPlatformId();
            String resourcePath = RESOURCE_BY_PLATFORM.get(platformId);
            if (resourcePath == null) {
                throw linkageError(
                    "Unsupported platform '" + platformId + "'. Supported platforms: " +
                        String.join(", ", sortedSupportedPlatforms()),
                    null
                );
            }

            try {
                Path extractedLibrary = extractToTemporaryFile(resourcePath);
                System.load(extractedLibrary.toAbsolutePath().toString());
                loaded = true;
            } catch (IOException e) {
                throw linkageError(
                    "Failed to load SlateDB native library for platform '" + platformId +
                        "' from classpath resource '" + resourcePath + "'.",
                    e
                );
            }
        }
    }

    private static Path extractToTemporaryFile(String resourcePath) throws IOException {
        String fileName = resourcePath.substring(resourcePath.lastIndexOf('/') + 1);
        Path tempDir = Files.createTempDirectory("slatedb-native-");
        tempDir.toFile().deleteOnExit();

        Path extractedFile = tempDir.resolve(fileName);
        try (InputStream input = NativeLibraryLoader.class.getClassLoader().getResourceAsStream(resourcePath)) {
            if (input == null) {
                throw new IOException("Missing classpath resource: " + resourcePath);
            }
            Files.copy(input, extractedFile, StandardCopyOption.REPLACE_EXISTING);
        }

        extractedFile.toFile().deleteOnExit();
        return extractedFile;
    }

    private static Set<String> sortedSupportedPlatforms() {
        return new TreeSet<>(RESOURCE_BY_PLATFORM.keySet());
    }

    private static String currentPlatformId() {
        return normalizeOs(System.getProperty("os.name", "")) + "-" +
            normalizeArch(System.getProperty("os.arch", ""));
    }

    private static String normalizeOs(String osName) {
        String normalized = osName.toLowerCase(Locale.ROOT);
        if (normalized.contains("win")) {
            return "windows";
        }
        if (normalized.contains("mac") || normalized.contains("darwin")) {
            return "macos";
        }
        if (normalized.contains("linux")) {
            return "linux";
        }
        return normalized;
    }

    private static String normalizeArch(String archName) {
        String normalized = archName.toLowerCase(Locale.ROOT);
        if (normalized.equals("amd64") || normalized.equals("x86_64") || normalized.equals("x64")) {
            return "x86_64";
        }
        if (normalized.equals("aarch64") || normalized.equals("arm64")) {
            return "aarch64";
        }
        return normalized;
    }

    private static UnsatisfiedLinkError linkageError(String message, Throwable cause) {
        UnsatisfiedLinkError error = new UnsatisfiedLinkError(message);
        if (cause != null) {
            error.initCause(cause);
        }
        return error;
    }
}

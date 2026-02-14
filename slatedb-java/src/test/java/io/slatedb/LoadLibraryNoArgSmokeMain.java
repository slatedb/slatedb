package io.slatedb;

/// Smoke-test entrypoint used by SlateDbTest to verify no-arg native loading via java.library.path.
public final class LoadLibraryNoArgSmokeMain {
    private LoadLibraryNoArgSmokeMain() {
    }

    public static void main(String[] args) {
        if (args.length != 0) {
            throw new IllegalArgumentException("LoadLibraryNoArgSmokeMain accepts no arguments");
        }

        SlateDb.loadLibrary();
        SlateDb.initLogging(SlateDbConfig.LogLevel.OFF);

        String defaults = SlateDb.settingsDefault();
        if (defaults == null || defaults.isBlank()) {
            throw new IllegalStateException("settingsDefault returned blank output");
        }
    }
}

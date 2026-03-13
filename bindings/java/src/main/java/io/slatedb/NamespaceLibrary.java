package io.slatedb;


import com.sun.jna.Library;
import com.sun.jna.Native;

final class NamespaceLibrary {
  static synchronized String findLibraryName(String componentName) {
    String libOverride = System.getProperty("uniffi.component." + componentName + ".libraryOverride");
    if (libOverride != null) {
        return libOverride;
    }
    return "slatedb_ffi";
  }

  static <Lib extends Library> Lib loadIndirect(String componentName, Class<Lib> clazz) {
    return Native.load(findLibraryName(componentName), clazz);
  }

  static void uniffiCheckContractApiVersion(UniffiLib lib) {
    // Get the bindings contract version from our ComponentInterface
    int bindingsContractVersion = 29;
    // Get the scaffolding contract version by calling the into the dylib
    int scaffoldingContractVersion = lib.ffi_slatedb_ffi_uniffi_contract_version();
    if (bindingsContractVersion != scaffoldingContractVersion) {
        throw new RuntimeException("UniFFI contract version mismatch: try cleaning and rebuilding your project");
    }
  }

  static void uniffiCheckApiChecksums(UniffiLib lib) {
    if (lib.uniffi_slatedb_ffi_checksum_func_default_settings_json() != ((short) 41457)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_func_resolve_object_store() != ((short) 23127)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_db_begin() != ((short) 5274)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_db_delete() != ((short) 53628)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_db_delete_with_options() != ((short) 46529)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_db_flush() != ((short) 56183)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_db_flush_with_options() != ((short) 16447)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_db_get() != ((short) 16615)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_db_get_key_value() != ((short) 30007)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_db_get_key_value_with_options() != ((short) 20887)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_db_get_with_options() != ((short) 34184)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_db_merge() != ((short) 49680)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_db_merge_with_options() != ((short) 29273)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_db_put() != ((short) 49345)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_db_put_with_options() != ((short) 19637)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_db_scan() != ((short) 20253)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_db_scan_prefix() != ((short) 50816)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_db_scan_prefix_with_options() != ((short) 54339)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_db_scan_with_options() != ((short) 26695)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_db_shutdown() != ((short) 37181)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_db_snapshot() != ((short) 7345)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_db_status() != ((short) 60950)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_db_write() != ((short) 52274)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_db_write_with_options() != ((short) 52159)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbbuilder_build() != ((short) 35713)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbbuilder_with_db_cache_disabled() != ((short) 30405)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbbuilder_with_merge_operator() != ((short) 26111)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbbuilder_with_seed() != ((short) 9556)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbbuilder_with_settings_json() != ((short) 26925)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbbuilder_with_sst_block_size() != ((short) 16153)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbbuilder_with_wal_object_store() != ((short) 61826)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbiterator_next() != ((short) 30810)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbiterator_seek() != ((short) 53625)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbsnapshot_get() != ((short) 28442)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbsnapshot_get_key_value() != ((short) 21199)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbsnapshot_get_key_value_with_options() != ((short) 7853)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbsnapshot_get_with_options() != ((short) 57868)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbsnapshot_scan() != ((short) 14532)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbsnapshot_scan_prefix() != ((short) 39527)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbsnapshot_scan_prefix_with_options() != ((short) 26923)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbsnapshot_scan_with_options() != ((short) 14264)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbtransaction_commit() != ((short) 38520)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbtransaction_commit_with_options() != ((short) 30206)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbtransaction_delete() != ((short) 31021)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbtransaction_get() != ((short) 5920)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbtransaction_get_key_value() != ((short) 26704)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbtransaction_get_key_value_with_options() != ((short) 58235)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbtransaction_get_with_options() != ((short) 37338)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbtransaction_id() != ((short) 29935)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbtransaction_mark_read() != ((short) 23131)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbtransaction_merge() != ((short) 47972)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbtransaction_merge_with_options() != ((short) 27318)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbtransaction_put() != ((short) 17459)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbtransaction_put_with_options() != ((short) 36745)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbtransaction_rollback() != ((short) 8231)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbtransaction_scan() != ((short) 63657)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbtransaction_scan_prefix() != ((short) 9925)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbtransaction_scan_prefix_with_options() != ((short) 4020)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbtransaction_scan_with_options() != ((short) 8054)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbtransaction_seqnum() != ((short) 15980)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbtransaction_unmark_write() != ((short) 46328)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_constructor_dbbuilder_new() != ((short) 30406)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_mergeoperator_merge() != ((short) 14285)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
  }
}

// Define FFI callback types

package io.slatedb.uniffi;


import com.sun.jna.Library;
import com.sun.jna.Native;

final class NamespaceLibrary {
  static synchronized String findLibraryName(String componentName) {
    String libOverride = System.getProperty("uniffi.component." + componentName + ".libraryOverride");
    if (libOverride != null) {
        return libOverride;
    }
    return "slatedb_uniffi";
  }

  static <Lib extends Library> Lib loadIndirect(String componentName, Class<Lib> clazz) {
    return Native.load(findLibraryName(componentName), clazz);
  }

  static void uniffiCheckContractApiVersion(UniffiLib lib) {
    // Get the bindings contract version from our ComponentInterface
    int bindingsContractVersion = 29;
    // Get the scaffolding contract version by calling the into the dylib
    int scaffoldingContractVersion = lib.ffi_slatedb_uniffi_uniffi_contract_version();
    if (bindingsContractVersion != scaffoldingContractVersion) {
        throw new RuntimeException("UniFFI contract version mismatch: try cleaning and rebuilding your project");
    }
  }

  static void uniffiCheckApiChecksums(UniffiLib lib) {
    if (lib.uniffi_slatedb_uniffi_checksum_func_init_logging() != ((short) 20973)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_db_begin() != ((short) 51275)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_db_delete() != ((short) 34129)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_db_delete_with_options() != ((short) 42509)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_db_flush() != ((short) 18130)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_db_flush_with_options() != ((short) 63293)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_db_get() != ((short) 50068)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_db_get_key_value() != ((short) 57684)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_db_get_key_value_with_options() != ((short) 20648)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_db_get_with_options() != ((short) 20501)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_db_merge() != ((short) 17999)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_db_merge_with_options() != ((short) 61231)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_db_metrics() != ((short) 63278)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_db_put() != ((short) 59996)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_db_put_with_options() != ((short) 58268)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_db_scan() != ((short) 38146)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_db_scan_prefix() != ((short) 16589)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_db_scan_prefix_with_options() != ((short) 37166)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_db_scan_with_options() != ((short) 57778)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_db_shutdown() != ((short) 43377)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_db_snapshot() != ((short) 13313)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_db_status() != ((short) 55824)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_db_write() != ((short) 13969)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_db_write_with_options() != ((short) 34167)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbbuilder_build() != ((short) 57780)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbbuilder_with_db_cache_disabled() != ((short) 47291)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbbuilder_with_merge_operator() != ((short) 26367)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbbuilder_with_seed() != ((short) 4525)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbbuilder_with_settings() != ((short) 60845)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbbuilder_with_sst_block_size() != ((short) 9450)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbbuilder_with_wal_object_store() != ((short) 59224)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbiterator_next() != ((short) 49160)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbiterator_seek() != ((short) 43547)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbreader_get() != ((short) 22886)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbreader_get_with_options() != ((short) 9133)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbreader_scan() != ((short) 19575)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbreader_scan_prefix() != ((short) 51732)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbreader_scan_prefix_with_options() != ((short) 24990)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbreader_scan_with_options() != ((short) 33406)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbreader_shutdown() != ((short) 33391)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbreaderbuilder_build() != ((short) 3383)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbreaderbuilder_with_checkpoint_id() != ((short) 20357)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbreaderbuilder_with_merge_operator() != ((short) 54971)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbreaderbuilder_with_options() != ((short) 5765)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbreaderbuilder_with_wal_object_store() != ((short) 15471)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbsnapshot_get() != ((short) 37663)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbsnapshot_get_key_value() != ((short) 1007)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbsnapshot_get_key_value_with_options() != ((short) 20762)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbsnapshot_get_with_options() != ((short) 29177)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbsnapshot_scan() != ((short) 18781)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbsnapshot_scan_prefix() != ((short) 43063)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbsnapshot_scan_prefix_with_options() != ((short) 39827)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbsnapshot_scan_with_options() != ((short) 1457)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbtransaction_commit() != ((short) 17358)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbtransaction_commit_with_options() != ((short) 53495)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbtransaction_delete() != ((short) 9717)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbtransaction_get() != ((short) 27661)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbtransaction_get_key_value() != ((short) 62855)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbtransaction_get_key_value_with_options() != ((short) 37939)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbtransaction_get_with_options() != ((short) 53534)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbtransaction_id() != ((short) 16876)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbtransaction_mark_read() != ((short) 26788)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbtransaction_merge() != ((short) 28294)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbtransaction_merge_with_options() != ((short) 63505)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbtransaction_put() != ((short) 30341)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbtransaction_put_with_options() != ((short) 24593)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbtransaction_rollback() != ((short) 23348)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbtransaction_scan() != ((short) 12571)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbtransaction_scan_prefix() != ((short) 49961)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbtransaction_scan_prefix_with_options() != ((short) 33081)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbtransaction_scan_with_options() != ((short) 55349)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbtransaction_seqnum() != ((short) 60506)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbtransaction_unmark_write() != ((short) 15301)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_logcallback_log() != ((short) 11398)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_mergeoperator_merge() != ((short) 9511)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_settings_set() != ((short) 31996)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_settings_to_json_string() != ((short) 62526)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_walfile_id() != ((short) 51355)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_walfile_iterator() != ((short) 50239)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_walfile_metadata() != ((short) 30832)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_walfile_next_file() != ((short) 52353)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_walfile_next_id() != ((short) 60587)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_walfileiterator_next() != ((short) 18233)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_walreader_get() != ((short) 40699)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_walreader_list() != ((short) 62366)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_writebatch_delete() != ((short) 37032)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_writebatch_merge() != ((short) 51939)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_writebatch_merge_with_options() != ((short) 30105)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_writebatch_put() != ((short) 35694)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_writebatch_put_with_options() != ((short) 23639)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_constructor_dbbuilder_new() != ((short) 20774)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_constructor_dbreaderbuilder_new() != ((short) 63705)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_constructor_objectstore_from_env() != ((short) 31525)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_constructor_objectstore_resolve() != ((short) 27737)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_constructor_settings_default() != ((short) 56170)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_constructor_settings_from_env() != ((short) 49511)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_constructor_settings_from_env_with_default() != ((short) 6106)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_constructor_settings_from_file() != ((short) 40167)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_constructor_settings_from_json_string() != ((short) 43399)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_constructor_settings_load() != ((short) 3704)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_constructor_walreader_new() != ((short) 791)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_constructor_writebatch_new() != ((short) 25201)) {
        throw new RuntimeException("UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
  }
}

// Define FFI callback types

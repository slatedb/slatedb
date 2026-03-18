package io.slatedb.jna.ffi;

import com.sun.jna.Library;
import com.sun.jna.Native;

final class NamespaceLibrary {
  static synchronized String findLibraryName(String componentName) {
    String libOverride =
        System.getProperty("uniffi.component." + componentName + ".libraryOverride");
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
      throw new RuntimeException(
          "UniFFI contract version mismatch: try cleaning and rebuilding your project");
    }
  }

  static void uniffiCheckApiChecksums(UniffiLib lib) {
    if (lib.uniffi_slatedb_uniffi_checksum_func_init_logging() != ((short) 37619)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_db_begin() != ((short) 14329)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_db_delete() != ((short) 50713)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_db_delete_with_options() != ((short) 58476)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_db_flush() != ((short) 31907)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_db_flush_with_options() != ((short) 25305)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_db_get() != ((short) 12477)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_db_get_key_value() != ((short) 24743)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_db_get_key_value_with_options()
        != ((short) 38272)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_db_get_with_options() != ((short) 42757)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_db_merge() != ((short) 31600)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_db_merge_with_options() != ((short) 59982)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_db_metrics() != ((short) 59929)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_db_put() != ((short) 53709)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_db_put_with_options() != ((short) 22274)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_db_scan() != ((short) 42782)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_db_scan_prefix() != ((short) 47427)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_db_scan_prefix_with_options() != ((short) 555)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_db_scan_with_options() != ((short) 44084)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_db_shutdown() != ((short) 16768)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_db_snapshot() != ((short) 23166)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_db_status() != ((short) 3648)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_db_write() != ((short) 32974)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_db_write_with_options() != ((short) 35773)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbbuilder_build() != ((short) 49797)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbbuilder_with_db_cache_disabled()
        != ((short) 23550)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbbuilder_with_merge_operator()
        != ((short) 53509)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbbuilder_with_seed() != ((short) 24155)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbbuilder_with_settings() != ((short) 7581)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbbuilder_with_sst_block_size()
        != ((short) 55141)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbbuilder_with_wal_object_store()
        != ((short) 13745)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbiterator_next() != ((short) 48403)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbiterator_seek() != ((short) 49966)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbreader_get() != ((short) 8025)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbreader_get_with_options() != ((short) 58698)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbreader_scan() != ((short) 18035)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbreader_scan_prefix() != ((short) 197)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbreader_scan_prefix_with_options()
        != ((short) 29037)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbreader_scan_with_options() != ((short) 48210)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbreader_shutdown() != ((short) 55236)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbreaderbuilder_build() != ((short) 44664)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbreaderbuilder_with_checkpoint_id()
        != ((short) 38586)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbreaderbuilder_with_merge_operator()
        != ((short) 14178)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbreaderbuilder_with_options()
        != ((short) 12316)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbreaderbuilder_with_wal_object_store()
        != ((short) 63885)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbsnapshot_get() != ((short) 37884)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbsnapshot_get_key_value() != ((short) 33717)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbsnapshot_get_key_value_with_options()
        != ((short) 36709)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbsnapshot_get_with_options()
        != ((short) 37581)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbsnapshot_scan() != ((short) 36474)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbsnapshot_scan_prefix() != ((short) 53560)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbsnapshot_scan_prefix_with_options()
        != ((short) 62871)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbsnapshot_scan_with_options()
        != ((short) 62995)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbtransaction_commit() != ((short) 43702)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbtransaction_commit_with_options()
        != ((short) 13115)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbtransaction_delete() != ((short) 54600)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbtransaction_get() != ((short) 61976)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbtransaction_get_key_value()
        != ((short) 11817)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbtransaction_get_key_value_with_options()
        != ((short) 37760)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbtransaction_get_with_options()
        != ((short) 23745)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbtransaction_id() != ((short) 19938)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbtransaction_mark_read() != ((short) 57562)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbtransaction_merge() != ((short) 34881)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbtransaction_merge_with_options()
        != ((short) 37707)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbtransaction_put() != ((short) 21505)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbtransaction_put_with_options()
        != ((short) 56410)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbtransaction_rollback() != ((short) 11015)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbtransaction_scan() != ((short) 9340)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbtransaction_scan_prefix() != ((short) 215)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbtransaction_scan_prefix_with_options()
        != ((short) 17517)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbtransaction_scan_with_options()
        != ((short) 56187)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbtransaction_seqnum() != ((short) 62915)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_dbtransaction_unmark_write() != ((short) 16334)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_logcallback_log() != ((short) 34306)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_mergeoperator_merge() != ((short) 4237)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_settings_set() != ((short) 8000)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_settings_to_json_string() != ((short) 25337)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_walfile_id() != ((short) 6532)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_walfile_iterator() != ((short) 23680)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_walfile_metadata() != ((short) 52889)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_walfile_next_file() != ((short) 7739)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_walfile_next_id() != ((short) 40501)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_walfile_shutdown() != ((short) 36982)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_walfileiterator_next() != ((short) 41163)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_walfileiterator_shutdown() != ((short) 60794)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_walreader_get() != ((short) 34963)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_walreader_list() != ((short) 42612)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_walreader_shutdown() != ((short) 34538)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_writebatch_delete() != ((short) 22112)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_writebatch_merge() != ((short) 15590)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_writebatch_merge_with_options()
        != ((short) 26306)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_writebatch_put() != ((short) 23075)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_method_writebatch_put_with_options() != ((short) 7719)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_constructor_dbbuilder_new() != ((short) 57363)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_constructor_dbreaderbuilder_new() != ((short) 14951)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_constructor_objectstore_from_env() != ((short) 51626)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_constructor_objectstore_resolve() != ((short) 20659)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_constructor_settings_default() != ((short) 31643)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_constructor_settings_from_env() != ((short) 31867)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_constructor_settings_from_env_with_default()
        != ((short) 22902)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_constructor_settings_from_file() != ((short) 18430)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_constructor_settings_from_json_string()
        != ((short) 13174)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_constructor_settings_load() != ((short) 23883)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_constructor_walreader_new() != ((short) 28632)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_uniffi_checksum_constructor_writebatch_new() != ((short) 11646)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
  }
}

// Define FFI callback types

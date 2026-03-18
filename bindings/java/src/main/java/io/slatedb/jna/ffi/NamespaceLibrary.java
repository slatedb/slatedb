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
      throw new RuntimeException(
          "UniFFI contract version mismatch: try cleaning and rebuilding your project");
    }
  }

  static void uniffiCheckApiChecksums(UniffiLib lib) {
    if (lib.uniffi_slatedb_ffi_checksum_func_init_logging() != ((short) 54254)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_db_begin() != ((short) 28200)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_db_delete() != ((short) 50032)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_db_delete_with_options() != ((short) 32367)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_db_flush() != ((short) 39017)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_db_flush_with_options() != ((short) 21103)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_db_get() != ((short) 47753)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_db_get_key_value() != ((short) 40515)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_db_get_key_value_with_options() != ((short) 49009)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_db_get_with_options() != ((short) 59259)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_db_merge() != ((short) 31328)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_db_merge_with_options() != ((short) 18622)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_db_metrics() != ((short) 10479)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_db_put() != ((short) 37265)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_db_put_with_options() != ((short) 23832)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_db_scan() != ((short) 16785)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_db_scan_prefix() != ((short) 52231)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_db_scan_prefix_with_options() != ((short) 32993)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_db_scan_with_options() != ((short) 38619)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_db_shutdown() != ((short) 1011)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_db_snapshot() != ((short) 420)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_db_status() != ((short) 21747)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_db_write() != ((short) 55178)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_db_write_with_options() != ((short) 23668)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbbuilder_build() != ((short) 54479)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbbuilder_with_db_cache_disabled()
        != ((short) 41151)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbbuilder_with_merge_operator() != ((short) 11670)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbbuilder_with_seed() != ((short) 18556)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbbuilder_with_settings() != ((short) 11407)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbbuilder_with_sst_block_size() != ((short) 24601)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbbuilder_with_wal_object_store()
        != ((short) 15581)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbiterator_next() != ((short) 63746)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbiterator_seek() != ((short) 3174)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbreader_get() != ((short) 150)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbreader_get_with_options() != ((short) 53767)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbreader_scan() != ((short) 48493)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbreader_scan_prefix() != ((short) 42624)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbreader_scan_prefix_with_options()
        != ((short) 44039)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbreader_scan_with_options() != ((short) 653)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbreader_shutdown() != ((short) 61444)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbreaderbuilder_build() != ((short) 53002)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbreaderbuilder_with_checkpoint_id()
        != ((short) 60214)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbreaderbuilder_with_merge_operator()
        != ((short) 15355)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbreaderbuilder_with_options() != ((short) 58185)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbreaderbuilder_with_wal_object_store()
        != ((short) 39998)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbsnapshot_get() != ((short) 25831)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbsnapshot_get_key_value() != ((short) 34552)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbsnapshot_get_key_value_with_options()
        != ((short) 33632)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbsnapshot_get_with_options() != ((short) 31030)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbsnapshot_scan() != ((short) 473)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbsnapshot_scan_prefix() != ((short) 50321)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbsnapshot_scan_prefix_with_options()
        != ((short) 5354)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbsnapshot_scan_with_options() != ((short) 47474)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbtransaction_commit() != ((short) 428)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbtransaction_commit_with_options()
        != ((short) 62426)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbtransaction_delete() != ((short) 36751)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbtransaction_get() != ((short) 17479)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbtransaction_get_key_value() != ((short) 42847)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbtransaction_get_key_value_with_options()
        != ((short) 12647)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbtransaction_get_with_options()
        != ((short) 51567)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbtransaction_id() != ((short) 6904)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbtransaction_mark_read() != ((short) 55492)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbtransaction_merge() != ((short) 14786)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbtransaction_merge_with_options()
        != ((short) 63514)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbtransaction_put() != ((short) 18111)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbtransaction_put_with_options()
        != ((short) 41295)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbtransaction_rollback() != ((short) 45610)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbtransaction_scan() != ((short) 37690)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbtransaction_scan_prefix() != ((short) 8910)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbtransaction_scan_prefix_with_options()
        != ((short) 747)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbtransaction_scan_with_options()
        != ((short) 48535)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbtransaction_seqnum() != ((short) 32854)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_dbtransaction_unmark_write() != ((short) 28889)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_logcallback_log() != ((short) 29875)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_mergeoperator_merge() != ((short) 9527)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_settings_set() != ((short) 35534)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_settings_to_json_string() != ((short) 56273)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_walfile_id() != ((short) 46596)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_walfile_iterator() != ((short) 44079)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_walfile_metadata() != ((short) 11831)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_walfile_next_file() != ((short) 18713)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_walfile_next_id() != ((short) 11112)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_walfile_shutdown() != ((short) 61794)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_walfileiterator_next() != ((short) 44449)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_walfileiterator_shutdown() != ((short) 8034)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_walreader_get() != ((short) 2234)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_walreader_list() != ((short) 34786)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_walreader_shutdown() != ((short) 16293)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_writebatch_delete() != ((short) 33429)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_writebatch_merge() != ((short) 23805)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_writebatch_merge_with_options() != ((short) 5442)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_writebatch_put() != ((short) 46996)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_writebatch_put_with_options() != ((short) 54841)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_constructor_dbbuilder_new() != ((short) 64175)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_constructor_dbreaderbuilder_new() != ((short) 43902)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_constructor_objectstore_from_env() != ((short) 16566)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_constructor_objectstore_resolve() != ((short) 25816)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_constructor_settings_default() != ((short) 47128)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_constructor_settings_from_env() != ((short) 51006)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_constructor_settings_from_env_with_default()
        != ((short) 19363)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_constructor_settings_from_file() != ((short) 34482)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_constructor_settings_from_json_string() != ((short) 4001)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_constructor_settings_load() != ((short) 6890)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_constructor_walreader_new() != ((short) 8955)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_constructor_writebatch_new() != ((short) 37826)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
  }
}

// Define FFI callback types

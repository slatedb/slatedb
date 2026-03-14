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
    if (lib.uniffi_slatedb_ffi_checksum_func_ffi_init_logging() != ((short) 42559)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffidb_begin() != ((short) 27320)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffidb_delete() != ((short) 23170)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffidb_delete_with_options() != ((short) 36049)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffidb_flush() != ((short) 8494)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffidb_flush_with_options() != ((short) 49534)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffidb_get() != ((short) 41469)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffidb_get_key_value() != ((short) 4319)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffidb_get_key_value_with_options()
        != ((short) 30550)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffidb_get_with_options() != ((short) 24693)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffidb_merge() != ((short) 17131)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffidb_merge_with_options() != ((short) 20187)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffidb_metrics() != ((short) 17180)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffidb_put() != ((short) 35423)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffidb_put_with_options() != ((short) 3469)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffidb_scan() != ((short) 13606)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffidb_scan_prefix() != ((short) 35855)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffidb_scan_prefix_with_options()
        != ((short) 28845)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffidb_scan_with_options() != ((short) 16422)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffidb_shutdown() != ((short) 62539)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffidb_snapshot() != ((short) 61667)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffidb_status() != ((short) 6095)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffidb_write() != ((short) 50936)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffidb_write_with_options() != ((short) 566)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffidbbuilder_build() != ((short) 20111)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffidbbuilder_with_db_cache_disabled()
        != ((short) 29934)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffidbbuilder_with_merge_operator()
        != ((short) 13534)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffidbbuilder_with_seed() != ((short) 50434)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffidbbuilder_with_settings() != ((short) 21510)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffidbbuilder_with_sst_block_size()
        != ((short) 43234)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffidbbuilder_with_wal_object_store()
        != ((short) 54976)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffidbiterator_next() != ((short) 35396)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffidbiterator_seek() != ((short) 16218)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffidbreader_get() != ((short) 46156)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffidbreader_get_with_options() != ((short) 13809)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffidbreader_scan() != ((short) 587)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffidbreader_scan_prefix() != ((short) 2092)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffidbreader_scan_prefix_with_options()
        != ((short) 2695)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffidbreader_scan_with_options() != ((short) 19389)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffidbreader_shutdown() != ((short) 60272)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffidbreaderbuilder_build() != ((short) 10134)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffidbreaderbuilder_with_checkpoint_id()
        != ((short) 43622)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffidbreaderbuilder_with_merge_operator()
        != ((short) 48295)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffidbreaderbuilder_with_options()
        != ((short) 1731)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffidbreaderbuilder_with_wal_object_store()
        != ((short) 35691)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffidbsnapshot_get() != ((short) 20211)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffidbsnapshot_get_key_value() != ((short) 59292)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffidbsnapshot_get_key_value_with_options()
        != ((short) 4124)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffidbsnapshot_get_with_options()
        != ((short) 20033)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffidbsnapshot_scan() != ((short) 5732)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffidbsnapshot_scan_prefix() != ((short) 43819)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffidbsnapshot_scan_prefix_with_options()
        != ((short) 41123)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffidbsnapshot_scan_with_options()
        != ((short) 10211)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffidbtransaction_commit() != ((short) 48317)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffidbtransaction_commit_with_options()
        != ((short) 23549)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffidbtransaction_delete() != ((short) 16039)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffidbtransaction_get() != ((short) 22094)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffidbtransaction_get_key_value()
        != ((short) 27481)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffidbtransaction_get_key_value_with_options()
        != ((short) 41640)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffidbtransaction_get_with_options()
        != ((short) 43185)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffidbtransaction_id() != ((short) 54287)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffidbtransaction_mark_read() != ((short) 22753)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffidbtransaction_merge() != ((short) 35715)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffidbtransaction_merge_with_options()
        != ((short) 25941)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffidbtransaction_put() != ((short) 44076)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffidbtransaction_put_with_options()
        != ((short) 21098)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffidbtransaction_rollback() != ((short) 9781)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffidbtransaction_scan() != ((short) 36573)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffidbtransaction_scan_prefix() != ((short) 49503)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffidbtransaction_scan_prefix_with_options()
        != ((short) 29076)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffidbtransaction_scan_with_options()
        != ((short) 34855)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffidbtransaction_seqnum() != ((short) 22659)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffidbtransaction_unmark_write() != ((short) 13895)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffilogcallback_log() != ((short) 63108)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffimergeoperator_merge() != ((short) 64881)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffisettings_set() != ((short) 25271)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffisettings_to_json_string() != ((short) 6471)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffiwalfile_id() != ((short) 4476)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffiwalfile_iterator() != ((short) 26463)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffiwalfile_metadata() != ((short) 53251)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffiwalfile_next_file() != ((short) 583)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffiwalfile_next_id() != ((short) 17961)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffiwalfile_shutdown() != ((short) 1197)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffiwalfileiterator_next() != ((short) 25733)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffiwalfileiterator_shutdown() != ((short) 10991)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffiwalreader_get() != ((short) 42864)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffiwalreader_list() != ((short) 64696)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffiwalreader_shutdown() != ((short) 48499)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffiwritebatch_delete() != ((short) 48646)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffiwritebatch_merge() != ((short) 16539)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffiwritebatch_merge_with_options()
        != ((short) 14093)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffiwritebatch_put() != ((short) 9515)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_method_ffiwritebatch_put_with_options()
        != ((short) 35097)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_constructor_ffidbbuilder_new() != ((short) 22766)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_constructor_ffidbreaderbuilder_new() != ((short) 56685)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_constructor_ffiobjectstore_from_env() != ((short) 29333)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_constructor_ffiobjectstore_resolve() != ((short) 52192)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_constructor_ffisettings_default() != ((short) 37692)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_constructor_ffisettings_from_env() != ((short) 42524)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_constructor_ffisettings_from_env_with_default()
        != ((short) 57925)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_constructor_ffisettings_from_file() != ((short) 47421)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_constructor_ffisettings_from_json_string()
        != ((short) 32936)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_constructor_ffisettings_load() != ((short) 61074)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_constructor_ffiwalreader_new() != ((short) 2695)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
    if (lib.uniffi_slatedb_ffi_checksum_constructor_ffiwritebatch_new() != ((short) 46470)) {
      throw new RuntimeException(
          "UniFFI API checksum mismatch: try cleaning and rebuilding your project");
    }
  }
}

// Define FFI callback types

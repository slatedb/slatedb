package io.slatedb;


import java.util.List;
import java.util.Map;

/**
 * SST block sizes that can be selected on [`crate::DbBuilder`].
 */

public enum SstBlockSize {
    /**
     * Use 1 KiB SST blocks.
     */
  BLOCK1_KIB,
    /**
     * Use 2 KiB SST blocks.
     */
  BLOCK2_KIB,
    /**
     * Use 4 KiB SST blocks.
     */
  BLOCK4_KIB,
    /**
     * Use 8 KiB SST blocks.
     */
  BLOCK8_KIB,
    /**
     * Use 16 KiB SST blocks.
     */
  BLOCK16_KIB,
    /**
     * Use 32 KiB SST blocks.
     */
  BLOCK32_KIB,
    /**
     * Use 64 KiB SST blocks.
     */
  BLOCK64_KIB;
}



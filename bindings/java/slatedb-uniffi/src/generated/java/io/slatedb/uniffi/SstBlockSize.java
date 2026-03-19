package io.slatedb.uniffi;


import java.util.List;
import java.util.Map;

/**
 * Block size used for newly written SSTable blocks.
 */

public enum SstBlockSize {
    /**
     * 1 KiB blocks.
     */
  BLOCK1_KIB,
    /**
     * 2 KiB blocks.
     */
  BLOCK2_KIB,
    /**
     * 4 KiB blocks.
     */
  BLOCK4_KIB,
    /**
     * 8 KiB blocks.
     */
  BLOCK8_KIB,
    /**
     * 16 KiB blocks.
     */
  BLOCK16_KIB,
    /**
     * 32 KiB blocks.
     */
  BLOCK32_KIB,
    /**
     * 64 KiB blocks.
     */
  BLOCK64_KIB;
}



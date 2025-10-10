package com.slatedb.config;

/**
 * Enumeration of SSTable block sizes supported by SlateDB.
 * 
 * Block size affects the granularity of reads and caching.
 * Larger blocks can improve sequential read performance but
 * may waste space for random access patterns.
 */
public enum SstBlockSize {
    /** 1 KiB blocks - minimal overhead, good for random access */
    SIZE_1_KIB(1),
    
    /** 2 KiB blocks - balance between overhead and efficiency */
    SIZE_2_KIB(2),
    
    /** 4 KiB blocks - default size, good general purpose choice */
    SIZE_4_KIB(4),
    
    /** 8 KiB blocks - good for sequential workloads */
    SIZE_8_KIB(8),
    
    /** 16 KiB blocks - better sequential performance */
    SIZE_16_KIB(16),
    
    /** 32 KiB blocks - high sequential performance */
    SIZE_32_KIB(32),
    
    /** 64 KiB blocks - maximum sequential performance */
    SIZE_64_KIB(64);
    
    private final int sizeKib;
    
    SstBlockSize(int sizeKib) {
        this.sizeKib = sizeKib;
    }
    
    /**
     * Gets the block size in KiB.
     * 
     * @return the block size in KiB
     */
    public int getSizeKib() {
        return sizeKib;
    }
    
    /**
     * Gets the block size in bytes.
     * 
     * @return the block size in bytes
     */
    public int getSizeBytes() {
        return sizeKib * 1024;
    }
    
    /**
     * Gets the integer value used for native interop.
     * 
     * @return the size value
     */
    public int getValue() {
        return sizeKib;
    }
    
    /**
     * Creates an SstBlockSize from its KiB value.
     * 
     * @param sizeKib the block size in KiB
     * @return the corresponding SstBlockSize enum
     * @throws IllegalArgumentException if the size is not supported
     */
    public static SstBlockSize fromKib(int sizeKib) {
        for (SstBlockSize size : values()) {
            if (size.sizeKib == sizeKib) {
                return size;
            }
        }
        throw new IllegalArgumentException("Unsupported SST block size: " + sizeKib + " KiB");
    }
    
    /**
     * Creates an SstBlockSize from its byte value.
     * 
     * @param sizeBytes the block size in bytes
     * @return the corresponding SstBlockSize enum
     * @throws IllegalArgumentException if the size is not supported
     */
    public static SstBlockSize fromBytes(int sizeBytes) {
        if (sizeBytes % 1024 != 0) {
            throw new IllegalArgumentException("Block size must be a multiple of 1024 bytes");
        }
        return fromKib(sizeBytes / 1024);
    }
    
    @Override
    public String toString() {
        return sizeKib + " KiB";
    }
}
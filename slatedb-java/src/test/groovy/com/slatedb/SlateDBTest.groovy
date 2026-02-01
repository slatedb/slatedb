package com.slatedb

import java.time.Duration

import com.slatedb.config.*
import com.slatedb.exceptions.*
import spock.lang.Specification
import spock.lang.TempDir
import java.nio.file.Path

/**
 * Unit tests for SlateDB using Spock framework.
 * 
 * These tests verify the core functionality of the SlateDB Java client
 * including CRUD operations, batch operations, configuration, and error handling.
 * Uses local backend for fast execution without requiring AWS credentials.
 */
class SlateDBTest extends Specification {
    
    @TempDir
    Path tempDir
    
    def "should create database with local storage"() {
        given:
        def storeConfig = StoreConfig.local()
        def dbPath = tempDir.resolve("test-db").toString()
        
        when:
        def db = SlateDB.open(dbPath, storeConfig, null)
        
        then:
        db != null
        !db.isClosed()
        
        cleanup:
        db?.close()
    }
    
    def "should perform basic put and get operations"() {
        given:
        def db = createTestDB()
        def key = "test-key".bytes
        def value = "test-value".bytes
        
        when:
        db.put(key, value)

        then:
        db.get(key) == value
        
        cleanup:
        db?.close()
    }
    
    def "should return null for non-existent key"() {
        given:
        def db = createTestDB()
        
        when:
        def result = db.get("non-existent".bytes)

        then:
        result == null

        cleanup:
        db?.close()
    }
    
    def "should handle delete operations"() {
        given:
        def db = createTestDB()
        def key = "delete-me".bytes
        def value = "some-value".bytes
        
        when:
        db.put(key, value)
        def beforeDelete = db.get(key)
        db.delete(key)
        def afterDelete = db.get(key)
        
        then:
        beforeDelete == value
        afterDelete == null
        
        cleanup:
        db?.close()
    }
    
    def "should handle empty values"() {
        given:
        def db = createTestDB()
        def key = "empty-value-key".bytes
        def emptyValue = new byte[0]
        
        when:
        db.put(key, emptyValue)
        def result = db.get(key)
        
        then:
        result == emptyValue
        
        cleanup:
        db?.close()
    }
    
    def "should reject null or empty keys"() {
        given:
        def db = createTestDB()
        
        when:
        db.put(invalidKey, "value".bytes)
        
        then:
        thrown(SlateDBInvalidArgumentException)
        
        cleanup:
        db?.close()
        
        where:
        invalidKey << [null, new byte[0]]
    }
    
    def "should handle write batch operations"() {
        given:
        def db = createTestDB()
        def batch = new WriteBatch()
        
        when:
        batch.put("key1".bytes, "value1".bytes)
        batch.put("key2".bytes, "value2".bytes)
        batch.delete("key3".bytes) // Non-existent key - should be safe
        db.write(batch)
        
        then:
        db.get("key1".bytes) == "value1".bytes
        db.get("key2".bytes) == "value2".bytes
        db.get("key3".bytes) == null
        
        cleanup:
        batch?.close()
        db?.close()
    }
    
    def "should prevent batch reuse after write"() {
        given:
        def db = createTestDB()
        def batch = new WriteBatch()
        batch.put("key1".bytes, "value1".bytes)
        db.write(batch)
        
        when:
        batch.put("key2".bytes, "value2".bytes)
        
        then:
        thrown(SlateDBException)
        
        cleanup:
        batch?.close()
        db?.close()
    }
    
    def "should handle write options"() {
        given:
        def db = createTestDB()
        def key = "write-options-key".bytes
        def value = "write-options-value".bytes
        def writeOptions = WriteOptions.builder().awaitDurable(false).build()
        
        when:
        db.put(key, value, null, writeOptions)
        def result = db.get(key)
        
        then:
        result == value
        
        cleanup:
        db?.close()
    }
    
    def "should handle put options with TTL"() {
        given:
        def db = createTestDB()
        def key = "ttl-key".bytes
        def value = "ttl-value".bytes
        def putOptions = PutOptions.expireAfter(Duration.ofHours(1))
        
        when:
        db.put(key, value, putOptions, null)

        then:
        db.get(key) == value
        
        cleanup:
        db?.close()
    }

    def "should handle put options with expireAfter eviction"() {
        given:
        def db = createTestDB()
        def key = "short-ttl-key".bytes
        def value = "short-ttl-value".bytes
        def putOptions = PutOptions.expireAfter(1000) // 1 second TTL for more predictable timing
        
        when: "putting value with TTL"
        db.put(key, value, putOptions, null)

        then: "value should be available immediately"
        db.get(key) == value
        
        when: "waiting for TTL to expire"
        sleep(1200) // Wait longer than TTL

        then: "value should be evicted after TTL expires"
        db.get(key) == null
        
        cleanup:
        db?.close()
    }
    
    def "should handle read options"() {
        given:
        def db = createTestDB()
        def key = "read-options-key".bytes
        def value = "read-options-value".bytes
        def readOptions = ReadOptions.builder().dirty(true).build()
        
        when:
        db.put(key, value)

        then:
        db.get(key, readOptions) == value
        
        cleanup:
        db?.close()
    }
    
    def "should create iterator for scanning"() {
        given:
        def db = createTestDB()
        
        // Put some test data
        db.put("prefix:key1".bytes, "value1".bytes)
        db.put("prefix:key2".bytes, "value2".bytes)
        db.put("prefix:key3".bytes, "value3".bytes)
        db.put("other:key1".bytes, "other-value".bytes)
        
        when:
        def iterator = db.scan("prefix:".bytes, "prefix;".bytes) // Scan prefix range
        
        then:
        iterator != null
        !iterator.isClosed()
        and:
        // Should find exactly the 3 keys with "prefix:" prefix in sorted order
        iterator.hasNext()
        new String(iterator.next().key) == "prefix:key1"
        and:
        iterator.hasNext()
        new String(iterator.next().key) == "prefix:key2"
        and:
        iterator.hasNext()
        new String(iterator.next().key) == "prefix:key3"
        and:
        !iterator.hasNext() // No more keys should be found
        
        cleanup:
        iterator?.close()
        db?.close()
    }
    
    def "should handle flush operations"() {
        given:
        def db = createTestDB()
        
        when:
        db.put("flush-test".bytes, "flush-value".bytes)
        and:
        db.flush()
        
        then:
        db.get("flush-test".bytes) == "flush-value".bytes
        
        cleanup:
        db?.close()
    }
    
    def "should handle database closure"() {
        given:
        def db = createTestDB()
        
        when:
        db.close()
        
        then:
        db.isClosed()
        
        when:
        db.get("any-key".bytes)
        
        then:
        thrown(SlateDBException)
    }
    
    def "should handle multiple close calls"() {
        given:
        def db = createTestDB()
        
        when:
        db.close()
        db.close() // Second close should be safe
        
        then:
        db.isClosed()
        noExceptionThrown()
    }
    
    def "should validate configuration objects"() {
        given:
        def awsConfig = AWSConfig.builder()
                .bucket("test-bucket")
                .region("us-east-1")
                .build()
        def storeConfig = StoreConfig.builder()
                .provider(Provider.AWS)
                .aws(awsConfig)
                .build()
        
        when:
        def result = storeConfig.getProvider()
        
        then:
        result == Provider.AWS
        storeConfig.getAws() == awsConfig
    }
    
    def "should create SlateDB options with builder pattern"() {
        given:
        def options = SlateDBOptions.builder()
                .l0SstSizeBytes(64L * 1024 * 1024) // 64MB
                .flushInterval(Duration.ofMillis(100))
                .build()
        
        expect:
        options.getL0SstSizeBytes() == 64L * 1024 * 1024
        options.getFlushInterval() == Duration.ofMillis(100)
    }
    
    def "should create AWS config with builder pattern"() {
        given:
        def awsConfig = AWSConfig.builder()
                .bucket("test-bucket")
                .region("us-east-1")
                .build()
        
        expect:
        awsConfig.getBucket() == "test-bucket"
        awsConfig.getRegion() == "us-east-1"
    }
    
    def "should create put options with builder pattern"() {
        given:
        def ttlDuration = Duration.ofHours(24)
        def putOptions = PutOptions.expireAfter(ttlDuration)
        
        expect:
        putOptions.getTtlType() == TTLType.EXPIRE_AFTER
        putOptions.getTtlValue() == ttlDuration.toMillis()
    }
    
    def "should create write options with builder pattern"() {
        given:
        def writeOptions = WriteOptions.builder()
                .awaitDurable(true)
                .build()
        
        expect:
        writeOptions.isAwaitDurable() == true
    }
    
    def "should create read options with builder pattern"() {
        given:
        def readOptions = ReadOptions.builder()
                .durabilityFilter(DurabilityLevel.MEMORY)
                .dirty(false)
                .build()
        
        expect:
        readOptions.getDurabilityFilter() == DurabilityLevel.MEMORY
        readOptions.isDirty() == false
    }
    
    def "should create scan options with builder pattern"() {
        given:
        def scanOptions = ScanOptions.builder()
                .durabilityFilter(DurabilityLevel.MEMORY)
                .dirty(false)
                .readAheadBytes(4 * 1024 * 1024)
                .cacheBlocks(true)
                .maxFetchTasks(4)
                .build()
        
        expect:
        scanOptions.getDurabilityFilter() == DurabilityLevel.MEMORY
        scanOptions.isDirty() == false
        scanOptions.getReadAheadBytes() == 4 * 1024 * 1024
        scanOptions.isCacheBlocks() == true
        scanOptions.getMaxFetchTasks() == 4
    }
    
    def "should validate null arguments"() {
        when:
        SlateDB.open(null, StoreConfig.local(), null)
        
        then:
        thrown(NullPointerException)
        
        when:
        SlateDB.open("/tmp/test", null, null)
        
        then:
        thrown(NullPointerException)
    }
    
    def "should handle WriteBatch lifecycle correctly"() {
        given:
        def batch = new WriteBatch()
        
        when:
        batch.put("key1".bytes, "value1".bytes)
        
        then:
        !batch.isClosed()
        !batch.isConsumed()
        
        when:
        batch.close()
        
        then:
        batch.isClosed()
        
        when:
        batch.put("key2".bytes, "value2".bytes)
        
        then:
        thrown(SlateDBException)
    }
    
    private SlateDB createTestDB() {
        def storeConfig = StoreConfig.local()
        def dbPath = tempDir.resolve("test-db-${UUID.randomUUID()}").toString()
        return SlateDB.open(dbPath, storeConfig, null)
    }
}

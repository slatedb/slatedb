package com.slatedb

import java.time.Duration
import java.util.concurrent.TimeUnit

import com.slatedb.config.AWSConfig
import com.slatedb.config.Provider
import com.slatedb.config.SlateDBOptions
import com.slatedb.config.StoreConfig
import spock.lang.Requires
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Tag
import spock.lang.Timeout

// SlateDB imports

/**
 * End-to-end integration tests for SlateDB with AWS S3 backend.
 * These tests require AWS credentials and S3 access.
 * 
 * Run with: ./gradlew e2eTest
 * 
 * Configuration via system properties:
 * - slatedb.test.s3.bucket (default: slatedb-sdk-dev)
 * - slatedb.test.aws.region (default: us-east-1)
 * - slatedb.test.aws.accessKey
 * - slatedb.test.aws.secretKey
 * 
 * Or environment variables:
 * - AWS_ACCESS_KEY_ID
 * - AWS_SECRET_ACCESS_KEY
 */
@Tag("e2e")
@Requires({ hasAwsCredentials() })
@Timeout(value = 5, unit = TimeUnit.MINUTES)
class SlateDBS3Test extends Specification {

    @Shared
    String s3Bucket
    
    @Shared
    String awsRegion
    
    @Shared
    String testKeyPrefix
    
    @Shared
    StoreConfig storeConfig
    
    def setupSpec() {
        s3Bucket = System.getProperty('slatedb.test.s3.bucket', 'slatedb-sdk-dev')
        awsRegion = System.getProperty('slatedb.test.aws.region', 'eu-west-2')
        testKeyPrefix = "e2e-test-${System.currentTimeMillis()}"
        
        // Create AWS configuration (credentials are handled via environment variables)
        def awsConfigBuilder = AWSConfig.builder()
            .bucket(s3Bucket)
            .region(awsRegion)
        
        def awsConfig = awsConfigBuilder.build()
        
        // Create S3 store configuration
        storeConfig = StoreConfig.builder()
            .provider(Provider.AWS)
            .aws(awsConfig)
            .build()
    }
    
    def "should open and close SlateDB with local backend"() {
        given: "SlateDB options for local backend"
        def localStoreConfig = StoreConfig.local()
        def options = SlateDBOptions.builder()
            .flushInterval(Duration.ofMillis(1000))
            .build()
            
        when: "opening SlateDB with local configuration"
        def db = SlateDB.open("/tmp/slatedb-e2e-local-${System.currentTimeMillis()}", localStoreConfig, options)
        
        then: "database should be successfully opened"
        db != null
        
        cleanup:
        db?.close()
    }

    def "should open and close SlateDB with S3 backend"() {
        given: "SlateDB options for S3 backend"
        def options = SlateDBOptions.builder()
            .flushInterval(Duration.ofMillis(1000))
            .build()
            
        when: "opening SlateDB with S3 configuration"
        def db = SlateDB.open("${testKeyPrefix}/basic-${System.currentTimeMillis()}", storeConfig, options)
        
        then: "database should be successfully opened"
        db != null
        
        cleanup:
        db?.close()
    }

    def "should perform basic CRUD operations with S3 backend"() {
        given: "an opened SlateDB instance with S3 backend"
        def options = SlateDBOptions.builder().build()
        def db = SlateDB.open("${testKeyPrefix}/crud-${System.currentTimeMillis()}", storeConfig, options)
        
        when: "putting a key-value pair"
        def key1 = "test-key-1".getBytes()
        def value1 = "test-value-1".getBytes()
        db.put(key1, value1)
        db.flush() // Ensure data is available for S3 backend
        
        then: "the value should be retrievable"
        db.get(key1) == value1

        when: "updating the value"
        def updatedValue1 = "updated-value-1".getBytes()
        db.put(key1, updatedValue1)
        db.flush() // Ensure updated data is available
        
        then: "the updated value should be retrievable"
        db.get(key1) == updatedValue1

        when: "deleting the key"
        db.delete(key1)
        
        then: "the key should no longer exist"
        db.get(key1) == null
        
        cleanup:
        db?.close()
    }

    def "should handle write batches with S3 backend"() {
        given: "an opened SlateDB instance with S3 backend"
        def options = SlateDBOptions.builder().build()
        def db = SlateDB.open("${testKeyPrefix}/batch-${System.currentTimeMillis()}", storeConfig, options)
        
        and: "a write batch with multiple operations"
        def batch = new WriteBatch()
        def key1 = "batch-key-1".getBytes()
        def key2 = "batch-key-2".getBytes()
        def key3 = "batch-key-3".getBytes()
        def value1 = "batch-value-1".getBytes()
        def value2 = "batch-value-2".getBytes()
        def value3 = "batch-value-3".getBytes()
        
        batch.put(key1, value1)
        batch.put(key2, value2)
        batch.put(key3, value3)
        batch.delete(key2)  // Delete one of them
        
        when: "writing the batch"
        db.write(batch)
        
        then: "the batch operations should be applied correctly"
        db.get(key1) == value1
        db.get(key2) == null  // Should be deleted
        db.get(key3) == value3

        cleanup:
        batch?.close()
        db?.close()
    }

    def "should iterate over keys with S3 backend"() {
        given: "an opened SlateDB instance with S3 backend"
        def options = SlateDBOptions.builder().build()
        def db = SlateDB.open("${testKeyPrefix}/iter-${System.currentTimeMillis()}", storeConfig, options)
        
        and: "multiple key-value pairs"
        def testData = [
            "iter-key-a": "value-a",
            "iter-key-b": "value-b", 
            "iter-key-c": "value-c",
            "iter-key-d": "value-d"
        ]
        
        testData.each { k, v ->
            db.put(k.getBytes(), v.getBytes())
        }
        
        when: "creating an iterator"
        def iterator = db.scan(null, null)
        def results = []
        
        while (iterator.hasNext()) {
            def kv = iterator.next()
            results.add([
                key: new String(kv.getKey()), 
                value: new String(kv.getValue())
            ])
        }
        
        then: "all key-value pairs should be iterated"
        results.size() >= 4  // At least our 4 pairs (may include others from previous tests)
        def ourResults = results.findAll { it.key.startsWith("iter-key-") }
        ourResults.size() == 4
        ourResults.find { it.key == "iter-key-a" && it.value == "value-a" }
        ourResults.find { it.key == "iter-key-b" && it.value == "value-b" }
        ourResults.find { it.key == "iter-key-c" && it.value == "value-c" }
        ourResults.find { it.key == "iter-key-d" && it.value == "value-d" }
        
        cleanup:
        iterator?.close()
        db?.close()
    }

    def "should handle large values with S3 backend"() {
        given: "an opened SlateDB instance with S3 backend"
        def options = SlateDBOptions.builder().build()
        def db = SlateDB.open("${testKeyPrefix}/large-${System.currentTimeMillis()}", storeConfig, options)
        
        and: "a large value (1MB)"
        def largeValue = ("x" * (1024 * 1024)).getBytes()  // 1MB byte array
        def largeKey = "large-key".getBytes()
        
        when: "storing the large value"
        db.put(largeKey, largeValue)
        
        then: "the large value should be retrievable"
        def retrievedValue = db.get(largeKey)
        Arrays.equals(retrievedValue, largeValue)
        retrievedValue.length == 1024 * 1024
        
        cleanup:
        db?.close()
    }

    def "should handle concurrent operations with S3 backend"() {
        given: "an opened SlateDB instance with S3 backend"
        def options = SlateDBOptions.builder()
            .flushInterval(Duration.ofMillis(500))  // More frequent flushes for concurrency test
            .build()
        def db = SlateDB.open("${testKeyPrefix}/concurrent-${System.currentTimeMillis()}", storeConfig, options)
        
        when: "performing concurrent operations"
        def threads = []
        def numThreads = 5
        def operationsPerThread = 10
        
        for (int i = 0; i < numThreads; i++) {
            final int threadId = i
            threads.add(Thread.start {
                for (int j = 0; j < operationsPerThread; j++) {
                    def key = "concurrent-key-${threadId}-${j}".getBytes()
                    def value = "concurrent-value-${threadId}-${j}".getBytes()
                    db.put(key, value)
                }
            })
        }
        
        // Wait for all threads to complete
        threads.each { it.join() }
        
        then: "all values should be present"
        def allPresent = true
        for (int i = 0; i < numThreads; i++) {
            for (int j = 0; j < operationsPerThread; j++) {
                def key = "concurrent-key-${i}-${j}".getBytes()
                def expectedValue = "concurrent-value-${i}-${j}".getBytes()
                def actualValue = db.get(key)
                if (!Arrays.equals(actualValue, expectedValue)) {
                    allPresent = false
                    println "Missing or incorrect value for key: concurrent-key-${i}-${j}, " +
                           "expected: ${new String(expectedValue)}, " +
                           "actual: ${actualValue ? new String(actualValue) : 'null'}"
                    break
                }
            }
            if (!allPresent) break
        }
        
        allPresent
        
        cleanup:
        db?.close()
    }

    def "should persist data across database reopenings with S3 backend"() {
        given: "a database path and test data"
        def dbPath = "${testKeyPrefix}/persist-${System.currentTimeMillis()}"
        def options = SlateDBOptions.builder().build()
        def testKey = "persist-key".getBytes()
        def testValue = "persist-value".getBytes()
        
        when: "storing data and closing database"
        def db1 = SlateDB.open(dbPath, storeConfig, options)
        db1.put(testKey, testValue)
        db1.close()
        
        and: "reopening the database"
        def db2 = SlateDB.open(dbPath, storeConfig, options)
        
        then: "the data should still be present"
        db2.get(testKey) == testValue

        cleanup:
        db2?.close()
    }

    private static boolean hasAwsCredentials() {
        def hasSystemProperty = System.getProperty('slatedb.test.aws.accessKey') != null
        def hasEnvironmentVar = System.getenv('AWS_ACCESS_KEY_ID') != null
        return hasSystemProperty || hasEnvironmentVar
    }
}

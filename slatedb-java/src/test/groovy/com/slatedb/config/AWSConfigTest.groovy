package com.slatedb.config

import com.slatedb.SlateDB
import spock.lang.Specification

class AWSConfigTest extends Specification {
    
    def "test AWS config serialization matches Go bindings format"() {
        given: "AWS config with test values"
        def awsConfig = AWSConfig.builder()
            .bucket("test-bucket")
            .region("us-east-1")
            .endpoint("http://localhost:4566")
            .build()
        
        def storeConfig = StoreConfig.builder()
            .provider(Provider.AWS)
            .aws(awsConfig)
            .build()
        
        when: "calling the SlateDB serialization method"
        def serializeMethod = SlateDB.class.getDeclaredMethod("serializeStoreConfig", StoreConfig.class)
        serializeMethod.setAccessible(true)
        def json = serializeMethod.invoke(null, storeConfig) as String
        println "Serialized JSON: $json"
        
        then: "JSON matches Go bindings format"
        json.contains('"provider":"aws"')
        json.contains('"bucket":"test-bucket"')
        json.contains('"region":"us-east-1"')
        json.contains('"endpoint":"http://localhost:4566"')
        json.contains('"request_timeout":null')
        
        and: "JSON does not contain fields that don't exist in Go bindings"
        !json.contains("access_key")
        !json.contains("secret_key")
        !json.contains("path_style_access")
    }
}

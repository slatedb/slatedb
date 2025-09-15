package com.slatedb.config;

import java.time.Duration;
import java.util.Objects;

/**
 * Configuration for AWS S3 object storage.
 * 
 * This class contains all the configuration options needed to connect to AWS S3
 * or S3-compatible object storage services, matching the Go bindings structure.
 */
public final class AWSConfig {
    private final String bucket;
    private final String region;
    private final String endpoint;
    private final Duration requestTimeout;
    
    private AWSConfig(Builder builder) {
        this.bucket = builder.bucket;
        this.region = builder.region;
        this.endpoint = builder.endpoint;
        this.requestTimeout = builder.requestTimeout;
    }
    
    /**
     * Gets the S3 bucket name.
     * 
     * @return the bucket name, or null if not specified
     */
    public String getBucket() {
        return bucket;
    }
    
    /**
     * Gets the AWS region.
     * 
     * @return the region, or null if not specified
     */
    public String getRegion() {
        return region;
    }
    
    /**
     * Gets the custom S3 endpoint (for S3-compatible services).
     * 
     * @return the endpoint URL, or null if using default AWS S3
     */
    public String getEndpoint() {
        return endpoint;
    }
    
    /**
     * Gets the request timeout duration.
     * 
     * @return the timeout duration, or null if using default
     */
    public Duration getRequestTimeout() {
        return requestTimeout;
    }
    
    /**
     * Creates a new builder for AWSConfig.
     * 
     * @return a new builder instance
     */
    public static Builder builder() {
        return new Builder();
    }
    
    /**
     * Creates a new builder initialized with values from this configuration.
     * 
     * @return a new builder instance
     */
    public Builder toBuilder() {
        return new Builder()
                .bucket(bucket)
                .region(region)
                .endpoint(endpoint)
                .requestTimeout(requestTimeout);
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        
        AWSConfig that = (AWSConfig) obj;
        return Objects.equals(bucket, that.bucket) &&
               Objects.equals(region, that.region) &&
               Objects.equals(endpoint, that.endpoint) &&
               Objects.equals(requestTimeout, that.requestTimeout);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(bucket, region, endpoint, requestTimeout);
    }
    
    @Override
    public String toString() {
        return "AWSConfig{" +
                "bucket='" + bucket + '\'' +
                ", region='" + region + '\'' +
                ", endpoint='" + endpoint + '\'' +
                ", requestTimeout=" + requestTimeout +
                '}';
    }
    
    /**
     * Builder for creating AWSConfig instances.
     */
    public static final class Builder {
        private String bucket;
        private String region;
        private String endpoint;
        private Duration requestTimeout;
        
        private Builder() {}
        
        /**
         * Sets the S3 bucket name.
         * 
         * @param bucket the bucket name
         * @return this builder
         */
        public Builder bucket(String bucket) {
            this.bucket = bucket;
            return this;
        }
        
        /**
         * Sets the AWS region.
         * 
         * @param region the region
         * @return this builder
         */
        public Builder region(String region) {
            this.region = region;
            return this;
        }
        
        /**
         * Sets the custom S3 endpoint for S3-compatible services.
         * 
         * @param endpoint the endpoint URL
         * @return this builder
         */
        public Builder endpoint(String endpoint) {
            this.endpoint = endpoint;
            return this;
        }
        
        /**
         * Sets the request timeout duration.
         * 
         * @param requestTimeout the timeout duration
         * @return this builder
         */
        public Builder requestTimeout(Duration requestTimeout) {
            this.requestTimeout = requestTimeout;
            return this;
        }
        
        /**
         * Builds the AWSConfig instance.
         * 
         * @return a new AWSConfig instance
         */
        public AWSConfig build() {
            return new AWSConfig(this);
        }
    }
}
package com.slatedb.config;

import java.util.Objects;

/**
 * Configuration for object storage providers in SlateDB.
 * 
 * This class specifies which object storage provider to use and its configuration.
 * Currently supports local filesystem and AWS S3 storage.
 */
public final class StoreConfig {
    private final Provider provider;
    private final AWSConfig aws;
    
    private StoreConfig(Builder builder) {
        this.provider = Objects.requireNonNull(builder.provider, "Provider cannot be null");
        this.aws = builder.aws;
        
        // Validation
        if (provider == Provider.AWS && aws == null) {
            throw new IllegalArgumentException("AWS configuration is required when using AWS provider");
        }
    }
    
    /**
     * Gets the storage provider.
     * 
     * @return the storage provider
     */
    public Provider getProvider() {
        return provider;
    }
    
    /**
     * Gets the AWS configuration.
     * 
     * @return the AWS configuration, or null if not using AWS provider
     */
    public AWSConfig getAws() {
        return aws;
    }
    
    /**
     * Creates a new builder for StoreConfig.
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
                .provider(provider)
                .aws(aws);
    }
    
    /**
     * Creates a StoreConfig for local filesystem storage.
     * 
     * @return a StoreConfig configured for local storage
     */
    public static StoreConfig local() {
        return builder().provider(Provider.LOCAL).build();
    }
    
    /**
     * Creates a StoreConfig for AWS S3 storage with the given configuration.
     * 
     * @param awsConfig the AWS configuration
     * @return a StoreConfig configured for AWS S3 storage
     */
    public static StoreConfig aws(AWSConfig awsConfig) {
        return builder().provider(Provider.AWS).aws(awsConfig).build();
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        
        StoreConfig that = (StoreConfig) obj;
        return provider == that.provider &&
               Objects.equals(aws, that.aws);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(provider, aws);
    }
    
    @Override
    public String toString() {
        return "StoreConfig{" +
                "provider=" + provider +
                ", aws=" + aws +
                '}';
    }
    
    /**
     * Builder for creating StoreConfig instances.
     */
    public static final class Builder {
        private Provider provider;
        private AWSConfig aws;
        
        private Builder() {}
        
        /**
         * Sets the storage provider.
         * 
         * @param provider the storage provider
         * @return this builder
         * @throws IllegalArgumentException if provider is null
         */
        public Builder provider(Provider provider) {
            this.provider = provider;
            return this;
        }
        
        /**
         * Sets the AWS configuration.
         * 
         * @param aws the AWS configuration
         * @return this builder
         */
        public Builder aws(AWSConfig aws) {
            this.aws = aws;
            return this;
        }
        
        /**
         * Builds the StoreConfig instance.
         * 
         * @return a new StoreConfig instance
         * @throws IllegalArgumentException if provider is null or if AWS provider is specified without AWS configuration
         */
        public StoreConfig build() {
            return new StoreConfig(this);
        }
    }
}
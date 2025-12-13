package com.titancache.config;

import com.titancache.core.TitanCache;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class CacheConfig {

    @Bean
    public TitanCache<String, String> titanCache() {
        return new TitanCache<>(100);
    }
}
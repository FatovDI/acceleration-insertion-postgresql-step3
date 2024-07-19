package com.example.postgresqlinsertion.config

import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.scheduling.annotation.EnableAsync
import java.util.concurrent.Executor
import java.util.concurrent.Executors


@Configuration
@EnableAsync
class SpringAsyncConfig {

    @Value("\${batch_insertion.pool_size}")
    private var poolSize: Int = 8

    @Bean(name = ["threadPoolAsyncInsertExecutor"])
    fun threadPoolAsyncInsertExecutor(): Executor {
        return Executors.newFixedThreadPool(poolSize)
    }

}
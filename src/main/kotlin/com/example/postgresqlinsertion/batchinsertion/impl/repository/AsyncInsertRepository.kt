package com.example.postgresqlinsertion.batchinsertion.impl.repository

import com.example.postgresqlinsertion.logic.entity.BaseAsyncInsertEntity
import org.hibernate.SessionFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import java.util.concurrent.Executors

abstract class AsyncInsertRepository<E: BaseAsyncInsertEntity> {

    @Value("\${batch_insertion.batch_size}")
    private var batchSize: Int = 5000

    @Value("\${batch_insertion.pool_size}")
    private var poolSize: Int = 8

    @Autowired
    lateinit var sessionFactory: SessionFactory

    private val executorService by lazy {
        Executors.newFixedThreadPool(poolSize)
    }

    open fun saveBatch(entities: List<E>): List<E> {
        val session = sessionFactory.openSession()
        val transaction = session.beginTransaction()
        entities.forEach { session.saveOrUpdate(it) }
        transaction.commit()
        session.close()
        return entities
    }

    fun saveAllByConcurrentBatch(entities: List<E>): List<E> {
        entities.forEach { it.readyToRead = false }
        val futures = entities.chunked(batchSize).map { executorService.submit<List<E>> { saveBatch(it) } }
        futures.flatMap { it.get() }
        entities.forEach { it.readyToRead = true }
        return saveBatch(entities)
    }
}

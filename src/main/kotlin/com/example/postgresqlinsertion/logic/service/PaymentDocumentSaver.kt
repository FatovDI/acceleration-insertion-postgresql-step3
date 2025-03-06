package com.example.postgresqlinsertion.logic.service

import com.example.postgresqlinsertion.logic.entity.PaymentDocumentEntity
import com.example.postgresqlinsertion.logic.repository.PaymentDocumentRepository
import org.hibernate.SessionFactory
import org.springframework.scheduling.annotation.Async
import org.springframework.stereotype.Component
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Future

@Component
class PaymentDocumentSaver(
    val paymentDocumentRepository: PaymentDocumentRepository,
    val sessionFactory: SessionFactory,
) {

    @Async("threadPoolAsyncInsertExecutor")
    fun saveBatchAsync(entities: List<PaymentDocumentEntity>): Future<List<PaymentDocumentEntity>> {
        val savedEntities = paymentDocumentRepository.saveAll(entities)
        return CompletableFuture.completedFuture(savedEntities)
    }

    fun batchUpdateBySession(entities: List<PaymentDocumentEntity>): List<PaymentDocumentEntity> {
        sessionFactory.openStatelessSession().use { session ->
            val transaction = session.beginTransaction()
            entities.forEach { session.update(it) }
            transaction.commit()
        }
        return entities
    }

}
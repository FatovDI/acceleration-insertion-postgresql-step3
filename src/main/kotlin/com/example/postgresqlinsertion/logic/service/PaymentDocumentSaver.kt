package com.example.postgresqlinsertion.logic.service

import com.example.postgresqlinsertion.logic.entity.PaymentDocumentEntity
import com.example.postgresqlinsertion.logic.repository.PaymentDocumentRepository
import org.hibernate.SessionFactory
import org.springframework.scheduling.annotation.Async
import org.springframework.scheduling.annotation.AsyncResult
import org.springframework.stereotype.Component
import java.util.concurrent.Future
import javax.transaction.Transactional

@Component
class PaymentDocumentSaver(
    val paymentDocumentRepository: PaymentDocumentRepository,
    val sessionFactory: SessionFactory,
) {

    @Transactional(Transactional.TxType.REQUIRES_NEW)
    @Async("threadPoolAsyncInsertExecutor")
    fun saveBatchAsync(entities: List<PaymentDocumentEntity>): Future<List<PaymentDocumentEntity>> {
        val savedEntities = paymentDocumentRepository.saveAll(entities)
        return AsyncResult(savedEntities)
    }

    @Async("threadPoolAsyncInsertExecutor")
    fun saveBatchBySessionAsync(entities: List<PaymentDocumentEntity>): Future<List<PaymentDocumentEntity>> {
        return AsyncResult(saveBatchBySession(entities))
    }

    fun saveBatchBySession(entities: List<PaymentDocumentEntity>): List<PaymentDocumentEntity> {
        val session = sessionFactory.openSession()
        val transaction = session.beginTransaction()
        entities.forEach { session.saveOrUpdate(it) }
        transaction.commit()
        session.close()
        return entities
    }

}
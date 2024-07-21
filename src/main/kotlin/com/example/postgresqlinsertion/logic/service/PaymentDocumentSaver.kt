package com.example.postgresqlinsertion.logic.service

import com.example.postgresqlinsertion.logic.entity.PaymentDocumentEntity
import com.example.postgresqlinsertion.logic.repository.PaymentDocumentRepository
import org.hibernate.SessionFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.scheduling.annotation.Async
import org.springframework.scheduling.annotation.AsyncResult
import org.springframework.stereotype.Component
import java.util.concurrent.Future
import javax.sql.DataSource
import javax.transaction.Transactional

@Component
class PaymentDocumentSaver(
    val paymentDocumentRepository: PaymentDocumentRepository,
    val sessionFactory: SessionFactory,
    final val dataSource: DataSource,
) {

    @Value("\${batch_insertion.batch_size}")
    private var batchSize: Int = 5000

    private val jdbcTemplate = JdbcTemplate(dataSource)

    fun setReadyToRead(idList: List<Long>): Array<IntArray> {
        return jdbcTemplate.batchUpdate(
            "update payment_document set ready_to_read = true where id = ?",
            idList, batchSize) {  ps, argument ->
            ps.setLong(1, argument)
        }
    }

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
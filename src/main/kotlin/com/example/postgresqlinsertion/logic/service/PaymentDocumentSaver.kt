package com.example.postgresqlinsertion.logic.service

import com.example.postgresqlinsertion.logic.entity.PaymentDocumentEntity
import com.example.postgresqlinsertion.logic.repository.PaymentDocumentRepository
import org.hibernate.SessionFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.scheduling.annotation.Async
import org.springframework.scheduling.annotation.AsyncResult
import org.springframework.stereotype.Component
import java.util.UUID
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

    fun setReadyToRead(transactionId: Long): Int {
        return jdbcTemplate.update(
            """
                update payment_document set ready_to_read = true where transaction_id = ?
            """.trimIndent()) {  ps ->
            ps.setObject(1, transactionId)
        }
    }

    fun setReadyToReadUnnest(idList: List<Long>): Int {
        return jdbcTemplate.update(
            """
                update payment_document pd set ready_to_read = true 
                from (select unnest(?) as id) as id_table where pd.id = id_table.id
            """.trimIndent()) {  ps ->
            ps.setObject(1, idList.toTypedArray())
        }
    }

    fun setReadyToReadArray(idList: List<Long>): Int {
        return jdbcTemplate.update(
            """
                update payment_document set ready_to_read = true where id = any (?)
            """.trimIndent()) {  ps ->
            ps.setObject(1, idList.toTypedArray())
        }
    }

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

    @Transactional(Transactional.TxType.REQUIRES_NEW)
    @Async("threadPoolAsyncInsertExecutor")
    fun setTransactionIdAsync(ids: List<Long>): Future<Array<IntArray>> {
        return AsyncResult(setTransactionId(ids))
    }

    fun setTransactionId(ids: List<Long>): Array<IntArray> {
        val uuid = UUID.randomUUID()
        return jdbcTemplate.batchUpdate(
            "update payment_document set transaction_id = ? where id = ?",
            ids, batchSize
        ) { ps, argument ->
            ps.setObject(1, uuid)
            ps.setLong(2, argument)
        }
    }

    @Async("threadPoolAsyncInsertExecutor")
    fun saveBatchBySessionAsync(entities: List<PaymentDocumentEntity>): Future<List<PaymentDocumentEntity>> {
        return AsyncResult(saveBatchBySession(entities))
    }

    fun saveBatchBySession(entities: List<PaymentDocumentEntity>): List<PaymentDocumentEntity> {
        sessionFactory.openSession().use { session ->
            val transaction = session.beginTransaction()
            entities.forEach { session.saveOrUpdate(it) }
            transaction.commit()
        }
        return entities
    }

}
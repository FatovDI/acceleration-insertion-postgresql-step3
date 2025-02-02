package com.example.postgresqlinsertion.logic.service

import com.example.postgresqlinsertion.batchinsertion.api.SqlHelper
import com.example.postgresqlinsertion.logic.entity.PaymentDocumentActiveTransactionEntity
import com.example.postgresqlinsertion.logic.entity.PaymentDocumentEntity
import com.example.postgresqlinsertion.logic.repository.PaymentDocumentActiveTransactionRepository
import com.example.postgresqlinsertion.logic.repository.PaymentDocumentRepository
import org.hibernate.SessionFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.scheduling.annotation.Async
import org.springframework.scheduling.annotation.AsyncResult
import org.springframework.stereotype.Component
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Future
import javax.sql.DataSource

@Component
class PaymentDocumentSaver(
    val paymentDocumentRepository: PaymentDocumentRepository,
    val activeTransactionRepository: PaymentDocumentActiveTransactionRepository,
    val sessionFactory: SessionFactory,
    val dataSource: DataSource,
    val sqlHelper: SqlHelper
) {

    @Value("\${batch_insertion.batch_size}")
    private var batchSize: Int = 5000

    private val jdbcTemplate = JdbcTemplate(dataSource)

    fun removeTransactionId(transactionId: UUID): Int {
        return jdbcTemplate.update(
            "update payment_document set transaction_id = null where transaction_id = ?"
        ) {  ps ->
            ps.setObject(1, transactionId)
        }
    }

    fun setReadyToRead(transactionId: UUID): Int {
        return jdbcTemplate.update(
            "update payment_document set ready_to_read = true  where transaction_id = ?") {  ps ->
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
            "update payment_document set ready_to_read = true where id = any (?)"
        ) {  ps ->
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

    @Async("threadPoolAsyncInsertExecutor")
    fun saveBatchAsync(entities: List<PaymentDocumentEntity>): Future<List<PaymentDocumentEntity>> {
        val savedEntities = paymentDocumentRepository.saveAll(entities)
        return CompletableFuture.completedFuture(savedEntities)
    }

    @Async("threadPoolAsyncInsertExecutor")
    fun saveBatchAsync(
        entities: List<PaymentDocumentEntity>,
        transactionId: UUID
    ): Future<List<PaymentDocumentEntity>> {
        val savedEntities = paymentDocumentRepository.saveAll(entities)
        return CompletableFuture.completedFuture(savedEntities)
    }
//    @Async("threadPoolAsyncInsertExecutor")
//    fun saveBatchAsync(
//        entities: List<PaymentDocumentEntity>,
//        transactionId: UUID
//    ): Future<List<PaymentDocumentEntity>> {
//        val savedEntities = entities
//            .map { PaymentDocumentActiveTransactionEntity(paymentDocument = it, transactionId = transactionId) }
//            .let { activeTransactionRepository.saveAll(it) }
//            .mapNotNull { it.paymentDocument }
//
//        return CompletableFuture.completedFuture(savedEntities)
//    }

    @Async("threadPoolAsyncInsertExecutor")
    fun setTransactionIdAsync(ids: List<Long>): Future<Array<IntArray>> {
        return AsyncResult(setTransactionId(ids))
    }

    fun setTransactionId(ids: List<Long>): Array<IntArray> {
//        val transactionId = getRandomString(10)
//        val transactionId = UUID.randomUUID()
        val transactionId = sqlHelper.nextTransactionId()
        return jdbcTemplate.batchUpdate(
            "update payment_document set transaction_id = ? where id = ?",
            ids, batchSize
        ) { ps, argument ->
            ps.setObject(1, transactionId)
            ps.setLong(2, argument)
        }
    }

    @Async("threadPoolAsyncInsertExecutor")
    fun saveBatchBySessionAsync(entities: List<PaymentDocumentEntity>): Future<List<PaymentDocumentEntity>> {
        return AsyncResult(batchUpdateBySession(entities))
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
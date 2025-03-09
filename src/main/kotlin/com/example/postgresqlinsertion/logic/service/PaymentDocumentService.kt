package com.example.postgresqlinsertion.logic.service

import com.example.postgresqlinsertion.logic.entity.AccountEntity
import com.example.postgresqlinsertion.logic.entity.CurrencyEntity
import com.example.postgresqlinsertion.logic.entity.PaymentDocumentActiveTransactionEntity
import com.example.postgresqlinsertion.logic.entity.PaymentDocumentEntity
import com.example.postgresqlinsertion.logic.repository.AccountRepository
import com.example.postgresqlinsertion.logic.repository.CurrencyRepository
import com.example.postgresqlinsertion.logic.repository.PaymentDocumentActiveTransactionRepository
import com.example.postgresqlinsertion.logic.repository.PaymentDocumentCustomRepository
import com.example.postgresqlinsertion.logic.sys.SqlHelper
import com.example.postgresqlinsertion.utils.getRandomString
import com.example.postgresqlinsertion.utils.logger
import com.fasterxml.uuid.Generators
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import java.math.BigDecimal
import java.math.BigInteger
import java.time.LocalDate
import java.util.*
import java.util.concurrent.Future
import javax.persistence.EntityManager
import javax.persistence.PersistenceContext
import javax.transaction.Transactional
import kotlin.random.Random


@Service
class PaymentDocumentService(
    @Value("\${batch_insertion.batch_size}")
    private val batchSize: Int,
    private val accountRepo: AccountRepository,
    private val currencyRepo: CurrencyRepository,
    private val sqlHelper: SqlHelper,
    private val repository: PaymentDocumentCustomRepository,
    private val saver: PaymentDocumentSaver,
    private val activeTransactionRepository: PaymentDocumentActiveTransactionRepository,
) {

    @PersistenceContext
    lateinit var entityManager: EntityManager

    private val log by logger()

    fun saveBySpringConcurrent(count: Int): List<Long> {
        val currencies = currencyRepo.findAll()
        val accounts = accountRepo.findAll()

        var listForSave = mutableListOf<PaymentDocumentEntity>()
        val saveTasks = mutableListOf<Future<List<PaymentDocumentEntity>>>()
        (1..count).forEach {
            listForSave.add(getRandomEntity(null, currencies.random(), accounts.random()))
            if (it != 0 && it % batchSize == 0) {
                saveTasks.add(saver.saveBatchAsync(listForSave))
                listForSave = mutableListOf()
            }
        }
        listForSave.takeIf { it.isNotEmpty() }?.let { saveTasks.add(saver.saveBatchAsync(it)) }
        return saveTasks.flatMap { it.get().mapNotNull { pd -> pd.id } }
    }

    fun saveBySpringConcurrentAtomic(count: Int): Int {
        val currencies = currencyRepo.findAll()
        val accounts = accountRepo.findAll()

        var listForSave = mutableListOf<PaymentDocumentEntity>()
        val saveTasks = mutableListOf<Future<List<PaymentDocumentEntity>>>()
        (1..count).forEach {
            listForSave.add(getRandomEntity(null, currencies.random(), accounts.random())
//                .apply { readyToRead = false }
            )
            if (it != 0 && it % batchSize == 0) {
                saveTasks.add(saver.saveBatchAsync(listForSave))
                listForSave = mutableListOf()
            }
        }
        listForSave.takeIf { it.isNotEmpty() }?.let { saveTasks.add(saver.saveBatchAsync(it)) }

        val ids = saveTasks.flatMap { it.get().mapNotNull { pd -> pd.id } }
        return saver.setReadyToReadArray(ids)
    }

    fun saveBySpringConcurrentWithTransactionIdAtomic(count: Int): Int {
        val currencies = currencyRepo.findAll()
        val accounts = accountRepo.findAll()

        var listForSave = mutableListOf<PaymentDocumentEntity>()
        val saveTasks = mutableListOf<Future<List<PaymentDocumentEntity>>>()
        val transactionId = Generators.timeBasedEpochGenerator().generate()
        (1..count).forEach {
            listForSave.add(
                getRandomEntity(null, currencies.random(), accounts.random(), transactionId)
//                    .apply { readyToRead = false }
            )
            if (it != 0 && it % batchSize == 0) {
                saveTasks.add(saver.saveBatchAsync(listForSave))
                listForSave = mutableListOf()
            }
        }
        listForSave.takeIf { it.isNotEmpty() }?.let { saveTasks.add(saver.saveBatchAsync(it)) }

        saveTasks.flatMap { it.get() }
        return saver.setReadyToRead(transactionId)
    }

    fun saveByConcurrentAtomicWithTransactionIdInAnotherTable(count: Int): Int {
        val currencies = currencyRepo.findAll()
        val accounts = accountRepo.findAll()

        var listForSave = mutableListOf<PaymentDocumentEntity>()
        val saveTasks = mutableListOf<Future<List<PaymentDocumentEntity>>>()
        val transactionId = Generators.timeBasedEpochGenerator().generate()
        (1..count).forEach {
            listForSave.add(
                getRandomEntity(null, currencies.random(), accounts.random())
            )
            if (it != 0 && it % batchSize == 0) {
                saveTasks.add(saver.saveBatchAsync(listForSave, transactionId))
                listForSave = mutableListOf()
            }
        }
        listForSave.takeIf { it.isNotEmpty() }?.let { saveTasks.add(saver.saveBatchAsync(it, transactionId)) }

        val docs = saveTasks.flatMap { it.get() }
        activeTransactionRepository.deleteAllByTransactionId(transactionId)
        return docs.size
    }

    fun saveByConcurrentAtomicWithTransactionIdInTwoTable(count: Int): Int {
        val currencies = currencyRepo.findAll()
        val accounts = accountRepo.findAll()

        var listForSave = mutableListOf<PaymentDocumentEntity>()
        val saveTasks = mutableListOf<Future<List<PaymentDocumentEntity>>>()
        val transactionId = Generators.timeBasedEpochGenerator().generate()
        activeTransactionRepository.saveAndFlush(PaymentDocumentActiveTransactionEntity(transactionId = transactionId))
        (1..count).forEach {
            listForSave.add(
                getRandomEntity(null, currencies.random(), accounts.random(), transactionId)
            )
            if (it != 0 && it % batchSize == 0) {
                saveTasks.add(saver.saveBatchAsync(listForSave))
                listForSave = mutableListOf()
            }
        }
        listForSave.takeIf { it.isNotEmpty() }?.let { saveTasks.add(saver.saveBatchAsync(it)) }

        val docs = saveTasks.flatMap { it.get() }
        activeTransactionRepository.deleteAllByTransactionId(transactionId)
        return docs.size
    }

    fun saveBySpringConcurrentWithTransactionId(count: Int, transactionId: UUID? = null) {
        val currencies = currencyRepo.findAll()
        val accounts = accountRepo.findAll()

        log.info("start save $count by Spring with async")
        val listForSave = mutableListOf<PaymentDocumentEntity>()
        for (i in 0 until count) {
//            listForSave.add(getRandomEntity(null, currencies.random(), accounts.random(), null, transactionId))
            listForSave.add(getRandomEntity(null, currencies.random(), accounts.random(), transactionId, null)
//                .apply { readyToRead = false }
            )
        }
        listForSave.chunked(batchSize).map { saver.saveBatchAsync(it) }.flatMap { it.get() }
        log.info("end save $count by Spring with async")
    }

    fun saveBySpringConcurrentWithSession(count: Int) {
        val currencies = currencyRepo.findAll()
        val accounts = accountRepo.findAll()

        log.info("start save $count by Spring with async and session")
        val listForSave = mutableListOf<PaymentDocumentEntity>()
        for (i in 0 until count) {
            listForSave.add(getRandomEntity(null, currencies.random(), accounts.random()))
        }
        listForSave.chunked(batchSize).map { saver.saveBatchBySessionAsync(it) }.flatMap { it.get() }
        log.info("end save $count by Spring with async and session")
    }

    @Transactional
    fun saveBySpring(count: Int, transactionId: UUID? = null) {
        val currencies = currencyRepo.findAll()
        val accounts = accountRepo.findAll()

        log.info("start save $count by Spring")
        for (i in 0 until count) {
            repository.save(
                getRandomEntity(null, currencies.random(), accounts.random(), transactionId, null)
            )
        }
        log.info("end save $count by Spring")
    }

    @Transactional
    fun updateBySpring(count: Int) {
        val listId = sqlHelper.getIdListForUpdate(count, PaymentDocumentEntity::class)
        val currencies = currencyRepo.findAll()
        val accounts = accountRepo.findAll()

        for (i in 0 until count) {
            repository.save(getRandomEntity(listId[i], currencies.random(), accounts.random()))
        }
    }

    fun updateBySpringWithSession(count: Int) {
        val listId = sqlHelper.getIdListForUpdate(count, PaymentDocumentEntity::class)
        val currencies = currencyRepo.findAll()
        val accounts = accountRepo.findAll()

        val listForSave = mutableListOf<PaymentDocumentEntity>()
        for (i in 0 until count) {
            listForSave.add(getRandomEntity(listId[i], currencies.random(), accounts.random()))
        }
        saver.batchUpdateBySession(listForSave)
    }

    fun setReadyToRead(count: Int): Int {
        log.info("get list id for set ready to read $count")
        val listId = sqlHelper.getIdListForSetReadyToRead(count, PaymentDocumentEntity::class)
        log.info("start set ready to read $count")
        return saver.setReadyToRead(listId).sumOf { it.sum() }
    }

    fun setReadyToReadUnnest(count: Int, ids: List<Long>? = null): Int {
        log.info("get list id for set ready to read with unnest $count")
        val listId = ids ?: sqlHelper.getIdListForSetReadyToRead(count, PaymentDocumentEntity::class)
        log.info("start set ready to read with unnest $count")
        return saver.setReadyToReadUnnest(listId)
    }

    fun setReadyToReadArray(count: Int, ids: List<Long>? = null): Int {
        log.info("get list id for set ready to read with array $count")
        val listId = ids ?: sqlHelper.getIdListForSetReadyToRead(count, PaymentDocumentEntity::class)
        log.info("start set ready to read with array $count")
        return saver.setReadyToReadArray(listId)
    }

    fun saveByConcurrentAtomic(count: Int): Int {
        val currencies = currencyRepo.findAll()
        val accounts = accountRepo.findAll()

        var listForSave = mutableListOf<PaymentDocumentEntity>()
        val saveTasks = mutableListOf<Future<List<PaymentDocumentEntity>>>()
        val transactionId = Generators.timeBasedEpochGenerator().generate()
        (1..count).forEach {
            listForSave.add(
                getRandomEntity(null, currencies.random(), accounts.random(), transactionId)
            )
            if (it != 0 && it % batchSize == 0) {
                saveTasks.add(saver.saveBatchAsync(listForSave))
                listForSave = mutableListOf()
            }
        }
        listForSave.takeIf { it.isNotEmpty() }?.let { saveTasks.add(saver.saveBatchAsync(it)) }
        saveTasks.flatMap { it.get() }

        return saver.removeTransactionId(transactionId)

    }

    fun saveByConcurrentAtomicWithReadyToRead(count: Int): Int {
        val currencies = currencyRepo.findAll()
        val accounts = accountRepo.findAll()

        log.info("start save $count by Spring with async")

        val listForSave = mutableListOf<PaymentDocumentEntity>()
        val transactionId = Generators.timeBasedEpochGenerator().generate()
        for (i in 0 until count) {
            listForSave.add(
                getRandomEntity(null, currencies.random(), accounts.random(), transactionId, null)
//                    .apply { readyToRead = false }
            )
        }
        listForSave.chunked(batchSize).map { saver.saveBatchAsync(it) }.flatMap { it.get() }
        return saver.setReadyToRead(transactionId)

    }

    fun setReadyToReadByTransactionId(transactionId: UUID): Int {
        return saver.setReadyToRead(transactionId)
    }

    fun removeTransactionId(transactionId: UUID): Int {
        return saver.removeTransactionId(transactionId)
    }

    fun setTransactionId(count: Int, countRow: Int = 4000000): Int {
        val results = mutableListOf<Future<Array<IntArray>>>()
        val listId = entityManager.createNativeQuery("select id from payment_document where transaction_id is null limit ${countRow*count}").resultList as List<BigInteger>
        listId.chunked(countRow).forEach {ids ->
            results.add(saver.setTransactionIdAsync(ids.map { it.toLong() }))
        }
        return results.flatMap { it.get().toList() }.sumOf { it.sum() }
    }

    private fun getRandomEntity(
        id: Long?,
        cur: CurrencyEntity,
        account: AccountEntity,
        transactionId: UUID? = null,
        orderNumber: String? = null
    ): PaymentDocumentEntity {
        return PaymentDocumentEntity(
            orderDate = LocalDate.now(),
            orderNumber = orderNumber?: getRandomString(10),
            amount = BigDecimal.valueOf(Random.nextDouble()),
            cur = cur,
            expense = Random.nextBoolean(),
            account = account,
            paymentPurpose = getRandomString(100),
            prop10 = getRandomString(10),
            prop15 = getRandomString(15),
            prop20 = getRandomString(20),
        )
            .apply { this.id = id }
            .apply { this.transactionId = transactionId }
//            .apply { this.readyToRead = transactionId?.let { false }?: true }
    }

}
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
import java.time.LocalDate
import java.util.*
import java.util.concurrent.Future
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
    }

}
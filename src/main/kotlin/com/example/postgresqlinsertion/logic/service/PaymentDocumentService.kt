package com.example.postgresqlinsertion.logic.service

import com.example.postgresqlinsertion.batchinsertion.api.SqlHelper
import com.example.postgresqlinsertion.batchinsertion.api.factory.BatchInsertionByEntityFactory
import com.example.postgresqlinsertion.batchinsertion.api.factory.BatchInsertionByPropertyFactory
import com.example.postgresqlinsertion.batchinsertion.api.factory.SaverType
import com.example.postgresqlinsertion.batchinsertion.api.processor.BatchInsertionByPropertyProcessor
import com.example.postgresqlinsertion.batchinsertion.getColumnsString
import com.example.postgresqlinsertion.batchinsertion.getColumnsStringByClass
import com.example.postgresqlinsertion.batchinsertion.utils.getRandomString
import com.example.postgresqlinsertion.batchinsertion.utils.logger
import com.example.postgresqlinsertion.logic.entity.AccountEntity
import com.example.postgresqlinsertion.logic.entity.CurrencyEntity
import com.example.postgresqlinsertion.logic.entity.PaymentDocumentEntity
import com.example.postgresqlinsertion.logic.repository.*
import org.springframework.beans.factory.annotation.Value
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.jdbc.core.namedparam.SqlParameterSourceUtils
import org.springframework.stereotype.Service
import java.math.BigDecimal
import java.math.BigInteger
import java.time.LocalDate
import java.util.*
import java.util.concurrent.Future
import javax.persistence.EntityManager
import javax.persistence.PersistenceContext
import javax.sql.DataSource
import javax.transaction.Transactional
import kotlin.random.Random
import kotlin.reflect.KMutableProperty1


@Service
class PaymentDocumentService(
    @Value("\${batch_insertion.batch_size}")
    private val batchSize: Int,
    private val accountRepo: AccountRepository,
    private val currencyRepo: CurrencyRepository,
    private val sqlHelper: SqlHelper,
    private val pdBatchByEntitySaverFactory: BatchInsertionByEntityFactory<PaymentDocumentEntity>,
    private val pdBatchByPropertySaverFactory: BatchInsertionByPropertyFactory<PaymentDocumentEntity>,
    private val repository: PaymentDocumentCustomRepository,
    private val jdbcTemplate: JdbcTemplate,
    private val namedJdbcTemplate: NamedParameterJdbcTemplate,
    private val pdCrudRepository: PaymentDocumentCrudRepository,
    private val byPropertyProcessor: BatchInsertionByPropertyProcessor,
    private val dataSource: DataSource,
    private val asyncRepo: PaymentDocumentAsyncInsertRepository,
    private val saver: PaymentDocumentSaver,
) {

    @PersistenceContext
    lateinit var entityManager: EntityManager

    private val log by logger()

    fun saveBySpringConcurrent(count: Int) {
        val currencies = currencyRepo.findAll()
        val accounts = accountRepo.findAll()

        log.info("start save $count by Spring with async")
        val listForSave = mutableListOf<PaymentDocumentEntity>()
        for (i in 0 until count) {
            listForSave.add(getRandomEntity(null, currencies.random(), accounts.random()))
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
    fun saveByConcurrentBatch(count: Int) {
        val currencies = currencyRepo.findAll()
        val accounts = accountRepo.findAll()

        log.info("start save $count by Spring with async repository")
        val listForSave = mutableListOf<PaymentDocumentEntity>()
        for (i in 0 until count) {
            listForSave.add(getRandomEntity(null, currencies.random(), accounts.random()))
        }
        asyncRepo.saveAllByConcurrentBatch(listForSave)
        log.info("end save $count by Spring with async repository")
    }

    fun saveByCopy(count: Int) {
        val currencies = currencyRepo.findAll()
        val accounts = accountRepo.findAll()

        pdBatchByEntitySaverFactory.getSaver(SaverType.COPY).use { saver ->
            for (i in 0 until count) {
                saver.addDataForSave(getRandomEntity(null, currencies.random(), accounts.random()))
            }
            saver.commit()
        }

    }

    fun saveByCopyBinary(count: Int) {
        val currencies = currencyRepo.findAll()
        val accounts = accountRepo.findAll()

        pdBatchByEntitySaverFactory.getSaver(SaverType.COPY_BINARY).use { saver ->
            for (i in 0 until count) {
                saver.addDataForSave(getRandomEntity(null, currencies.random(), accounts.random()))
            }
            saver.commit()
        }

    }

    fun saveByCopyAndKProperty(count: Int) {
        val currencies = currencyRepo.findAll()
        val accounts = accountRepo.findAll()
        val data = mutableMapOf<KMutableProperty1<PaymentDocumentEntity, *>, Any?>()

        log.info("start collect data for copy saver by property $count")

        pdBatchByPropertySaverFactory.getSaver(SaverType.COPY).use { saver ->
            for (i in 0 until count) {
                fillRandomDataByKProperty(null, currencies.random(), accounts.random(), data)
                saver.addDataForSave(data)
                if (i != 0 && i % batchSize == 0) {
                    log.info("save batch insertion $batchSize by copy method by property ")
                    saver.saveData(data.keys)
                }
            }
            saver.saveData(data.keys)
            log.info("start commit data by copy method by property $count to DB")
            saver.commit()
        }

        log.info("end save data by copy method by property $count")
    }

    fun saveByCopyBinaryAndKProperty(count: Int) {
        val currencies = currencyRepo.findAll()
        val accounts = accountRepo.findAll()
        val data = mutableMapOf<KMutableProperty1<PaymentDocumentEntity, *>, Any?>()

        log.info("start collect binary data for copy saver by property $count")

        pdBatchByPropertySaverFactory.getSaver(SaverType.COPY_BINARY).use { saver ->
            for (i in 0 until count) {
                fillRandomDataByKProperty(null, currencies.random(), accounts.random(), data)
                saver.addDataForSave(data)
                if (i != 0 && i % batchSize == 0) {
                    log.info("save batch insertion $batchSize by copy with binary data method by property")
                    saver.saveData(data.keys)
                }
            }
            saver.saveData(data.keys)
            log.info("start commit binary data by copy method by property $count to DB")
            saver.commit()
        }

        log.info("end save binary data by copy method by property $count")
    }

    fun saveByCopyViaFile(count: Int) {
        val currencies = currencyRepo.findAll()
        val accounts = accountRepo.findAll()

        pdBatchByEntitySaverFactory.getSaver(SaverType.COPY_VIA_FILE).use { saver ->
            for (i in 0 until count) {
                saver.addDataForSave(getRandomEntity(null, currencies.random(), accounts.random()))
            }
            saver.commit()
        }

    }

    fun saveByCopyViaBinaryFile(count: Int) {
        val currencies = currencyRepo.findAll()
        val accounts = accountRepo.findAll()

        pdBatchByEntitySaverFactory.getSaver(SaverType.COPY_BINARY_VIA_FILE).use { saver ->
            for (i in 0 until count) {
                saver.addDataForSave(getRandomEntity(null, currencies.random(), accounts.random()))
            }
            saver.commit()
        }

    }

    fun saveByCopyAnpPropertyViaFile(count: Int) {
        val currencies = currencyRepo.findAll()
        val accounts = accountRepo.findAll()
        val data = mutableMapOf<KMutableProperty1<PaymentDocumentEntity, *>, Any?>()

        log.info("start creation file by property $count")

        pdBatchByPropertySaverFactory.getSaver(SaverType.COPY_VIA_FILE).use { saver ->
            for (i in 0 until count) {
                fillRandomDataByKProperty(null, currencies.random(), accounts.random(), data)
                saver.addDataForSave(data)
            }

            log.info("start save file by property $count to DB")

            saver.saveData(data.keys)
            saver.commit()
        }

        log.info("end save file by property $count")

    }

    fun saveByCopyAnpPropertyViaBinaryFile(count: Int) {
        val currencies = currencyRepo.findAll()
        val accounts = accountRepo.findAll()
        val data = mutableMapOf<KMutableProperty1<PaymentDocumentEntity, *>, Any?>()

        log.info("start creation binary file by property $count")

        pdBatchByPropertySaverFactory.getSaver(SaverType.COPY_BINARY_VIA_FILE).use { saver ->
            for (i in 0 until count) {
                fillRandomDataByKProperty(null, currencies.random(), accounts.random(), data)
                saver.addDataForSave(data)
            }

            log.info("start save binary file by property $count to DB")

            saver.saveData(data.keys)
            saver.commit()
        }

        log.info("end save binary file by property $count")

    }

    fun saveByInsert(count: Int) {
        val currencies = currencyRepo.findAll()
        val accounts = accountRepo.findAll()

        pdBatchByEntitySaverFactory.getSaver(SaverType.INSERT).use { saver ->
            for (i in 0 until count) {
                saver.addDataForSave(getRandomEntity(null, currencies.random(), accounts.random()))
            }
            saver.commit()
        }

    }

    fun saveByInsertWithPreparedStatement(count: Int, orderNumber: String? = null) {
        val currencies = currencyRepo.findAll()
        val accounts = accountRepo.findAll()

        pdBatchByEntitySaverFactory.getSaver(SaverType.INSERT_PREPARED_STATEMENT).use { saver ->
            for (i in 0 until count) {
                saver.addDataForSave(getRandomEntity(null, currencies.random(), accounts.random(), orderNumber))
            }
            saver.commit()
        }

    }

    fun saveByInsertWithPreparedStatementAndUnnest(count: Int, orderNumber: String? = null) {
        val currencies = currencyRepo.findAll()
        val accounts = accountRepo.findAll()

        pdBatchByEntitySaverFactory.getSaver(SaverType.INSERT_PREPARED_STATEMENT_UNNEST).use { saver ->
            for (i in 0 until count) {
                saver.addDataForSave(getRandomEntity(null, currencies.random(), accounts.random(), orderNumber))
            }
            saver.commit()
        }

    }

    fun update(count: Int) {
        val listId = sqlHelper.getIdListForUpdate(count, PaymentDocumentEntity::class)
        val currencies = currencyRepo.findAll()
        val accounts = accountRepo.findAll()

        pdBatchByEntitySaverFactory.getSaver(SaverType.UPDATE).use { saver ->
            for (i in 0 until count) {
                saver.addDataForSave(getRandomEntity(listId[i], currencies.random(), accounts.random()))
            }
            saver.commit()
        }

    }

    fun updatePreparedStatement(count: Int) {
        val listId = sqlHelper.getIdListForUpdate(count, PaymentDocumentEntity::class)
        val currencies = currencyRepo.findAll()
        val accounts = accountRepo.findAll()

        pdBatchByEntitySaverFactory.getSaver(SaverType.UPDATE_PREPARED_STATEMENT).use { saver ->
            for (i in 0 until count) {
                saver.addDataForSave(getRandomEntity(listId[i], currencies.random(), accounts.random()))
            }
            saver.commit()
        }

    }

    fun saveByInsertAndProperty(count: Int) {
        val currencies = currencyRepo.findAll()
        val accounts = accountRepo.findAll()
        val data = mutableMapOf<KMutableProperty1<PaymentDocumentEntity, *>, Any?>()

        log.info("start collect insertion $count by property")

        pdBatchByPropertySaverFactory.getSaver(SaverType.INSERT).use { saver ->
            for (i in 0 until count) {
                fillRandomDataByKProperty(null, currencies.random(), accounts.random(), data)
                saver.addDataForSave(data)
                if (i != 0 && i % batchSize == 0) {
                    log.info("save batch insertion $batchSize by property")
                    saver.saveData(data.keys)
                }
            }
            saver.saveData(data.keys)
            log.info("start commit insert collection $count by property")
            saver.commit()
        }

        log.info("end save insert collection $count by property")

    }

    fun updateByProperty(count: Int) {
        val listId = sqlHelper.getIdListForUpdate(count, PaymentDocumentEntity::class)
        val currencies = currencyRepo.findAll()
        val accounts = accountRepo.findAll()
        val data = mutableMapOf<KMutableProperty1<PaymentDocumentEntity, *>, Any?>()

        log.info("start update $count by property")

        pdBatchByPropertySaverFactory.getSaver(SaverType.UPDATE).use { saver ->
            for (i in 0 until count) {
                fillRandomDataByKProperty(listId[i], currencies.random(), accounts.random(), data)
                saver.addDataForSave(data)
                if (i != 0 && i % batchSize == 0) {
                    log.info("save batch update $batchSize by property")
                    saver.saveData(data.keys)
                }
            }
            saver.saveData(data.keys)
            log.info("start commit update collection $count by property")
            saver.commit()
        }

        log.info("end update collection $count by property")

    }

    fun updateByPropertyPreparedStatement(count: Int) {
        val listId = sqlHelper.getIdListForUpdate(count, PaymentDocumentEntity::class)
        val currencies = currencyRepo.findAll()
        val accounts = accountRepo.findAll()
        val data = mutableMapOf<KMutableProperty1<PaymentDocumentEntity, *>, Any?>()

        log.info("start update $count by property prepared statement")

        pdBatchByPropertySaverFactory.getSaver(SaverType.UPDATE_PREPARED_STATEMENT).use { saver ->
            for (i in 0 until count) {
                fillRandomDataByKProperty(listId[i], currencies.random(), accounts.random(), data)
                saver.addDataForSave(data)
                if (i != 0 && i % batchSize == 0) {
                    log.info("save batch update $batchSize by property prepared statemen")
                    saver.saveData(data.keys)
                }
            }
            saver.saveData(data.keys)
            log.info("start commit update collection $count by property prepared statemen")
            saver.commit()
        }

        log.info("end update collection $count by property prepared statemen")

    }

    fun updateOnlyOneFieldByProperty(count: Int) {
        val listId = sqlHelper.getIdListForUpdate(count, PaymentDocumentEntity::class)
        val data = mutableMapOf<KMutableProperty1<PaymentDocumentEntity, *>, String?>()

        log.info("start update only one field $count by property")

        pdBatchByPropertySaverFactory.getSaver(SaverType.UPDATE).use { saver ->
            for (i in 0 until count) {
                data[PaymentDocumentEntity::id] = listId[i].toString()
                data[PaymentDocumentEntity::prop10] = getRandomString(10)
                saver.addDataForSave(data)
                if (i != 0 && i % batchSize == 0) {
                    log.info("save batch update only one field $batchSize by property")
                    saver.saveData(data.keys)
                }
            }
            saver.saveData(data.keys)
            log.info("start commit update only one field collection $count by property")
            saver.commit()
        }

        log.info("end update only one field collection $count by property")

    }

    fun updateOnlyOneFieldByPropertyPreparedStatement(count: Int) {
        val listId = sqlHelper.getIdListForUpdate(count, PaymentDocumentEntity::class)
        val data = mutableMapOf<KMutableProperty1<PaymentDocumentEntity, *>, String?>()

        log.info("start update only one field $count by property prepared statement")

        pdBatchByPropertySaverFactory.getSaver(SaverType.UPDATE_PREPARED_STATEMENT).use { saver ->
            for (i in 0 until count) {
                data[PaymentDocumentEntity::id] = listId[i].toString()
                data[PaymentDocumentEntity::prop10] = getRandomString(10)
                saver.addDataForSave(data)
                if (i != 0 && i % batchSize == 0) {
                    log.info("save batch update only one field $batchSize by property prepared statement")
                    saver.saveData(data.keys)
                }
            }
            saver.saveData(data.keys)
            log.info("start commit update only one field collection $count by property prepared statement")
            saver.commit()
        }

        log.info("end update only one field collection $count by property prepared statement")

    }

    fun updateOnlyOneFieldWithCommonCondition(orderNumber: String, prop10: String): Int {
        val connection = dataSource.connection

        log.info("start update only one field with common condition")

        val count = byPropertyProcessor.updateDataToDataBasePreparedStatement(
            PaymentDocumentEntity::class,
            setOf(PaymentDocumentEntity::prop10),
            listOf(listOf(prop10, orderNumber)),
            listOf("order_number"),
            connection
        )

        connection.close()

        log.info("end update only one field collection with common condition")

        return count

    }

    fun saveByInsertWithDropIndex(count: Int) {
        val currencies = currencyRepo.findAll()
        val accounts = accountRepo.findAll()

        log.info("start drop index before insertion $count")

        val scriptForCreateIndexes = sqlHelper.dropIndex(PaymentDocumentEntity::class)

        pdBatchByEntitySaverFactory.getSaver(SaverType.INSERT_PREPARED_STATEMENT).use { saver ->
            for (i in 0 until count) {
                saver.addDataForSave(getRandomEntity(null, currencies.random(), accounts.random()))
            }
            saver.commit()
        }

        log.info("start create index after insertion $count")

        sqlHelper.executeScript(scriptForCreateIndexes)

        log.info("stop create index after insertion $count")

    }

    @Transactional
    fun saveAllBySpring(count: Int) {
        val currencies = currencyRepo.findAll()
        val accounts = accountRepo.findAll()

        log.info("start saveAll $count via spring")

        (1..count)
            .map {  getRandomEntity(null, currencies.random(), accounts.random()) }
            .let {

                log.info("start call saveAll method $count via spring")

                repository.saveAll(it)
            }

        log.info("end saveAll $count via spring")

    }

    @Transactional
    fun saveBySpring(count: Int) {
        val currencies = currencyRepo.findAll()
        val accounts = accountRepo.findAll()

        log.info("start save $count by Spring")
        for (i in 0 until count) {
            repository.save(
                getRandomEntity(null, currencies.random(), accounts.random())
            )
        }
        log.info("end save $count by Spring")
    }

    @Transactional
    fun saveBySpringWithManualPersisting(count: Int) {
        val currencies = currencyRepo.findAll()
        val accounts = accountRepo.findAll()

        log.info("start save $count by Spring with manual persisting")

        for (i in 0 until count) {
            entityManager.persist(getRandomEntity(null, currencies.random(), accounts.random()))
            if (i != 0 && i % batchSize == 0) {
                log.info("save batch $batchSize by Spring with manual persisting")
                entityManager.flush()
                entityManager.clear()
            }
        }

        log.info("end save $count by Spring with manual persisting")

    }

    @Transactional
    fun saveByCopyViaSpring(count: Int) {
        val currencies = currencyRepo.findAll()
        val accounts = accountRepo.findAll()

        log.info("start save by copy $count via spring")

        for (i in 0 until count) {
            repository.saveByCopy(getRandomEntity(null, currencies.random(), accounts.random()))
        }

        log.info("end save by copy $count via spring")

    }

    @Transactional
    fun saveByCopyConcurrentViaSpring(count: Int) {
        val currencies = currencyRepo.findAll()
        val accounts = accountRepo.findAll()

        log.info("start save by copy concurrent $count via Spring")

        for (i in 0 until count) {
            repository.saveByCopyConcurrent(getRandomEntity(null, currencies.random(), accounts.random()))
        }

        log.info("end save by copy concurrent $count via Spring")

    }

    @Transactional
    fun saveAllByCopyViaSpring(count: Int) {
        val currencies = currencyRepo.findAll()
        val accounts = accountRepo.findAll()

        log.info("start save all by copy $count via Spring")

        (1..count)
            .map {getRandomEntity(null, currencies.random(), accounts.random())}
            .let { repository.saveAllByCopy(it) }

        log.info("end save all by copy $count via Spring")

    }

    @Transactional
    fun saveByCrudRepositorySpring(count: Int) {
        val currencies = currencyRepo.findAll()
        val accounts = accountRepo.findAll()

        log.info("start save by crud repository Spring $count")

        for (i in 0 until count) {
            pdCrudRepository.save(getRandomEntity(null, currencies.random(), accounts.random()))
        }

        log.info("end save by crud repository Spring $count")

    }

    @Transactional
    fun saveByJdbcTemplateSpring(count: Int) {
        val currencies = currencyRepo.findAll()
        val accounts = accountRepo.findAll()

        log.info("start save by jdbcTemplate $count")

        val batch = mutableListOf<Array<Any?>>()
        val data = mutableMapOf<KMutableProperty1<PaymentDocumentEntity, *>, Any?>()
        val sql = mutableMapOf<KMutableProperty1<PaymentDocumentEntity, *>, Any?>()
            .apply { fillRandomDataByKProperty(null, currencies.random(), accounts.random(), this) }
            .let { "INSERT INTO payment_document (${getColumnsString(it.keys)}) VALUES (${it.keys.joinToString { "?" }})" }

        for (i in 0 until count) {

            fillRandomDataByKProperty(null, currencies.random(), accounts.random(), data)
            batch.add(data.values.toTypedArray())
            if (i != 0 && i % batchSize == 0) {
                log.info("save batch $batchSize by jdbcTemplate")
                jdbcTemplate.batchUpdate(sql, batch)
                batch.clear()
            }

        }

        if (batch.size != 0) {
            jdbcTemplate.batchUpdate(sql, batch)
        }

        log.info("end save by jdbcTemplate $count")

    }

    @Transactional
    fun saveByNamedJdbcTemplateSpring(count: Int) {
        val currencies = currencyRepo.findAll()
        val accounts = accountRepo.findAll()

        log.info("start save by namedJdbcTemplate $count")

        val columns = PaymentDocumentEntity::class.java.declaredFields.joinToString(",") { ":${it.name}" }

        // todo разобраться почему не работает с PaymentDocumentEntity.
        val batch = mutableListOf<PaymentDocumentEntity>()

        for (i in 0 until count) {

            batch.add(getRandomEntity(null, currencies.random(), accounts.random()))
            if (i != 0 && i % batchSize == 0) {
                log.info("save batch $batchSize by namedJdbcTemplate")
                namedJdbcTemplate.batchUpdate(
                    "INSERT INTO payment_document (${getColumnsStringByClass(PaymentDocumentEntity::class)}) VALUES ($columns)",
                    SqlParameterSourceUtils.createBatch(batch)
                )
                batch.clear()
            }

        }

        if (batch.size != 0) {
            namedJdbcTemplate.batchUpdate(
                "INSERT INTO payment_document VALUES ($columns)",
                SqlParameterSourceUtils.createBatch(batch)
            )
        }

        log.info("end save by namedJdbcTemplate $count")

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
        saver.saveBatchBySession(listForSave)
    }

    fun setReadyToRead(count: Int): Int {
        log.info("get list id for set ready to read $count")
        val listId = sqlHelper.getIdListForSetReadyToRead(count, PaymentDocumentEntity::class)
        log.info("start set ready to read $count")
        return saver.setReadyToRead(listId).sumOf { it.sum() }
    }

    fun setReadyToReadUnnest(count: Int): Int {
        log.info("get list id for set ready to read with unnest $count")
        val listId = sqlHelper.getIdListForSetReadyToRead(count, PaymentDocumentEntity::class)
        log.info("start set ready to read with unnest $count")
        return saver.setReadyToReadUnnest(listId)
    }

    fun setReadyToReadByTransactionId(transactionId: UUID): Int {
        return saver.setReadyToRead(transactionId)
    }

    fun setTransactionId(count: Int, countRow: Int = 4000000): Int {
        val results = mutableListOf<Future<Array<IntArray>>>()
        for (i in 0 until count) {
            val listId = entityManager.createNativeQuery("select id from payment_document where transaction_id is null limit $countRow").resultList as List<BigInteger>

            results.add(saver.setTransactionIdAsync(listId.map { it.toLong() }))
        }
        return results.flatMap { it.get().toList() }.sumOf { it.sum() }
    }

    fun findAllByOrderNumberAndOrderDate(orderNumber: String, orderDate: LocalDate): List<PaymentDocumentEntity> {
        return repository.findAllByOrderNumberAndOrderDate(orderNumber, orderDate)
    }

    private fun getRandomEntity(
        id: Long?,
        cur: CurrencyEntity,
        account: AccountEntity,
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
        ).apply { this.id = id }
    }

    private fun fillRandomDataByKProperty(
        id: Long?,
        cur: CurrencyEntity,
        account: AccountEntity,
        data: MutableMap<KMutableProperty1<PaymentDocumentEntity, *>, Any?>
    ) {
        id?.let { data[PaymentDocumentEntity::id] = it }
        data[PaymentDocumentEntity::account] = account.id
        data[PaymentDocumentEntity::amount] = BigDecimal.valueOf(Random.nextDouble())
        data[PaymentDocumentEntity::expense] = Random.nextBoolean()
        data[PaymentDocumentEntity::cur] = cur.code
        data[PaymentDocumentEntity::orderDate] = LocalDate.now()
        data[PaymentDocumentEntity::orderNumber] = getRandomString(10)
        data[PaymentDocumentEntity::paymentPurpose] = getRandomString(100)
        data[PaymentDocumentEntity::prop10] = getRandomString(10)
        data[PaymentDocumentEntity::prop15] = getRandomString(15)
        data[PaymentDocumentEntity::prop20] = getRandomString(20)
    }
}
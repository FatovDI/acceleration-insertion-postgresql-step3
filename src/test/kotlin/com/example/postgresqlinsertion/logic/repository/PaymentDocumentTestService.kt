package com.example.postgresqlinsertion.logic.repository

import com.example.postgresqlinsertion.logic.entity.AccountEntity
import com.example.postgresqlinsertion.logic.entity.PaymentDocumentEntity
import org.springframework.context.annotation.Lazy
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Propagation
import org.springframework.transaction.annotation.Transactional
import java.time.LocalDate

@Service
class PaymentDocumentTestService(
    private val pdCustomRepository: PaymentDocumentCustomRepository,
    @Lazy
    private val selfService: PaymentDocumentTestService
) {

    @Transactional
    fun saveSeveralDataInTwoTransactionWithRequiredNew(count: Int, orderNumber: String, orderDate: LocalDate) {

        val paymentPurpose = "save entity via copy method"

        pdCustomRepository.save(
            PaymentDocumentEntity(
                paymentPurpose = paymentPurpose,
                orderNumber = orderNumber,
                orderDate = orderDate,
                prop15 = "END"
            )
        )

        selfService.saveSeveralDataInRequiredNewTransaction(count, orderNumber, orderDate)

        selfService.saveSeveralDataInRequiredNewTransaction(count, orderNumber, orderDate)
    }

    @Transactional
    fun saveSeveralDataInTwoTransactionWithRequiredNewAndException(count: Int, orderNumber: String, orderDate: LocalDate) {

        val paymentPurpose = "save entity via copy method"

        pdCustomRepository.save(
            PaymentDocumentEntity(
                paymentPurpose = paymentPurpose,
                orderNumber = orderNumber,
                orderDate = orderDate,
                prop15 = "END"
            )
        )

        selfService.saveSeveralDataInRequiredNewTransaction(count, orderNumber, orderDate)

        throw RuntimeException("Unexpected error for rollback")

    }


    @Transactional(propagation = Propagation.REQUIRES_NEW)
    fun saveSeveralDataInRequiredNewTransaction(count: Int, orderNumber: String, orderDate: LocalDate) {

        val paymentPurpose = "save entity via copy method"

        for (i in 0 until count) {
            pdCustomRepository.save(
                PaymentDocumentEntity(
                    paymentPurpose = paymentPurpose,
                    orderNumber = orderNumber,
                    orderDate = orderDate,
                    prop15 = "END"
                )
            )
        }

    }

    @Transactional
    fun saveSeveralDataInTwoTransactionWithRequiredNewAndIncorrectData(count: Int, orderNumber: String, orderDate: LocalDate) {

        val paymentPurpose = "save entity via copy method"

        pdCustomRepository.save(
            PaymentDocumentEntity(
                account = AccountEntity().apply { id = 1 },
                paymentPurpose = paymentPurpose,
                orderNumber = orderNumber,
                orderDate = orderDate,
                prop15 = "END"
            )
        )

        selfService.saveSeveralDataInRequiredNewTransaction(count, orderNumber, orderDate)

    }

    fun findAllByOrderNumberAndOrderDate(orderNumber: String, orderDate: LocalDate): List<PaymentDocumentEntity> {
        return pdCustomRepository.findAllByOrderNumberAndOrderDate(orderNumber, orderDate)
    }

}

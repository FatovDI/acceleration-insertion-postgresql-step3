package com.example.postgresqlinsertion.logic.repository

import com.example.postgresqlinsertion.logic.entity.PaymentDocumentActiveTransactionEntity
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Modifying
import org.springframework.data.jpa.repository.Query
import java.util.*
import javax.transaction.Transactional

interface PaymentDocumentActiveTransactionRepository: JpaRepository<PaymentDocumentActiveTransactionEntity, Long> {
    @Transactional
    @Modifying
    @Query("delete from PaymentDocumentActiveTransactionEntity where transactionId = :transactionId")
    fun deleteAllByTransactionId(transactionId: UUID)
}
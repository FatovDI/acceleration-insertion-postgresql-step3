package com.example.postgresqlinsertion.logic.repository

import com.example.postgresqlinsertion.batchinsertion.impl.repository.AsyncInsertRepository
import com.example.postgresqlinsertion.logic.entity.PaymentDocumentEntity
import org.springframework.stereotype.Component

@Component
class PaymentDocumentAsyncInsertRepository(
    val repo: PaymentDocumentRepository,
): PaymentDocumentRepository by repo, AsyncInsertRepository<PaymentDocumentEntity>()
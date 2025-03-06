package com.example.postgresqlinsertion.logic.repository

import org.springframework.stereotype.Component

@Component
class PaymentDocumentCustomRepository(
    val repo: PaymentDocumentRepository,
) : PaymentDocumentRepository by repo
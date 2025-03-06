package com.example.postgresqlinsertion.logic.entity

import java.util.*
import javax.persistence.*

@Entity
@Table(name = "active_transaction")
class PaymentDocumentActiveTransactionEntity(
    @Id
    var transactionId: UUID? = null,
)
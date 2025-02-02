package com.example.postgresqlinsertion.logic.entity

import java.util.*
import javax.persistence.*

@Entity
@Table(name = "active_transaction")
class PaymentDocumentActiveTransactionEntity(
    var transactionId: UUID? = null,
): BaseEntity()


//@Entity
//@Table(name = "active_transaction")
//class PaymentDocumentActiveTransactionEntity(
//    @Id
//    var id: Long? = null,
//    @OneToOne(cascade = [CascadeType.PERSIST])
//    @MapsId
//    @JoinColumn(name = "id")
//    var paymentDocument: PaymentDocumentEntity? = null,
//    var transactionId: UUID? = null,
//)

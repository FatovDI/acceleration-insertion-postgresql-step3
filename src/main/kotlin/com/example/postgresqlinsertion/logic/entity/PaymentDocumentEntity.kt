package com.example.postgresqlinsertion.logic.entity

import org.hibernate.annotations.Where
import java.math.BigDecimal
import java.time.LocalDate
import java.util.*
import javax.persistence.*

//@Entity
//@Table(name = "payment_document")
//@Where(clause = "NOT EXISTS (SELECT * FROM active_transaction at WHERE at.transaction_id = transaction_id)")
//class PaymentDocumentEntity(
//    var orderDate: LocalDate? = null,
//    var orderNumber: String? = null,
//    var amount: BigDecimal? = null,
//    @ManyToOne(fetch = FetchType.EAGER)
//    @JoinColumn(name = "cur", referencedColumnName = "code")
//    var cur: CurrencyEntity? = null,
//    var expense: Boolean? = null,
//    @ManyToOne(fetch = FetchType.EAGER)
//    @JoinColumn(name = "account_id")
//    var account: AccountEntity? = null,
//    var paymentPurpose: String? = null,
//    @Column(name = "prop_10")
//    var prop10: String? = null,
//    @Column(name = "prop_15")
//    var prop15: String? = null,
//    @Column(name = "prop_20")
//    var prop20: String? = null,
//) : BaseAsyncInsertEntity()

// ---------------- ANTI JOIN with active transaction
//@Entity
//@Table(name = "payment_document")
//@Where(clause = "NOT EXISTS (SELECT * FROM active_transaction at WHERE at.id = id)")
//class PaymentDocumentEntity(
//    var orderDate: LocalDate? = null,
//    var orderNumber: String? = null,
//    var amount: BigDecimal? = null,
//    @ManyToOne(fetch = FetchType.EAGER)
//    @JoinColumn(name = "cur", referencedColumnName = "code")
//    var cur: CurrencyEntity? = null,
//    var expense: Boolean? = null,
//    @ManyToOne(fetch = FetchType.EAGER)
//    @JoinColumn(name = "account_id")
//    var account: AccountEntity? = null,
//    var paymentPurpose: String? = null,
//    @Column(name = "prop_10")
//    var prop10: String? = null,
//    @Column(name = "prop_15")
//    var prop15: String? = null,
//    @Column(name = "prop_20")
//    var prop20: String? = null,
//    @Column(name = "statements")
//    var statements: Long? = null,
//) : BaseEntity()

// ---------------- LEFT JOIN with active transaction
@Entity
@SecondaryTable(name = "active_transaction",
    pkJoinColumns = [PrimaryKeyJoinColumn(name = "id", referencedColumnName = "id")]
)
@Table(name = "payment_document")
@Where(clause = "#{active_transaction_condition}")
class PaymentDocumentEntity(
    var orderDate: LocalDate? = null,
    var orderNumber: String? = null,
    var amount: BigDecimal? = null,
    @ManyToOne(fetch = FetchType.EAGER)
    @JoinColumn(name = "cur", referencedColumnName = "code")
    var cur: CurrencyEntity? = null,
    var expense: Boolean? = null,
    @ManyToOne(fetch = FetchType.EAGER)
    @JoinColumn(name = "account_id")
    var account: AccountEntity? = null,
    var paymentPurpose: String? = null,
    @Column(name = "prop_10")
    var prop10: String? = null,
    @Column(name = "prop_15")
    var prop15: String? = null,
    @Column(name = "prop_20")
    var prop20: String? = null,
    @Column(table = "active_transaction")
    var transactionId: UUID? = null,
) : BaseEntity()

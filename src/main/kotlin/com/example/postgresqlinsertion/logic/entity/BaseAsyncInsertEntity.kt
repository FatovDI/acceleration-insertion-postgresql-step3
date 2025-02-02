package com.example.postgresqlinsertion.logic.entity

import org.hibernate.annotations.Where
import java.util.*
import javax.persistence.*

@Entity
@Inheritance(strategy = InheritanceType.TABLE_PER_CLASS)
@Where(clause = "NOT EXISTS (SELECT * FROM active_transaction at WHERE at.transaction_id = transaction_id)")
abstract class BaseAsyncInsertEntity : BaseEntity() {

    var transactionId: UUID? = null

}

//@Entity
//@Inheritance(strategy = InheritanceType.TABLE_PER_CLASS)
//@Where(clause = "ready_to_read = true")
//abstract class BaseAsyncInsertEntity : BaseEntity() {
//
//    var transactionId: UUID? = null
//    var readyToRead: Boolean = true
//
//}
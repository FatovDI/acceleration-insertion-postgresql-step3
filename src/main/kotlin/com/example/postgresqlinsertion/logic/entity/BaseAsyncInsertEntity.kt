package com.example.postgresqlinsertion.logic.entity

import org.hibernate.annotations.Where
import java.util.*
import javax.persistence.Entity
import javax.persistence.Inheritance
import javax.persistence.InheritanceType

@Entity
@Inheritance(strategy = InheritanceType.TABLE_PER_CLASS)
@Where(clause = "transaction_id is null")
abstract class BaseAsyncInsertEntity : BaseEntity() {

    var transactionId: Short? = null

}


//    var readyToRead: Boolean = true
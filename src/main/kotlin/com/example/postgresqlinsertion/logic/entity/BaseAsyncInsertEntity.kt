package com.example.postgresqlinsertion.logic.entity

import org.hibernate.annotations.Where
import java.util.*
import javax.persistence.*

// todo uncomment
@Entity
@Inheritance(strategy = InheritanceType.TABLE_PER_CLASS)
//@Where(clause = "ready_to_read = true")
abstract class BaseAsyncInsertEntity : BaseEntity() {

    @Transient
    var transactionId: UUID? = null
    @Transient
    var readyToRead: Boolean = true

}
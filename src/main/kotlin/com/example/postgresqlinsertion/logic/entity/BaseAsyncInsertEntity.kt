package com.example.postgresqlinsertion.logic.entity

import org.hibernate.annotations.Where
import javax.persistence.Entity
import javax.persistence.Inheritance
import javax.persistence.InheritanceType

@Entity
@Inheritance(strategy = InheritanceType.TABLE_PER_CLASS)
@Where(clause = "ready_to_read = true")
abstract class BaseAsyncInsertEntity : BaseEntity() {

    var readyToRead: Boolean = true

}

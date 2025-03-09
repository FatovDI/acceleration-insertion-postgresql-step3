package com.example.postgresqlinsertion.logic.sys

import com.example.postgresqlinsertion.exception.BatchInsertionException
import com.example.postgresqlinsertion.logic.entity.BaseEntity
import com.example.postgresqlinsertion.utils.toSnakeCase
import org.springframework.stereotype.Component
import java.sql.ResultSet
import javax.persistence.Table
import javax.sql.DataSource
import kotlin.reflect.KClass
import kotlin.reflect.jvm.jvmName

@Component
class SqlHelper(
    private val dataSource: DataSource
) {

    fun getIdListForUpdate(count: Int, clazz: KClass<out BaseEntity>): List<Long> {
        return dataSource.connection.use { conn ->
            conn
                .prepareStatement("SELECT id FROM ${getTableName(clazz)} limit ?")
                .use { stmnt ->
                    stmnt.setInt(1, count)
                    stmnt.executeQuery().toList { it.getLong(1) }
                }
        }
    }

    fun getIdListForSetReadyToRead(count: Int, clazz: KClass<out BaseEntity>): List<Long> {
        return dataSource.connection.use { conn ->
            conn
                .prepareStatement("SELECT id FROM ${getTableName(clazz)} where ready_to_read = false limit ?")
                .use { stmnt ->
                    stmnt.setInt(1, count)
                    stmnt.executeQuery().toList { it.getLong(1) }
                }
        }
    }

    private fun <K, V> ResultSet.toMap(mapBlock: (rs: ResultSet) -> Pair<K, V>): Map<K, V> {
        val map = mutableMapOf<K, V>()
        while (this.next()) {
            map.plusAssign(mapBlock(this))
        }
        return map
    }

    private fun <T> ResultSet.toList(mapBlock: (rs: ResultSet) -> T): List<T> {
        val list = mutableListOf<T>()
        while (this.next()) {
            list.add(mapBlock(this))
        }
        return list
    }

    fun getTableName(clazz: KClass<*>): String {
        val columnAnnotation = clazz.annotations.find { it.annotationClass == Table::class } as Table?
        return columnAnnotation?.name
            ?: clazz.simpleName?.toSnakeCase()
            ?: throw BatchInsertionException(
                "Can not define table name by class: ${clazz.jvmName}"
            )
    }

}
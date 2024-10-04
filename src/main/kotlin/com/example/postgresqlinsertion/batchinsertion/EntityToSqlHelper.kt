package com.example.postgresqlinsertion.batchinsertion

import com.example.postgresqlinsertion.batchinsertion.exception.BatchInsertionException
import com.example.postgresqlinsertion.batchinsertion.utils.toSnakeCase
import com.example.postgresqlinsertion.logic.entity.BaseEntity
import java.lang.reflect.Field
import javax.persistence.Column
import javax.persistence.JoinColumn
import javax.persistence.Table
import kotlin.reflect.KClass
import kotlin.reflect.KProperty1
import kotlin.reflect.jvm.javaField
import kotlin.reflect.jvm.jvmName

fun getStringDataFromEntity(entity: BaseEntity) =
    getDataFromEntity(entity).map { it?.toString() }

fun getDataFromEntity(entity: BaseEntity) =
    getAllFields(entity.javaClass).map { field ->
        field.trySetAccessible()
        getDataFromEntityByField(entity, field)
    }

fun getAllFields(clazz: Class<*>?): List<Field> {
    if (clazz == null) {
        return emptyList()
    }
    return mutableListOf<Field>().apply {
        addAll(getAllFields(clazz.superclass))
        addAll(clazz.declaredFields.filter { it.name != "id" })
    }
}

fun getDataFromEntityByField(entity: BaseEntity, field: Field) =
    when (val obj = field.get(entity)) {
        null -> null
        is BaseEntity -> {
            field.annotations
                ?.filterIsInstance(JoinColumn::class.java)
                ?.firstOrNull()
                ?.referencedColumnName
                ?.takeIf { it.isNotEmpty() }
                ?.let { obj.javaClass.getDeclaredField(it) }
                ?.apply { trySetAccessible() }
                ?.get(obj)
                ?: obj.id
        }
        else -> obj
    }

fun getTableName(clazz: KClass<*>): String {
    val columnAnnotation = clazz.annotations.find { it.annotationClass == Table::class } as Table?
    return columnAnnotation?.name
        ?: clazz.simpleName?.toSnakeCase()
        ?: throw BatchInsertionException(
            "Can not define table name by class: ${clazz.jvmName}"
        )
}

fun getColumnsStringByClass(clazz: KClass<out BaseEntity>) =
    getAllFields(clazz.java).joinToString(",") { getColumnName(it) }

fun getColumnsByClass(clazz: KClass<out BaseEntity>) =
    getAllFields(clazz.java).map { getColumnName(it) }

fun getColumnsString(columns: Set<KProperty1<*, *>>) =
    columns.joinToString(",") { getColumnName(it.javaField) }

fun getColumnName(field: Field?) =
    field?.annotations.let { annotations ->
        annotations
            ?.find { it.annotationClass == Column::class }
            ?.let { it as Column }
            ?.name
            ?.takeIf { it.isNotEmpty() }
            ?: annotations
                ?.find { it.annotationClass == JoinColumn::class }
                ?.let { it as JoinColumn }?.name
    }
        ?: field?.name?.toSnakeCase()
        ?: throw BatchInsertionException("Can not define column name by field")


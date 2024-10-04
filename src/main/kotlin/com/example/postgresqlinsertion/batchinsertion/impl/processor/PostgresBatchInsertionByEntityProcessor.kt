package com.example.postgresqlinsertion.batchinsertion.impl.processor

import com.example.postgresqlinsertion.batchinsertion.*
import com.example.postgresqlinsertion.batchinsertion.api.processor.BatchInsertionByEntityProcessor
import com.example.postgresqlinsertion.batchinsertion.api.processor.DataForUpdate
import com.example.postgresqlinsertion.logic.entity.BaseEntity
import org.springframework.stereotype.Component
import java.io.BufferedWriter
import java.io.DataOutputStream
import java.io.Reader
import java.sql.Connection
import kotlin.reflect.KClass

@Component
class PostgresBatchInsertionByEntityProcessor(
): AbstractBatchInsertionProcessor(), BatchInsertionByEntityProcessor {

    private val delimiter = "|"
    private val nullValue = "NULL"

    override fun addDataForCreate(data: BaseEntity, writer: BufferedWriter) {
        val values = getStringDataFromEntity(data)
        writer.write(getStringForWrite(values, delimiter, nullValue))
        writer.newLine()
    }

    override fun addDataForCreateWithBinary(data: BaseEntity, outputStream: DataOutputStream) {
        val fields = getAllFields(data.javaClass)
        outputStream.writeShort(fields.size)
        fields.map { field ->
            field.trySetAccessible()

            writeBinaryDataForCopyMethod(getDataFromEntityByField(data, field), outputStream)

        }
    }

    override fun getStringForUpdate(data: BaseEntity) =
        getStringForInsert(data).let { "($it) where id = '${data.id}'" }

    override fun getStringForInsert(data: BaseEntity) =
        getStringForInsert(getStringDataFromEntity(data), nullValue)

    override fun saveToDataBaseByCopyMethod(clazz: KClass<out BaseEntity>, from: Reader, conn: Connection) {
        saveToDataBaseByCopyMethod(clazz, from, delimiter, nullValue, conn)
    }

    /**
     * save list data with insert method and prepared statement
     * @param clazz - entity class
     * @param data - list of string by columns
     * @param conn - DB connection
     */
    override fun insertDataToDataBasePreparedStatement(
        clazz: KClass<out BaseEntity>,
        data: List<List<Any?>>,
        conn: Connection
    ) {
        insertDataToDataBasePreparedStatement(getTableName(clazz), getColumnsByClass(clazz), data, conn)
    }


    override fun insertDataToDataBasePreparedStatementAndUnnest(
        clazz: KClass<out BaseEntity>,
        data: List<List<Any?>>,
        pgTypes: List<String>,
        conn: Connection
    ) {
        insertDataToDataBasePreparedStatementAndUnnest(getTableName(clazz), getColumnsByClass(clazz), data, pgTypes, conn)
    }

    /**
     * get pg types by columns
     * @param clazz - entity class
     * @param conn - DB connection
     * @return List<String> - list with pg type name
     */
    override fun getPgTypes(clazz: KClass<out BaseEntity>, conn: Connection): List<String> {
        return getPgTypes(getTableName(clazz), getColumnsByClass(clazz), conn)
    }

    override fun updateDataToDataBasePreparedStatement(
        clazz: KClass<out BaseEntity>,
        data: List<List<Any?>>,
        conditionParams: List<String>,
        conn: Connection
    ) {
        updateDataToDataBasePreparedStatement(
            DataForUpdate(getTableName(clazz), getColumnsByClass(clazz), conditionParams, data),
            conn
        )
    }

}

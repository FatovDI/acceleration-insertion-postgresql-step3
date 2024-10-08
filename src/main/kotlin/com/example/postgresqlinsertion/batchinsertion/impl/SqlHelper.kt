package com.example.postgresqlinsertion.batchinsertion.impl

import com.example.postgresqlinsertion.batchinsertion.api.SqlHelper
import com.example.postgresqlinsertion.batchinsertion.getTableName
import com.example.postgresqlinsertion.logic.entity.BaseEntity
import org.springframework.stereotype.Component
import java.sql.ResultSet
import javax.sql.DataSource
import kotlin.reflect.KClass

@Component
class SqlHelper(
    private val dataSource: DataSource
): SqlHelper {

    override fun nextTransactionId(): Short {
        return dataSource.connection.use { conn ->
            conn
                .prepareStatement("SELECT nextval('seq_trans_id')")
                .use { stmnt ->
                    stmnt.executeQuery().toList { it.getShort(1) }.first()
                }
        }
    }

    override fun nextIdList(count: Int): List<Long> {
        return dataSource.connection.use { conn ->
            conn
                .prepareStatement("SELECT nextval('seq_id') FROM generate_series(1, ?)")
                .use { stmnt ->
                    stmnt.setInt(1, count)
                    stmnt.executeQuery().toList { it.getLong(1) }
                }
        }
    }

    override fun getIdListForUpdate(count: Int, clazz: KClass<out BaseEntity>): List<Long> {
        return dataSource.connection.use { conn ->
            conn
                .prepareStatement("SELECT id FROM ${getTableName(clazz)} limit ?")
                .use { stmnt ->
//                    stmnt.setString(1, tableName)
                    stmnt.setInt(1, count)
                    stmnt.executeQuery().toList { it.getLong(1) }
                }
        }
    }

    override fun getIdListForSetReadyToRead(count: Int, clazz: KClass<out BaseEntity>): List<Long> {
        return dataSource.connection.use { conn ->
            conn
                .prepareStatement("SELECT id FROM ${getTableName(clazz)} where ready_to_read = false limit ?")
                .use { stmnt ->
//                    stmnt.setString(1, tableName)
                    stmnt.setInt(1, count)
                    stmnt.executeQuery().toList { it.getLong(1) }
                }
        }
    }

    override fun dropIndex(clazz: KClass<out BaseEntity>): String {

        val scriptForCreate: String

        dataSource.connection.use { conn ->
            val indexMap =
                conn
                    .prepareStatement(
                        """
                        SELECT 
                            indexname, 
                            indexdef 
                        FROM pg_indexes 
                        WHERE schemaname = ? AND tablename = ? AND indexdef LIKE 'CREATE INDEX%'
                        """
                    )
                    .use { stmnt ->
                        stmnt.setString(1, conn.schema)
                        stmnt.setString(2, getTableName(clazz))
                        stmnt.executeQuery().toMap { it.getString(1) to it.getString(2) }
                    }
            conn.createStatement().use { stmnt ->
                stmnt.executeLargeUpdate(
                    indexMap.keys.joinToString("\n") { s ->
                        "DROP INDEX $s;"
                    }
                )
            }
            scriptForCreate = indexMap.values.joinToString("\n") { s -> "$s;" }
        }
        return scriptForCreate
    }


    override fun executeScript(script: String) {
        script.takeIf { it.isNotEmpty() }?.let {
            dataSource.connection.use { conn ->
                conn.createStatement().use { stmnt ->
                    stmnt.execute(it)
                }
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
}
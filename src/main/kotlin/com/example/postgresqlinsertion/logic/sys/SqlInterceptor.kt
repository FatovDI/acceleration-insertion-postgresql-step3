package com.example.postgresqlinsertion.logic.sys

import org.hibernate.resource.jdbc.spi.StatementInspector

class SqlInterceptor: StatementInspector {

    override fun inspect(sql: String?): String? {

        return sql
            ?.takeIf { it.contains("#{active_transaction_condition}") }
            ?.let { s ->
                val joinName = "join active_transaction\\s+(\\w+)".toRegex().findAll(s)
                var finallySql = s
                joinName.forEach {
                    finallySql = s.replaceFirst(
                        "#{active_transaction_condition}",
                        "${it.groupValues[1]}.transaction_id is null"
                    )
                }
                finallySql
            } ?: sql
    }
}
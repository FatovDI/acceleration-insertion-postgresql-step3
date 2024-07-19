package com.example.postgresqlinsertion.logic.controller

import com.example.postgresqlinsertion.logic.dto.ResponseDto
import com.example.postgresqlinsertion.logic.service.PaymentDocumentService
import org.springframework.web.bind.annotation.*
import kotlin.system.measureTimeMillis

@RestController
@RequestMapping("/test-insertion")
class PaymentDocumentInsertionController(
    val service: PaymentDocumentService
) {

    @PostMapping("/insert-by-spring-with-async/{count}")
    fun insertBySpringWithAsync(@PathVariable count: Int): ResponseDto {
        val time = measureTimeMillis {
            service.saveBySpringConcurrent(count)
        }
        return ResponseDto(
            name = "Save by spring with async",
            count = count,
            time = getTimeString(time)
        )
    }

    @PostMapping("/insert-with-async-repository/{count}")
    fun insertByConcurrentBatch(@PathVariable count: Int): ResponseDto {
        val time = measureTimeMillis {
            service.saveByConcurrentBatch(count)
        }
        return ResponseDto(
            name = "Save by spring with async repository",
            count = count,
            time = getTimeString(time)
        )
    }

    @PostMapping("/copy/{count}")
    fun insertViaCopy(@PathVariable count: Int): ResponseDto {
        val time = measureTimeMillis {
            service.saveByCopy(count)
        }
        return ResponseDto(
            name = "Copy method",
            count = count,
            time = getTimeString(time)
        )
    }

    @PostMapping("/copy-by-binary/{count}")
    fun insertViaCopyByBinary(@PathVariable count: Int): ResponseDto {
        val time = measureTimeMillis {
            service.saveByCopyBinary(count)
        }
        return ResponseDto(
            name = "Copy method by binary",
            count = count,
            time = getTimeString(time)
        )
    }

    @PostMapping("/copy-by-property/{count}")
    fun insertViaCopyAndProperty(@PathVariable count: Int): ResponseDto {
        val time = measureTimeMillis {
            service.saveByCopyAndKProperty(count)
        }
        return ResponseDto(
            name = "Copy method by property",
            count = count,
            time = getTimeString(time)
        )
    }

    @PostMapping("/copy-by-binary-and-property/{count}")
    fun insertViaCopyByBinaryAndProperty(@PathVariable count: Int): ResponseDto {
        val time = measureTimeMillis {
            service.saveByCopyBinaryAndKProperty(count)
        }
        return ResponseDto(
            name = "Copy method by binary and property",
            count = count,
            time = getTimeString(time)
        )
    }

    @PostMapping("/copy-by-file/{count}")
    fun insertViaCopyByFile(@PathVariable count: Int): ResponseDto {
        val time = measureTimeMillis {
            service.saveByCopyViaFile(count)
        }
        return ResponseDto(
            name = "Copy method via file",
            count = count,
            time = getTimeString(time)
        )
    }

    @PostMapping("/copy-by-binary-file/{count}")
    fun insertViaCopyByBinaryFile(@PathVariable count: Int): ResponseDto {
        val time = measureTimeMillis {
            service.saveByCopyViaBinaryFile(count)
        }
        return ResponseDto(
            name = "Copy method via binary file",
            count = count,
            time = getTimeString(time)
        )
    }

    @PostMapping("/copy-by-file-and-property/{count}")
    fun insertViaCopyByFileAndProperty(@PathVariable count: Int): ResponseDto {
        val time = measureTimeMillis {
            service.saveByCopyAnpPropertyViaFile(count)
        }
        return ResponseDto(
            name = "Copy method via file and property",
            count = count,
            time = getTimeString(time)
        )
    }


    @PostMapping("/copy-by-binary-file-and-property/{count}")
    fun insertViaCopyByBinaryFileAndProperty(@PathVariable count: Int): ResponseDto {
        val time = measureTimeMillis {
            service.saveByCopyAnpPropertyViaBinaryFile(count)
        }
        return ResponseDto(
            name = "Copy method via binary file and property",
            count = count,
            time = getTimeString(time)
        )
    }

    @PostMapping("/insert/{count}")
    fun insertViaInsert(@PathVariable count: Int): ResponseDto {
        val time = measureTimeMillis {
            service.saveByInsert(count)
        }
        return ResponseDto(
            name = "Insert method",
            count = count,
            time = getTimeString(time)
        )
    }

    @PostMapping("/insert-prepared-statement/{count}")
    fun insertViaInsertWithPreparedStatement(
        @PathVariable count: Int,
        @RequestParam orderNumber: String? = null
    ): ResponseDto {
        val time = measureTimeMillis {
            service.saveByInsertWithPreparedStatement(count, orderNumber)
        }
        return ResponseDto(
            name = "Insert method with prepared statement",
            count = count,
            time = getTimeString(time)
        )
    }

    @PostMapping("/insert-prepared-statement-unnest/{count}")
    fun insertViaInsertWithPreparedStatementUnnest(
        @PathVariable count: Int,
        @RequestParam orderNumber: String? = null
    ): ResponseDto {
        val time = measureTimeMillis {
            service.saveByInsertWithPreparedStatementAndUnnest(count, orderNumber)
        }
        return ResponseDto(
            name = "Insert method with prepared statement and unnest",
            count = count,
            time = getTimeString(time)
        )
    }

    @PostMapping("/update/{count}")
    fun update(@PathVariable count: Int): ResponseDto {
        val time = measureTimeMillis {
            service.update(count)
        }
        return ResponseDto(
            name = "Update method",
            count = count,
            time = getTimeString(time)
        )
    }

    @PostMapping("/update-prepared-statement/{count}")
    fun updatePreparedStatement(@PathVariable count: Int): ResponseDto {
        val time = measureTimeMillis {
            service.updatePreparedStatement(count)
        }
        return ResponseDto(
            name = "Update method prepared statement",
            count = count,
            time = getTimeString(time)
        )
    }

    @PostMapping("/insert-by-property/{count}")
    fun insertViaInsertAndProperty(@PathVariable count: Int): ResponseDto {
        val time = measureTimeMillis {
            service.saveByInsertAndProperty(count)
        }
        return ResponseDto(
            name = "Insert method by property",
            count = count,
            time = getTimeString(time)
        )
    }

    @PostMapping("/update-by-property/{count}")
    fun updateViaInsertAndProperty(@PathVariable count: Int): ResponseDto {
        val time = measureTimeMillis {
            service.updateByProperty(count)
        }
        return ResponseDto(
            name = "Update method by property",
            count = count,
            time = getTimeString(time)
        )
    }

    @PostMapping("/update-by-property-prepared-statement/{count}")
    fun updateViaInsertAndPropertyPreparedStatement(@PathVariable count: Int): ResponseDto {
        val time = measureTimeMillis {
            service.updateByPropertyPreparedStatement(count)
        }
        return ResponseDto(
            name = "Update method by property prepared statement",
            count = count,
            time = getTimeString(time)
        )
    }

    @PostMapping("/update-only-one-field-by-property/{count}")
    fun updateOnlyOneFieldViaProperty(@PathVariable count: Int): ResponseDto {
        val time = measureTimeMillis {
            service.updateOnlyOneFieldByProperty(count)
        }
        return ResponseDto(
            name = "Update only one field by property with transaction",
            count = count,
            time = getTimeString(time)
        )
    }

    @PostMapping("/update-only-one-field-by-property-prepared-statement/{count}")
    fun updateOnlyOneFieldViaPropertyPreparedStatement(@PathVariable count: Int): ResponseDto {
        val time = measureTimeMillis {
            service.updateOnlyOneFieldByPropertyPreparedStatement(count)
        }
        return ResponseDto(
            name = "Update only one field by property with transaction prepared statement",
            count = count,
            time = getTimeString(time)
        )
    }

    @PostMapping("/update-only-one-field-with-common-condition/{orderNumber}")
    fun updateOnlyOneFieldWithCommonCondition(
        @PathVariable orderNumber: String,
        @RequestParam prop10: String
    ): ResponseDto {
        var count: Int
        val time = measureTimeMillis {
            count = service.updateOnlyOneFieldWithCommonCondition(orderNumber, prop10)
        }
        return ResponseDto(
            name = "Update only one field with common condition",
            count = count,
            time = getTimeString(time)
        )
    }

    @PostMapping("/insert-with-drop-index/{count}")
    fun insertViaInsertWithDropIndex(@PathVariable count: Int): ResponseDto {
        val time = measureTimeMillis {
            service.saveByInsertWithDropIndex(count)
        }
        return ResponseDto(
            name = "Insert method with drop index",
            count = count,
            time = getTimeString(time)
        )
    }

    @PostMapping("/spring/{count}")
    fun insertViaSpring(@PathVariable count: Int): ResponseDto {
        val time = measureTimeMillis {
            service.saveBySpring(count)
        }
        return ResponseDto(
            name = "Save by Spring",
            count = count,
            time = getTimeString(time)
        )
    }

    @PostMapping("/spring-save-all/{count}")
    fun insertViaSaveAllSpring(@PathVariable count: Int): ResponseDto {
        val time = measureTimeMillis {
            service.saveAllBySpring(count)
        }
        return ResponseDto(
            name = "Save all via Spring",
            count = count,
            time = getTimeString(time)
        )
    }

    @PostMapping("/spring-with-manual-persisting/{count}")
    fun insertViaSpringWithManualPersisting(@PathVariable count: Int): ResponseDto {
        val time = measureTimeMillis {
            service.saveBySpringWithManualPersisting(count)
        }
        return ResponseDto(
            name = "Save by Spring with manual batching",
            count = count,
            time = getTimeString(time)
        )
    }

    @PostMapping("/spring-with-copy/{count}")
    fun insertViaSpringWithCopy(@PathVariable count: Int): ResponseDto {
        val time = measureTimeMillis {
            service.saveByCopyViaSpring(count)
        }
        return ResponseDto(
            name = "Save by Spring with copy method",
            count = count,
            time = getTimeString(time)
        )
    }

    @PostMapping("/spring-with-copy-concurrent/{count}")
    fun insertViaSpringWithCopyConcurrent(@PathVariable count: Int): ResponseDto {
        val time = measureTimeMillis {
            service.saveByCopyConcurrentViaSpring(count)
        }
        return ResponseDto(
            name = "Save by Spring with copy concurrent method",
            count = count,
            time = getTimeString(time)
        )
    }

    @PostMapping("/spring-save-all-with-copy/{count}")
    fun insertViaSpringSaveAllWithCopy(@PathVariable count: Int): ResponseDto {
        val time = measureTimeMillis {
            service.saveAllByCopyViaSpring(count)
        }
        return ResponseDto(
            name = "Save by Spring with save all and copy method",
            count = count,
            time = getTimeString(time)
        )
    }

    @PostMapping("/spring-by-crud-repository/{count}")
    fun insertViaSpringSaveByCrudRepository(@PathVariable count: Int): ResponseDto {
        val time = measureTimeMillis {
            service.saveByCrudRepositorySpring(count)
        }
        return ResponseDto(
            name = "Save by crud repository Spring",
            count = count,
            time = getTimeString(time)
        )
    }

    @PostMapping("/spring-jdbc-template/{count}")
    fun insertViaJdbcTemplate(@PathVariable count: Int): ResponseDto {
        val time = measureTimeMillis {
            service.saveByJdbcTemplateSpring(count)
        }
        return ResponseDto(
            name = "Save by jdbc template",
            count = count,
            time = getTimeString(time)
        )
    }

    @PostMapping("/spring-named-jdbc-template/{count}")
    fun insertViaNamedJdbcTemplate(@PathVariable count: Int): ResponseDto {
        val time = measureTimeMillis {
            service.saveByNamedJdbcTemplateSpring(count)
        }
        return ResponseDto(
            name = "Save by named jdbc template",
            count = count,
            time = getTimeString(time)
        )
    }

    @PostMapping("/spring-update/{count}")
    fun updateViaSpring(@PathVariable count: Int): ResponseDto {
        val time = measureTimeMillis {
            service.updateBySpring(count)
        }
        return ResponseDto(
            name = "Update via spring",
            count = count,
            time = getTimeString(time)
        )
    }

    private fun getTimeString(time: Long):String {
        val min = (time / 1000) / 60
        val sec = (time / 1000) % 60
        val ms = time - min*1000*60 - sec*1000
        return "$min min, $sec sec $ms ms"
    }
}
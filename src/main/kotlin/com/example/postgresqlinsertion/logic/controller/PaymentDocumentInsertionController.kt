package com.example.postgresqlinsertion.logic.controller

import com.example.postgresqlinsertion.logic.dto.ResponseDto
import com.example.postgresqlinsertion.logic.service.PaymentDocumentService
import com.example.postgresqlinsertion.logic.sys.SqlHelper
import com.fasterxml.uuid.Generators
import org.springframework.web.bind.annotation.*
import java.util.*
import kotlin.system.measureTimeMillis

@RestController
@RequestMapping("/test-insertion")
class PaymentDocumentInsertionController(
    val service: PaymentDocumentService,
) {

    @PostMapping("/insert-by-spring-with-async/{count}")
    fun insertBySpringWithAsync(
        @PathVariable count: Int,
        @RequestParam transactionId: UUID? = null
    ): ResponseDto {
        val time = measureTimeMillis {
            service.saveBySpringConcurrentWithTransactionId(count, transactionId)
        }
        return ResponseDto(
            name = "Save by spring with async",
            count = count,
            time = getTimeString(time)
        )
    }


    @PostMapping("/save-by-spring-with-async/{count}")
    fun saveBySpringWithAsync(@PathVariable count: Int): ResponseDto {
        val time = measureTimeMillis {
            service.saveBySpringConcurrent(count)
        }
        return ResponseDto(
            name = "Save by spring with async",
            count = count,
            time = getTimeString(time)
        )
    }


    @PostMapping("/insert-by-spring-with-async-and-session/{count}")
    fun insertBySpringWithAsyncAndSession(@PathVariable count: Int): ResponseDto {
        val time = measureTimeMillis {
            service.saveBySpringConcurrentWithSession(count)
        }
        return ResponseDto(
            name = "Save by spring with async and session",
            count = count,
            time = getTimeString(time)
        )
    }

    @PostMapping("/set-ready-to-read/{count}")
    fun setReadyToRead(@PathVariable count: Int): ResponseDto {
        var countUpd: Int
        val time = measureTimeMillis {
            countUpd = service.setReadyToRead(count)
        }
        return ResponseDto(
            name = "Set ready to read jdbcTemplate",
            count = countUpd,
            time = getTimeString(time)
        )
    }

    @PostMapping("/set-ready-to-read-unnest/{count}")
    fun setReadyToReadUnnest(@PathVariable count: Int): ResponseDto {
        var countUpd: Int
        val time = measureTimeMillis {
            countUpd = service.setReadyToReadUnnest(count)
        }
        return ResponseDto(
            name = "Set ready to read unnest",
            count = countUpd,
            time = getTimeString(time)
        )
    }

    @PostMapping("/set-ready-to-read-array/{count}")
    fun setReadyToReadArray(@PathVariable count: Int): ResponseDto {
        var countUpd: Int
        val time = measureTimeMillis {
            countUpd = service.setReadyToReadArray(count)
        }
        return ResponseDto(
            name = "Set ready to read array",
            count = countUpd,
            time = getTimeString(time)
        )
    }

    @PostMapping("/set-transaction-id/{count}")
    fun setTransactionId(
        @PathVariable count: Int,
        @RequestParam countRow: Int = 4000000
    ): ResponseDto {
        var countUpd: Int
        val time = measureTimeMillis {
            countUpd = service.setTransactionId(count, countRow)
        }
        return ResponseDto(
            name = "Set transaction ID",
            count = countUpd,
            time = getTimeString(time)
        )
    }

    @PostMapping("/set-ready-to-read-by-transaction-id/{transactionId}")
    fun setReadyToReadByTransactionId(@PathVariable transactionId: UUID): ResponseDto {
        var countUpd: Int
        val time = measureTimeMillis {
            countUpd = service.setReadyToReadByTransactionId(transactionId)
        }
        return ResponseDto(
            name = "Set ready to read by transaction ID",
            count = countUpd,
            time = getTimeString(time)
        )
    }

    @PostMapping("/set-ready-to-read-after-insert/{count}")
    fun setReadyToReadAfterInsert(@PathVariable count: Int): ResponseDto {
        var countUpd: Int
//        val transactionId = getRandomString(10)
//        val transactionId = sqlHelper.nextIdList(1).first()
//        val transactionId = sqlHelper.nextTransactionId()
        val transactionId = UUID.randomUUID()
        service.saveBySpringConcurrentWithTransactionId(count, transactionId)
        val time = measureTimeMillis {
            countUpd = service.setReadyToReadByTransactionId(transactionId)
        }
        return ResponseDto(
            name = "Set ready to read after insert",
            count = countUpd,
            time = getTimeString(time)
        )
    }

    @PostMapping("/remove-transaction-id-after-insert/{count}")
    fun sremoveTransactionIdAfterInsert(@PathVariable count: Int): ResponseDto {
        var countUpd: Int
        val transactionId = Generators.timeBasedEpochGenerator().generate()
        service.saveBySpringConcurrentWithTransactionId(count, transactionId)
        val time = measureTimeMillis {
            countUpd = service.removeTransactionId(transactionId)
        }
        return ResponseDto(
            name = "Remove transaction id after insert",
            count = countUpd,
            time = getTimeString(time)
        )
    }

    @PostMapping("/set-ready-to-read-array-after-insert/{count}")
    fun setReadyToReadArrayAfterInsert(@PathVariable count: Int): ResponseDto {
        var countUpd: Int
        val ids = service.saveBySpringConcurrent(count)
        val time = measureTimeMillis {
            countUpd = service.setReadyToReadArray(count, ids)
        }
        return ResponseDto(
            name = "Set ready to read array after insert",
            count = countUpd,
            time = getTimeString(time)
        )
    }

    @PostMapping("/save-concurrent-and-atomic/{count}")
    fun saveBySpringConcurrentAndAtomic(@PathVariable count: Int): ResponseDto {
        var countUpd: Int
        val time = measureTimeMillis {
            countUpd = service.saveBySpringConcurrentAtomic(count)
        }
        return ResponseDto(
            name = "save concurrent and atomic",
            count = countUpd,
            time = getTimeString(time)
        )
    }

    @PostMapping("/save-concurrent-and-atomic-by-transaction-id/{count}")
    fun saveBySpringConcurrentAndAtomicByTransactionId(@PathVariable count: Int): ResponseDto {
        var countUpd: Int
        val time = measureTimeMillis {
            countUpd = service.saveBySpringConcurrentWithTransactionIdAtomic(count)
        }
        return ResponseDto(
            name = "save concurrent and atomic by tr. ID",
            count = countUpd,
            time = getTimeString(time)
        )
    }

    @PostMapping("/save-concurrent-and-atomic-by-transaction-id-another-table/{count}")
    fun saveBySpringConcurrentAndAtomicByTransactionIdAnotherTable(@PathVariable count: Int): ResponseDto {
        var countUpd: Int
        val time = measureTimeMillis {
            countUpd = service.saveByConcurrentAtomicWithTransactionIdInAnotherTable(count)
        }
        return ResponseDto(
            name = "save concurrent and atomic by tr. ID in another table",
            count = countUpd,
            time = getTimeString(time)
        )
    }

    @PostMapping("/save-concurrent-and-atomic-by-transaction-id-two-table/{count}")
    fun saveBySpringConcurrentAndAtomicByTransactionIdTwoTable(@PathVariable count: Int): ResponseDto {
        var countUpd: Int
        val time = measureTimeMillis {
            countUpd = service.saveByConcurrentAtomicWithTransactionIdInTwoTable(count)
        }
        return ResponseDto(
            name = "save concurrent and atomic by tr. ID in two table",
            count = countUpd,
            time = getTimeString(time)
        )
    }

    @PostMapping("/spring/{count}")
    fun insertViaSpring(
        @PathVariable count: Int,
        @RequestParam transactionId: UUID? = null
    ): ResponseDto {
        val time = measureTimeMillis {
            service.saveBySpring(count, transactionId)
        }
        return ResponseDto(
            name = "Save by Spring",
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

    @PostMapping("/spring-update-by-session/{count}")
    fun updateViaSpringBySession(@PathVariable count: Int): ResponseDto {
        val time = measureTimeMillis {
            service.updateBySpringWithSession(count)
        }
        return ResponseDto(
            name = "Update via spring by session",
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
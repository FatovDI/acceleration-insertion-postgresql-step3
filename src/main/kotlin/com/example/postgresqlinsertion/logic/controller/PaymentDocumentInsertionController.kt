package com.example.postgresqlinsertion.logic.controller

import com.example.postgresqlinsertion.logic.dto.ResponseDto
import com.example.postgresqlinsertion.logic.service.PaymentDocumentService
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import kotlin.system.measureTimeMillis

@RestController
@RequestMapping("/test-insertion")
class PaymentDocumentInsertionController(
    val service: PaymentDocumentService,
) {

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
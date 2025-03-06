package com.example.postgresqlinsertion.logic.repository

import org.assertj.core.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.TestPropertySource
import org.springframework.test.context.junit4.SpringRunner
import java.time.LocalDate

@RunWith(SpringRunner::class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureMockMvc
@TestPropertySource(locations = ["classpath:application-test.properties"])
internal class PaymentDocumentCustomRepositoryTest {

    @Autowired
    lateinit var service: PaymentDocumentTestService

    @Test
    fun `save several payment document in two transaction with required new rollback test`() {
        val orderNumber = "CR_8"
        val orderDate = LocalDate.now()
        val count = 10

        assertThrows<RuntimeException> {
            service.saveSeveralDataInTwoTransactionWithRequiredNewAndException(count, orderNumber, orderDate)
        }
        val savedPd = service.findAllByOrderNumberAndOrderDate(orderNumber, orderDate)
        Assertions.assertThat(savedPd.size).isEqualTo(count)
    }

    @Test
    fun `save several payment document in two transaction with required new and incorrect data test`() {
        val orderNumber = "CR_9"
        val orderDate = LocalDate.now()
        val count = 10

        assertThrows<RuntimeException> {
            service.saveSeveralDataInTwoTransactionWithRequiredNewAndIncorrectData(count, orderNumber, orderDate)
        }
        val savedPd = service.findAllByOrderNumberAndOrderDate(orderNumber, orderDate)
        Assertions.assertThat(savedPd.size).isEqualTo(count)
    }

    @Test
    fun `save several payment document in required new transaction test`() {
        val orderNumber = "CR_11"
        val orderDate = LocalDate.now()
        val count = 10

        service.saveSeveralDataInTwoTransactionWithRequiredNew(count, orderNumber, orderDate)

        val savedPd = service.findAllByOrderNumberAndOrderDate(orderNumber, orderDate)
        Assertions.assertThat(savedPd.size).isEqualTo(count*2+1)
    }

}


package org.http4k.websocket

import com.fasterxml.jackson.databind.JsonNode
import com.natpryce.hamkrest.assertion.assertThat
import com.natpryce.hamkrest.equalTo
import com.natpryce.hamkrest.present
import com.natpryce.hamkrest.throws
import graphql.ExecutionResult
import graphql.GraphQLError
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.reactive.asPublisher
import org.http4k.core.ContentType
import org.http4k.core.Method
import org.http4k.core.Request
import org.http4k.core.Response
import org.http4k.core.Status
import org.http4k.core.with
import org.http4k.format.Jackson
import org.http4k.lens.Header
import org.http4k.routing.websockets
import org.http4k.testing.Approver
import org.http4k.testing.ClosedWebsocket
import org.http4k.testing.JsonApprovalTest
import org.http4k.testing.TestWsClient
import org.http4k.testing.testWsClient
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.extension.ExtendWith
import java.time.Duration
import java.util.concurrent.CompletableFuture.completedFuture
import java.util.concurrent.TimeUnit

@Timeout(5, unit = TimeUnit.SECONDS)
@ExtendWith(JsonApprovalTest::class)
class GraphQLWsConsumerTest {

    @Test
    fun `on connection_init a connection_ack message is sent with payload from onConnectionInit`(approver: Approver) =
        GraphQLWsConsumer(emptyResult, onConnectionInit = { ValidationResult.valid(it) }).withTestClient {
            sendConnectionInit(payload = { obj("some" to string("value")) })

            approver.assertApproved(receivedMessages().toList())
        }

    @Test
    fun `on connection_init the socket is closed when the validation fails`() =
        GraphQLWsConsumer(emptyResult, onConnectionInit = { ValidationResult.invalid() }).withTestClient {
            sendConnectionInit(payload = { obj("some" to string("value")) })

            assertThat({ receivedMessages().toList() },
                throws(equalTo(ClosedWebsocket(WsStatus(4403, "Forbidden")))))
        }

    @Test
    fun `when connection_init is not sent withing timeout the socket is closed`() =
        GraphQLWsConsumer(emptyResult, connectionInitWaitTimeout = Duration.ofMillis(1)).withTestClient {
            assertThat({ receivedMessages().toList() },
                throws(equalTo(ClosedWebsocket(WsStatus(4408, "Connection initialisation timeout")))))
        }

    @Test
    fun `on multiple connection_init the socket is closed`() =
        GraphQLWsConsumer(emptyResult).withTestClient {
            sendConnectionInit()
            sendConnectionInit()

            assertThat({ receivedMessages().toList() },
                throws(equalTo(ClosedWebsocket(WsStatus(4429, "Too many initialisation requests")))))
        }

    @Test
    fun `on ping a pong message is sent with payload from onPing`(approver: Approver) =
        GraphQLWsConsumer(emptyResult, onPing = { it }).withTestClient {
            sendConnectionInit()
            send { obj("type" to string("ping"), "payload" to obj("some" to string("value"))) }

            approver.assertApproved(receivedMessages().toList())
        }

    @Test
    fun `on pong no message is sent and onPong is invoked`(approver: Approver) {
        val onPongPayload = mutableMapOf<String, Any>()
        GraphQLWsConsumer(emptyResult, onPong = { onPongPayload.putAll(it ?: emptyMap()) }).withTestClient {
            sendConnectionInit()
            send { obj("type" to string("pong"), "payload" to obj("some" to string("value"))) }

            approver.assertApproved(receivedMessages().toList())
            assertThat(onPongPayload, equalTo(mapOf("some" to "value")))
        }
    }

    @Test
    fun `on subscribe one next message is sent per result and one complete is sent when done`(approver: Approver) {
        val requestExecutor: GraphQLWsRequestExecutor = {
            completedFuture(FakeExecutionResult(data = listOf(1, 2, 3).asFlow().asPublisher()))
        }
        GraphQLWsConsumer(requestExecutor).withTestClient {
            sendConnectionInit()
            sendSubscribe("subscribe-1")

            approver.assertApproved(receivedMessages().toList())
        }
    }

    @Test
    fun `on subscribe with existing id the socket is closed`() {
        val requestExecutor: GraphQLWsRequestExecutor = {
            val data = listOf(1).asFlow().onEach { delay(1000) }.asPublisher()
            completedFuture(FakeExecutionResult(data))
        }
        GraphQLWsConsumer(requestExecutor).withTestClient {
            sendConnectionInit()
            sendSubscribe("subscribe-1")
            sendSubscribe("subscribe-1")

            assertThat({ receivedMessages().toList() },
                throws(equalTo(ClosedWebsocket(WsStatus(4409, "Subscriber for 'subscriber-1' already exists")))))
        }
    }

    @Test
    fun `on subscribe without connection_init the socket is closed`() {
        GraphQLWsConsumer(emptyResult).withTestClient {
            sendSubscribe("subscribe-1")

            assertThat({ receivedMessages().toList() },
                throws(equalTo(ClosedWebsocket(WsStatus(4401, "Unauthorized")))))
        }
    }

    @Test
    fun `on subscribe with id of previously completed subscription is allowed`(approver: Approver) {
        val requestExecutor: GraphQLWsRequestExecutor = {
            completedFuture(FakeExecutionResult(listOf(1).asFlow().asPublisher()))
        }
        GraphQLWsConsumer(requestExecutor).withTestClient {
            sendConnectionInit()
            sendSubscribe("subscribe-1")

            val completedMessage = receivedMessages().find { it.path("type").asText() == "complete" }
            assertThat(completedMessage, present())

            sendSubscribe("subscribe-1")

            approver.assertApproved(receivedMessages().toList())
        }
    }

    private fun GraphQLWsConsumer.withTestClient(block: TestWsClient.() -> Unit) {
        use {
            block(websockets(it).testWsClient(Request(Method.GET, ""), receiveTimeout = Duration.ofMillis(100)))
        }
    }

    companion object {
        private val json = Jackson

        private fun TestWsClient.send(creator: Jackson.() -> JsonNode) {
            send(WsMessage(json.pretty(creator(json))))
        }
        private fun TestWsClient.sendConnectionInit(payload: Jackson.() -> JsonNode = { nullNode() }) =
            send { obj("type" to string("connection_init"), "payload" to payload(json)) }

        private fun TestWsClient.sendSubscribe(id: String) =
            send { obj("type" to string("subscribe"), "id" to string(id),
                "payload" to obj("query" to string("test query"))) }

        private fun TestWsClient.receivedMessages() = received().map { json.parse(it.bodyString()) }

        private fun Approver.assertApproved(messages: List<JsonNode>) = assertApproved(
            Response(Status.OK)
                .with(Header.CONTENT_TYPE of ContentType.APPLICATION_JSON)
                .body(json.asFormatString(json.array(messages)))
        )

        private val emptyResult: GraphQLWsRequestExecutor = {
            completedFuture(FakeExecutionResult(data = emptyFlow<Int>().asPublisher()))
        }
    }
}

private data class FakeExecutionResult(private val data: Any? = null,
                                       private val errors: List<GraphQLError> = emptyList()) : ExecutionResult {
    override fun getErrors(): List<GraphQLError> = errors
    @Suppress("UNCHECKED_CAST")
    override fun <T : Any?> getData(): T? = data as? T
    override fun isDataPresent(): Boolean = true
    override fun getExtensions(): Map<Any, Any> = emptyMap()
    override fun toSpecification(): Map<String, Any> = emptyMap()
}

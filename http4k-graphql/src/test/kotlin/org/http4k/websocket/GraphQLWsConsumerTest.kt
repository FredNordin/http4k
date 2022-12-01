package org.http4k.websocket

import com.fasterxml.jackson.databind.JsonNode
import com.natpryce.hamkrest.Matcher
import com.natpryce.hamkrest.and
import com.natpryce.hamkrest.assertion.assertThat
import com.natpryce.hamkrest.equalTo
import com.natpryce.hamkrest.has
import com.natpryce.hamkrest.present
import com.natpryce.hamkrest.throws
import graphql.ExecutionResult
import graphql.GraphQLError
import graphql.language.SourceLocation
import graphql.validation.ValidationError.newValidationError
import graphql.validation.ValidationErrorType
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.map
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
import org.http4k.websocket.GraphQLWsMessage.ConnectionAck
import org.http4k.websocket.GraphQLWsMessage.Pong
import org.http4k.websocket.GraphQLWsMessage.Subscribe
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.extension.ExtendWith
import java.lang.IllegalStateException
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletableFuture.completedFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

@Timeout(5, unit = TimeUnit.SECONDS)
@ExtendWith(JsonApprovalTest::class)
class GraphQLWsConsumerTest {

    @Test
    fun `on connection_init a connection_ack message is sent with result from onConnectionInit`(approver: Approver) =
        GraphQLWsConsumer(emptyResult, onConnect = { ConnectionAck(it.payload) }).withTestClient {
            sendConnectionInit(payload = { obj("some" to string("value")) })

            approver.assertApproved(receivedMessages().take(1).toList())
        }

    @Test
    fun `on connection_init the socket is closed when onConnectionInit returns null`() =
        GraphQLWsConsumer(emptyResult, onConnect = { null }).withTestClient {
            sendConnectionInit(payload = { obj("some" to string("value")) })

            assertThat({ receivedMessages().take(1).toList() },
                throws(closedWebsocketWithStatus(4403, "Forbidden")))
        }

    @Test
    fun `when connection_init is not sent withing timeout the socket is closed`() =
        GraphQLWsConsumer(emptyResult, connectionInitWaitTimeout = Duration.ofMillis(1)).withTestClient {
            assertThat({ receivedMessages().take(1).toList() },
                throws(closedWebsocketWithStatus(4408, "Connection initialisation timeout")))
        }

    @Test
    fun `on multiple connection_init the socket is closed`() =
        GraphQLWsConsumer(emptyResult).withTestClient {
            sendConnectionInit()
            sendConnectionInit()

            assertThat({ receivedMessages().take(2).toList() },
                throws(closedWebsocketWithStatus(4429, "Too many initialisation requests")))
        }

    @Test
    fun `on ping a pong message is sent with result from onPing`(approver: Approver) =
        GraphQLWsConsumer(emptyResult, onPing = { Pong(it.payload) }).withTestClient {
            sendConnectionInit()
            send { obj("type" to string("ping"), "payload" to obj("some" to string("value"))) }

            approver.assertApproved(receivedMessages().take(2).toList())
        }

    @Test
    fun `on pong no message is sent and onPong is invoked`(approver: Approver) {
        val onPongInvoked = AtomicBoolean(false)
        GraphQLWsConsumer(emptyResult, onPong = { onPongInvoked.set(it.payload?.get("some") != null) }).withTestClient {
            sendConnectionInit()
            send { obj("type" to string("pong"), "payload" to obj("some" to string("value"))) }

            approver.assertApproved(receivedMessages().take(2).toList())
            assertThat(onPongInvoked.get(), equalTo(true))
        }
    }

    @Test
    fun `on subscribe one next message is sent per result and one complete is sent when done`(approver: Approver) {
        val onSubscribe: OnSubscribe = {
            completedFuture(FakeExecutionResult(data = listOf(1, 2, 3).asFlow().asPublisher()))
        }
        GraphQLWsConsumer(onSubscribe).withTestClient {
            sendConnectionInit()
            sendSubscribe("subscribe-1")

            approver.assertApproved(receivedMessages().take(5).toList())
        }
    }

    @Test
    fun `on subscribe with existing id the socket is closed`() {
        val onSubscribe: OnSubscribe = {
            val data = listOf(1).asFlow().onEach { delay(1000) }.asPublisher()
            completedFuture(FakeExecutionResult(data))
        }
        GraphQLWsConsumer(onSubscribe).withTestClient {
            sendConnectionInit()
            sendSubscribe("subscribe-1")
            sendSubscribe("subscribe-1")

            assertThat({ receivedMessages().take(4).toList() },
                throws(closedWebsocketWithStatus(4409, "Subscriber for 'subscribe-1' already exists")))
        }
    }

    @Test
    fun `on subscribe without connection_init the socket is closed`() {
        GraphQLWsConsumer(emptyResult).withTestClient {
            sendSubscribe("subscribe-1")

            assertThat({ receivedMessages().take(1).toList() },
                throws(closedWebsocketWithStatus(4401, "Unauthorized")))
        }
    }

    @Test
    fun `on subscribe with id of previously completed subscription is allowed`(approver: Approver) {
        val onSubscribe: OnSubscribe = {
            completedFuture(FakeExecutionResult(listOf(1).asFlow().asPublisher()))
        }
        GraphQLWsConsumer(onSubscribe).withTestClient {
            sendConnectionInit()
            sendSubscribe("subscribe-1")

            val completeMessage = receivedMessages().take(3).find { it.path("type").asText() == "complete" }
            assertThat(completeMessage, present())

            sendSubscribe("subscribe-1")

            approver.assertApproved(receivedMessages().take(2).toList())
        }
    }

    @Test
    fun `on subscribe an error message is sent when execution result has errors`(approver: Approver) {
        val onSubscribe: OnSubscribe = {
            completedFuture(FakeExecutionResult(errors = listOf(
                newValidationError()
                    .validationErrorType(ValidationErrorType.FieldUndefined)
                    .description("someField")
                    .sourceLocation(SourceLocation(12, 34))
                    .build(),
                newValidationError()
                    .validationErrorType(ValidationErrorType.FieldUndefined)
                    .description("anotherField")
                    .sourceLocation(SourceLocation(56, 78))
                    .build()
            )))
        }
        GraphQLWsConsumer(onSubscribe).withTestClient {
            sendConnectionInit()
            sendSubscribe("subscribe-1")

            approver.assertApproved(receivedMessages().take(3).toList())
        }
    }

    @Test
    fun `on subscribe with id of previously errored subscription is allowed`(approver: Approver) {
        val onSubscribe: OnSubscribe = {
            completedFuture(FakeExecutionResult(errors = listOf(
                newValidationError()
                    .validationErrorType(ValidationErrorType.FieldUndefined)
                    .description("someField")
                    .sourceLocation(SourceLocation(12, 34))
                    .build()
            )))
        }
        GraphQLWsConsumer(onSubscribe).withTestClient {
            sendConnectionInit()
            sendSubscribe("subscribe-1")

            val errorMessage = receivedMessages().take(2).find { it.path("type").asText() == "error" }
            assertThat(errorMessage, present())

            sendSubscribe("subscribe-1")

            approver.assertApproved(receivedMessages().take(1).toList())
        }
    }

    @Test
    fun `on subscribe an error message is sent when request executor returns error`(approver: Approver) {
        val onSubscribe: OnSubscribe = {
            CompletableFuture<ExecutionResult>().apply { completeExceptionally(IllegalStateException("Boom!")) }
        }
        GraphQLWsConsumer(onSubscribe).withTestClient {
            sendConnectionInit()
            sendSubscribe("subscribe-1")

            approver.assertApproved(receivedMessages().take(3).toList())
        }
    }

    @Test
    fun `on subscribe an error message is sent and no more messages when subscription result contains error`(approver: Approver) {
        val onSubscribe: OnSubscribe = {
            val data = listOf({ 1 }, { throw IllegalStateException("Boom!") }, { 2 })
                .asFlow().map { it() }.asPublisher()
            completedFuture(FakeExecutionResult(data = data))
        }
        GraphQLWsConsumer(onSubscribe).withTestClient {
            sendConnectionInit()
            sendSubscribe("subscribe-1")

            approver.assertApproved(receivedMessages().take(4).toList())
        }
    }

    @Test
    fun `on subscribe for multiple subscriptions results in messages for all subscriptions`(approver: Approver) {
        val onSubscribe: OnSubscribe = {
            completedFuture(FakeExecutionResult(data = listOf(1, 2, 3).asFlow().asPublisher()))
        }
        GraphQLWsConsumer(onSubscribe).withTestClient {
            sendConnectionInit()
            sendSubscribe("subscribe-1")
            sendSubscribe("subscribe-2")

            val messagesById = receivedMessages().sortedBy { it.path("id").asText() }.take(10).toList()

            approver.assertApproved(messagesById)
        }
    }

    @Test
    fun `on complete stops sending messages for active subscription`(approver: Approver) {
        val onSubscribe: OnSubscribe = {
            val data = listOf(1, 2, 3).asFlow().onEach { if (it > 1) delay(40) }.asPublisher()
            completedFuture(FakeExecutionResult(data))
        }
        GraphQLWsConsumer(onSubscribe).withTestClient {
            sendConnectionInit()
            sendSubscribe("subscribe-1")

            val messages = receivedMessages().onEach {
                if (it.path("type").asText() == "next") {
                    sendComplete("subscribe-1")
                }
            }.take(3).toList()

            approver.assertApproved(messages)
        }
    }

    @Test
    fun `on complete does nothing when the id does not match any subscriptions`(approver: Approver) {
        val onSubscribe: OnSubscribe = {
            val data = listOf(1, 2, 3).asFlow().onEach { if (it > 1) delay(10) }.asPublisher()
            completedFuture(FakeExecutionResult(data))
        }
        GraphQLWsConsumer(onSubscribe).withTestClient {
            sendConnectionInit()
            sendSubscribe("subscribe-1")

            val messages = receivedMessages().onEach {
                if (it.path("type").asText() == "next") {
                    sendComplete("subscribe-2")
                }
            }.take(5).toList()

            approver.assertApproved(messages)
        }
    }

    @Test
    fun `on invalid message type the socket is closed`() {
        GraphQLWsConsumer(emptyResult).withTestClient {
            send { obj("type" to string("unknown")) }

            assertThat({ receivedMessages().take(1).toList() },
                throws(closedWebsocketWithStatus(4400, "graphql-ws message field 'type' must be string")))
        }
    }

    @Test
    fun `on connection_ack or next or error the messages are ignored`(approver: Approver) {
        GraphQLWsConsumer(emptyResult).withTestClient {
            sendConnectionInit()
            send { obj("type" to string("connection_ack")) }
            send { obj("type" to string("next"), "id" to string("anId")) }
            send { obj("type" to string("error"), "id" to string("anId"), "payload" to array(emptyList())) }

            approver.assertApproved(receivedMessages().take(2).toList())
        }
    }

    @Test
    fun `onClose is called when the socket is closed`() {
        var onCloseStatus: WsStatus? = null
        GraphQLWsConsumer(emptyResult, onClose = { onCloseStatus = it }).withTestClient {
            sendSubscribe("subscribe-1")

            assertThat({ receivedMessages().take(1).toList() }, throws<ClosedWebsocket>())
            assertThat(onCloseStatus, present(hasStatus(4401, "Unauthorized")))
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

        private fun TestWsClient.sendComplete(id: String) =
            send { obj("type" to string("complete"), "id" to string(id)) }

        private fun TestWsClient.receivedMessages() = received()
            .map { json.parse(it.bodyString()) }

        private fun Approver.assertApproved(messages: List<JsonNode>) = assertApproved(
            Response(Status.OK)
                .with(Header.CONTENT_TYPE of ContentType.APPLICATION_JSON)
                .body(json.asFormatString(json.array(messages)))
        )

        private val emptyResult: OnSubscribe = {
            completedFuture(FakeExecutionResult(data = emptyFlow<Int>().asPublisher()))
        }

        private fun closedWebsocketWithStatus(code: Int, description: String): Matcher<ClosedWebsocket> =
            has(ClosedWebsocket::status, hasStatus(code, description))

        private fun hasStatus(code: Int, description: String): Matcher<WsStatus> =
            has(WsStatus::code, equalTo(code)) and has(WsStatus::description, equalTo(description))

        private fun GraphQLWsConsumer.withTestClient(block: TestWsClient.() -> Unit) {
            use {
                block(websockets(it).testWsClient(Request(Method.GET, ""), receiveTimeout = Duration.ofMillis(50)))
            }
        }
    }
}

typealias OnSubscribe = Request.(Subscribe) -> CompletableFuture<ExecutionResult>

private data class FakeExecutionResult(private val data: Any? = null,
                                       private val errors: List<GraphQLError> = emptyList()) : ExecutionResult {
    override fun getErrors(): List<GraphQLError> = errors
    @Suppress("UNCHECKED_CAST")
    override fun <T : Any?> getData(): T? = data as? T
    override fun isDataPresent(): Boolean = errors.isEmpty()
    override fun getExtensions(): Map<Any, Any> = emptyMap()
    override fun toSpecification(): Map<String, Any> = emptyMap()
}

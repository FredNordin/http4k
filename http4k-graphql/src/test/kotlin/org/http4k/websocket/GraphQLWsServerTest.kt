package org.http4k.websocket

import com.fasterxml.jackson.databind.JsonNode
import com.natpryce.hamkrest.Matcher
import com.natpryce.hamkrest.allElements
import com.natpryce.hamkrest.and
import com.natpryce.hamkrest.assertion.assertThat
import com.natpryce.hamkrest.equalTo
import com.natpryce.hamkrest.has
import com.natpryce.hamkrest.isA
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
import org.http4k.graphql.GraphQLRequest
import org.http4k.graphql.ws.GraphQLWsMessage.Complete
import org.http4k.graphql.ws.GraphQLWsMessage.ConnectionAck
import org.http4k.graphql.ws.GraphQLWsMessage.ConnectionInit
import org.http4k.graphql.ws.GraphQLWsMessage.Next
import org.http4k.graphql.ws.GraphQLWsMessage.Ping
import org.http4k.graphql.ws.GraphQLWsMessage.Pong
import org.http4k.graphql.ws.GraphQLWsMessage.Subscribe
import org.http4k.lens.Header
import org.http4k.routing.websockets
import org.http4k.testing.Approver
import org.http4k.testing.ClosedWebsocket
import org.http4k.testing.JsonApprovalTest
import org.http4k.testing.TestWsClient
import org.http4k.testing.testWsClient
import org.http4k.websocket.GraphQLWsEvent.Closed
import org.http4k.websocket.GraphQLWsEvent.MessageReceived
import org.http4k.websocket.GraphQLWsEvent.MessageSent
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.extension.ExtendWith
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletableFuture.completedFuture
import java.util.concurrent.TimeUnit

@Timeout(5, unit = TimeUnit.SECONDS)
@ExtendWith(JsonApprovalTest::class)
class GraphQLWsServerTest {

    @Test
    fun `on connection_init a connection_ack message is sent with result from connection handler`(approver: Approver) =
        GraphQLWsServer(connectionHandler = { ConnectionAck(it.payload) }, subscribeHandler = emptyResult).withTestClient {
            sendConnectionInit(payload = { obj("some" to string("value")) })

            approver.assertApproved(receivedMessages().take(1).toList())
        }

    @Test
    fun `on connection_init the socket is closed when connection handler returns null`() =
        GraphQLWsServer(connectionHandler = { null }, subscribeHandler = emptyResult).withTestClient {
            sendConnectionInit(payload = { obj("some" to string("value")) })

            assertThat({ receivedMessages().take(1).toList() },
                throws(closedWebsocketWithStatus(4403, "Forbidden")))
        }

    @Test
    fun `when connection_init is not sent withing timeout the socket is closed`() =
        GraphQLWsServer(connectionInitWaitTimeout = Duration.ofMillis(1), subscribeHandler = emptyResult).withTestClient {
            assertThat({ receivedMessages().take(1).toList() },
                throws(closedWebsocketWithStatus(4408, "Connection initialisation timeout")))
        }

    @Test
    fun `on multiple connection_init the socket is closed`() =
        GraphQLWsServer(subscribeHandler = emptyResult).withTestClient {
            sendConnectionInit()
            sendConnectionInit()

            assertThat({ receivedMessages().take(2).toList() },
                throws(closedWebsocketWithStatus(4429, "Too many initialisation requests")))
        }

    @Test
    fun `on ping a pong message is sent with result from ping handler`(approver: Approver) =
        GraphQLWsServer(pingHandler = { Pong(it.payload) }, subscribeHandler = emptyResult).withTestClient {
            sendConnectionInit()
            send { obj("type" to string("ping"), "payload" to obj("some" to string("value"))) }

            approver.assertApproved(receivedMessages().take(2).toList())
        }

    @Test
    fun `on subscribe one next message is sent per result and one complete is sent when done`(approver: Approver) {
        GraphQLWsServer {
            completedFuture(FakeExecutionResult(data = listOf(1, 2, 3).asFlow().asPublisher()))
        }.withTestClient {
            sendConnectionInit()
            sendSubscribe("subscribe-1")

            approver.assertApproved(receivedMessages().take(5).toList())
        }
    }

    @Test
    fun `on subscribe with existing id the socket is closed`() {
        GraphQLWsServer {
            val data = listOf(1).asFlow().onEach { delay(1000) }.asPublisher()
            completedFuture(FakeExecutionResult(data))
        }.withTestClient {
            sendConnectionInit()
            sendSubscribe("subscribe-1")
            sendSubscribe("subscribe-1")

            assertThat({ receivedMessages().take(4).toList() },
                throws(closedWebsocketWithStatus(4409, "Subscriber for 'subscribe-1' already exists")))
        }
    }

    @Test
    fun `on subscribe without connection_init the socket is closed`() {
        GraphQLWsServer(subscribeHandler = emptyResult).withTestClient {
            sendSubscribe("subscribe-1")

            assertThat({ receivedMessages().take(1).toList() },
                throws(closedWebsocketWithStatus(4401, "Unauthorized")))
        }
    }

    @Test
    fun `on subscribe with id of previously completed subscription is allowed`(approver: Approver) {
        GraphQLWsServer {
            completedFuture(FakeExecutionResult(listOf(1).asFlow().asPublisher()))
        }.withTestClient {
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
        GraphQLWsServer {
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
        }.withTestClient {
            sendConnectionInit()
            sendSubscribe("subscribe-1")

            approver.assertApproved(receivedMessages().take(3).toList())
        }
    }

    @Test
    fun `on subscribe with id of previously errored subscription is allowed`(approver: Approver) {
        GraphQLWsServer {
            completedFuture(FakeExecutionResult(errors = listOf(
                newValidationError()
                    .validationErrorType(ValidationErrorType.FieldUndefined)
                    .description("someField")
                    .sourceLocation(SourceLocation(12, 34))
                    .build()
            )))
        }.withTestClient {
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
        GraphQLWsServer {
            CompletableFuture<ExecutionResult>().apply { completeExceptionally(IllegalStateException("Boom!")) } // Yuck! Java 8 :(
        }.withTestClient {
            sendConnectionInit()
            sendSubscribe("subscribe-1")

            approver.assertApproved(receivedMessages().take(3).toList())
        }
    }

    @Test
    fun `on subscribe an error message is sent and no more messages when subscription result contains error`(approver: Approver) {
        GraphQLWsServer {
            val data = listOf({ 1 }, { throw IllegalStateException("Boom!") }, { 2 }).asFlow().map { it() }.asPublisher()
            completedFuture(FakeExecutionResult(data = data))
        }.withTestClient {
            sendConnectionInit()
            sendSubscribe("subscribe-1")

            approver.assertApproved(receivedMessages().take(4).toList())
        }
    }

    @Test
    fun `on subscribe for multiple subscriptions results in messages for all subscriptions`(approver: Approver) {
        GraphQLWsServer {
            completedFuture(FakeExecutionResult(data = listOf(1, 2, 3).asFlow().asPublisher()))
        }.withTestClient {
            sendConnectionInit()
            sendSubscribe("subscribe-1")
            sendSubscribe("subscribe-2")

            val messagesById = receivedMessages().sortedBy { it.path("id").asText() }.take(10).toList()

            approver.assertApproved(messagesById)
        }
    }

    @Test
    fun `on subscribe for single non-streaming result results in one next and one complete message`(approver: Approver) {
        GraphQLWsServer {
            completedFuture(FakeExecutionResult(data = 1))
        }.withTestClient {
            sendConnectionInit()
            sendSubscribe("subscribe-1")

            val messagesById = receivedMessages().take(3).toList()

            approver.assertApproved(messagesById)
        }
    }

    @Test
    fun `on subscribe for single non-streaming error results in one error message and no complete`(approver: Approver) {
        GraphQLWsServer {
            completedFuture(FakeExecutionResult(errors = listOf(
                newValidationError()
                    .validationErrorType(ValidationErrorType.FieldUndefined)
                    .description("someField")
                    .sourceLocation(SourceLocation(12, 34))
                    .build()
            )))
        }.withTestClient {
            sendConnectionInit()
            sendSubscribe("subscribe-1")

            val messagesById = receivedMessages().take(3).toList()

            approver.assertApproved(messagesById)
        }
    }

    @Test
    fun `on complete stops sending messages for active subscription`(approver: Approver) {
        GraphQLWsServer {
            val data = listOf(1, 2, 3).asFlow().onEach { if (it > 1) delay(40) }.asPublisher()
            completedFuture(FakeExecutionResult(data))
        }.withTestClient {
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
        GraphQLWsServer {
            val data = listOf(1, 2, 3).asFlow().onEach { if (it > 1) delay(10) }.asPublisher()
            completedFuture(FakeExecutionResult(data))
        }.withTestClient {
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
        GraphQLWsServer(subscribeHandler = emptyResult).withTestClient {
            send { obj("type" to string("unknown")) }

            assertThat({ receivedMessages().take(1).toList() },
                throws(closedWebsocketWithStatus(4400, "graphql-ws message field 'type' must be string")))
        }
    }

    @Test
    fun `on connection_ack or next or error or pong the messages are ignored`(approver: Approver) {
        GraphQLWsServer(subscribeHandler = emptyResult).withTestClient {
            sendConnectionInit()
            send { obj("type" to string("connection_ack")) }
            send { obj("type" to string("next"), "id" to string("anId")) }
            send { obj("type" to string("error"), "id" to string("anId"), "payload" to array(emptyList())) }
            send { obj("type" to string("pong")) }

            approver.assertApproved(receivedMessages().take(2).toList())
        }
    }

    @Test
    fun `onEvent is called when each message is sent`(approver: Approver) {
        val events = mutableListOf<GraphQLWsEvent>()
        GraphQLWsServer(onEvent = { events += it }) {
            completedFuture(FakeExecutionResult(data = listOf(1, 2).asFlow().asPublisher()))
        }.withTestClient {
            sendConnectionInit()
            sendSubscribe("subscribe-1")

            receivedMessages().take(4).toList()

            val messageSentEvents = events.filterIsInstance<MessageSent>()

            assertThat(messageSentEvents, equalTo(listOf(
                MessageSent(ConnectionAck(payload = null)),
                MessageSent(Next("subscribe-1", 1)),
                MessageSent(Next("subscribe-1", 2)),
                MessageSent(Complete("subscribe-1"))
            )))
        }
    }

    @Test
    fun `onEvent is called when each message is received`(approver: Approver) {
        val events = mutableListOf<GraphQLWsEvent>()
        GraphQLWsServer(onEvent = { events += it }) {
            completedFuture(FakeExecutionResult(data = listOf(1, 2).asFlow().asPublisher()))
        }.withTestClient {
            sendConnectionInit()
            send { obj("type" to string("ping")) }
            sendSubscribe("subscribe-1")

            receivedMessages().take(5).toList()

            val messageSentEvents = events.filterIsInstance<MessageReceived>()

            assertThat(messageSentEvents, equalTo(listOf(
                MessageReceived(ConnectionInit(payload = null)),
                MessageReceived(Ping(payload = null)),
                MessageReceived(Subscribe("subscribe-1", GraphQLRequest("test query")))
            )))
        }
    }

    @Test
    fun `onEvent is called when the socket is closed`() {
        val events = mutableListOf<GraphQLWsEvent>()
        GraphQLWsServer(onEvent = { events += it }, subscribeHandler = emptyResult).withTestClient {
            close(WsStatus.GOING_AWAY)

            assertThat(events, allElements(
                isA(has(Closed::status, hasStatus(1001, "Going away")))
            ))
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

        private val emptyResult: Request.(Subscribe) -> CompletableFuture<ExecutionResult> = {
            completedFuture(FakeExecutionResult(data = emptyFlow<Int>().asPublisher()))
        }

        private fun closedWebsocketWithStatus(code: Int, description: String): Matcher<ClosedWebsocket> =
            has(ClosedWebsocket::status, hasStatus(code, description))

        private fun hasStatus(code: Int, description: String): Matcher<WsStatus> =
            has(WsStatus::code, equalTo(code)) and has(WsStatus::description, equalTo(description))

        private fun GraphQLWsServer.withTestClient(block: TestWsClient.() -> Unit) {
            use {
                block(websockets(it).testWsClient(Request(Method.GET, ""), receiveTimeout = Duration.ofMillis(50)))
            }
        }
    }
}

private data class FakeExecutionResult(private val data: Any? = null,
                                       private val errors: List<GraphQLError> = emptyList()) : ExecutionResult {
    override fun getErrors(): List<GraphQLError> = errors
    @Suppress("UNCHECKED_CAST")
    override fun <T : Any?> getData(): T? = data as? T
    override fun isDataPresent(): Boolean = errors.isEmpty()
    override fun getExtensions(): Map<Any, Any> = emptyMap()
    override fun toSpecification(): Map<String, Any> = emptyMap()
}

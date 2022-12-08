package org.http4k.websocket

import com.natpryce.hamkrest.and
import com.natpryce.hamkrest.anyElement
import com.natpryce.hamkrest.assertion.assertThat
import com.natpryce.hamkrest.equalTo
import com.natpryce.hamkrest.has
import com.natpryce.hamkrest.hasElement
import com.natpryce.hamkrest.hasSize
import com.natpryce.hamkrest.isA
import com.natpryce.hamkrest.present
import graphql.GraphQLException
import org.http4k.core.Body
import org.http4k.core.Method
import org.http4k.core.Request
import org.http4k.core.Uri
import org.http4k.format.Jackson
import org.http4k.graphql.GraphQLRequest
import org.http4k.graphql.ws.GraphQLWsMessage
import org.http4k.graphql.ws.GraphQLWsMessage.Complete
import org.http4k.graphql.ws.GraphQLWsMessage.ConnectionAck
import org.http4k.graphql.ws.GraphQLWsMessage.ConnectionInit
import org.http4k.graphql.ws.GraphQLWsMessage.Error
import org.http4k.graphql.ws.GraphQLWsMessage.Next
import org.http4k.graphql.ws.GraphQLWsMessage.Ping
import org.http4k.graphql.ws.GraphQLWsMessage.Pong
import org.http4k.graphql.ws.GraphQLWsMessage.Subscribe
import org.http4k.lens.GraphQLWsMessageLens
import org.http4k.websocket.GraphQLWsEvent.Closed
import org.http4k.websocket.GraphQLWsEvent.MessageReceived
import org.http4k.websocket.GraphQLWsEvent.MessageSent
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

@Timeout(5, unit = TimeUnit.SECONDS)
class GraphQLWsClientTest {

    @Test
    fun `sends connection_init on ws connect`() {
        val events = LinkedBlockingQueue<GraphQLWsEvent>()
        GraphQLWsClient(onEvent = { events.add(it) }){}.withFakeServer { server ->
            server.awaitConnected()

            events.mustHaveItems(
                MessageSent(ConnectionInit(payload = null)),
                MessageReceived(ConnectionAck(payload = null))
            )
        }
    }

    @Test
    fun `sends connection_init with result of custom connectionHandler on ws connect`() {
        val events = LinkedBlockingQueue<GraphQLWsEvent>()
        GraphQLWsClient(
            connectionHandler = { ConnectionInit(payload = mapOf("method" to method, "uri" to uri)) },
            onEvent = { events.add(it) }){
        }.withFakeServer { server ->
            server.awaitConnected()

            events.mustHaveItems(
                MessageSent(ConnectionInit(payload = mapOf("method" to Method.GET, "uri" to Uri.of("graphql-ws")))),
                MessageReceived(ConnectionAck(payload = null))
            )
        }
    }

    @Test
    fun `when socket is closed during connection a closed event is emitted`() {
        val events = LinkedBlockingQueue<GraphQLWsEvent>()
        GraphQLWsClient(onEvent = { events.add(it) }) {}.withFakeServer(allowConnection = false) {
            events.mustHaveItems(
                MessageSent(ConnectionInit(payload = null)),
                Closed(WsStatus(4403, "Forbidden"))
            )
        }
    }

    @Test
    fun `when connection_ack is not received within timout the socket is closed`() {
        val events = LinkedBlockingQueue<GraphQLWsEvent>()
        GraphQLWsClient(connectionAckWaitTimeout = Duration.ofMillis(10), onEvent = { events.add(it) }) {}
            .withFakeServer(sendConnectionAck = false) {
                events.mustHaveItems(
                    MessageSent(ConnectionInit(payload = null)),
                    Closed(WsStatus(4408, "Connection initialisation timeout"))
                )
            }
    }

    @Test
    fun `when ping is received then a pong is sent`() {
        val events = LinkedBlockingQueue<GraphQLWsEvent>()
        GraphQLWsClient(onEvent = { events.add(it) }){}.withFakeServer { server ->
            server.awaitConnected()
            server.send(Ping(payload = mapOf("some" to "value")))

            events.mustHaveItems(
                MessageSent(ConnectionInit(payload = null)),
                MessageSent(Pong(payload = null)),
                MessageReceived(ConnectionAck(payload = null)),
                MessageReceived(Ping(payload = mapOf("some" to "value")))
            )
        }
    }

    @Test
    fun `when ping is received using a custom pingHandler then a pong is sent`() {
        val events = LinkedBlockingQueue<GraphQLWsEvent>()
        GraphQLWsClient(
            pingHandler = { Pong(payload = mapOf("client" to "value") + it.payload.orEmpty()) },
            onEvent = { events.add(it) }){
        }.withFakeServer { server ->
            server.awaitConnected()
            server.send(Ping(payload = mapOf("server" to "value")))

            events.mustHaveItems(
                MessageSent(ConnectionInit(payload = null)),
                MessageSent(Pong(payload = mapOf("client" to "value", "server" to "value"))),
                MessageReceived(ConnectionAck(payload = null)),
                MessageReceived(Ping(payload = mapOf("server" to "value")))
            )
        }
    }

    @Test
    fun `subscription that completes results in all values returned`() {
        val events = LinkedBlockingQueue<GraphQLWsEvent>()
        val subscriber = TestSubscriber()
        GraphQLWsClient(onEvent = { events.add(it) }) { connection ->
            connection.newSubscription("sub-1", GraphQLRequest("some subscription")).subscribe(subscriber)
        }.withFakeServer { server ->
            server.awaitConnected()
            server.sendNext()
            server.sendNext()
            server.sendNext()
            server.sendComplete()

            subscriber.values.mustHaveItems("1", "2", "3")

            events.mustHaveItems(
                MessageSent(ConnectionInit(payload = null)),
                MessageReceived(ConnectionAck(payload = null)),
                MessageSent(Subscribe("sub-1", GraphQLRequest("some subscription"))),
                MessageReceived(Next("sub-1", "1")),
                MessageReceived(Next("sub-1", "2")),
                MessageReceived(Next("sub-1", "3")),
                MessageReceived(Complete("sub-1"))
            )
        }
    }

    @Test
    fun `cancelling subscription results in only values received to that point being returned`() {
        val events = LinkedBlockingQueue<GraphQLWsEvent>()
        val subscriber = TestSubscriber()
        GraphQLWsClient(onEvent = { events.add(it) }) { connection ->
            connection.newSubscription("sub-1", GraphQLRequest("some subscription")).subscribe(subscriber)
        }.withFakeServer { server ->
            server.awaitConnected()
            server.sendNext()
            subscriber.cancel()
            server.sendNext()
            server.sendComplete()

            subscriber.values.mustHaveItems("1")

            events.mustHaveItems(
                MessageSent(ConnectionInit(payload = null)),
                MessageReceived(ConnectionAck(payload = null)),
                MessageSent(Subscribe("sub-1", GraphQLRequest("some subscription"))),
                MessageReceived(Next("sub-1", "1")),
                MessageSent(Complete("sub-1"))
            )
        }
    }

    @Test
    fun `only messages for open client subscriptions are returned`() {
        val events = LinkedBlockingQueue<GraphQLWsEvent>()
        val subscriber = TestSubscriber()
        GraphQLWsClient(onEvent = { events.add(it) }) { connection ->
            connection.newSubscription("sub-1", GraphQLRequest("some subscription")).subscribe(subscriber)
        }.withFakeServer { server ->
            server.awaitConnected()
            server.sendNext()
            server.sendNext("sub-2")
            server.sendNext("sub-3")
            server.sendComplete("sub-2")
            server.sendComplete("sub-3")
            server.sendComplete()

            subscriber.values.mustHaveItems("1")

            events.mustHaveItems(
                MessageSent(ConnectionInit(payload = null)),
                MessageReceived(ConnectionAck(payload = null)),
                MessageSent(Subscribe("sub-1", GraphQLRequest("some subscription"))),
                MessageReceived(Next("sub-1", "1")),
                MessageReceived(Next("sub-2", "2")),
                MessageReceived(Next("sub-3", "3")),
                MessageReceived(Complete("sub-1")),
                MessageReceived(Complete("sub-2")),
                MessageReceived(Complete("sub-3"))
            )
        }
    }

    @Test
    fun `subscription that errors results in error passed to subscriber`() {
        val events = LinkedBlockingQueue<GraphQLWsEvent>()
        var error: Throwable? = null
        val subscriber = TestSubscriber { error = it }
        GraphQLWsClient(onEvent = { events.add(it) }) { connection ->
            connection.newSubscription("sub-1", GraphQLRequest("some subscription")).subscribe(subscriber)
        }.withFakeServer { server ->
            server.awaitConnected()
            server.sendError()

            assertThat(error, present(isA(has(GraphQLException::message, equalTo("[{message=boom}]")))))

            events.mustHaveItems(
                MessageSent(ConnectionInit(payload = null)),
                MessageReceived(ConnectionAck(payload = null)),
                MessageSent(Subscribe("sub-1", GraphQLRequest("some subscription"))),
                MessageReceived(Error("sub-1", listOf(mapOf("message" to "boom"))))
            )
        }
    }

    @Test
    fun `subscription with existing id results in socket being closed`() {
        val events = LinkedBlockingQueue<GraphQLWsEvent>()
        GraphQLWsClient(onEvent = { events.add(it) }) { connection ->
            connection.newSubscription("sub-1", GraphQLRequest("some subscription")).subscribe(TestSubscriber())
            connection.newSubscription("sub-1", GraphQLRequest("another subscription")).subscribe(TestSubscriber())
        }.withFakeServer { server ->
            server.awaitConnected()

            events.mustHaveItems(
                MessageSent(ConnectionInit(payload = null)),
                MessageReceived(ConnectionAck(payload = null)),
                MessageSent(Subscribe("sub-1", GraphQLRequest("some subscription"))),
                Closed(WsStatus(4409, "Subscriber for 'sub-1' already exists"))
            )
        }
    }

    @Test
    fun `subscription with id existing on server results in socket being closed`() {
        val events = LinkedBlockingQueue<GraphQLWsEvent>()
        GraphQLWsClient(onEvent = { events.add(it) }) { connection ->
            connection.newSubscription("sub-1", GraphQLRequest("some subscription")).subscribe(TestSubscriber())
        }.withFakeServer(subscriptionAlreadyExists = true) { server ->
            server.awaitConnected()

            events.mustHaveItems(
                MessageSent(ConnectionInit(payload = null)),
                MessageReceived(ConnectionAck(payload = null)),
                MessageSent(Subscribe("sub-1", GraphQLRequest("some subscription"))),
                Closed(WsStatus(4409, "Subscriber for 'sub-1' already exists"))
            )
        }
    }

    @Test
    fun `disconnecting from connection results in socket being closed with normal status and no more subscription messages`() {
        val events = LinkedBlockingQueue<GraphQLWsEvent>()
        GraphQLWsClient(onEvent = { events.add(it) }) { connection ->
            connection.newSubscription("sub-1", GraphQLRequest("some subscription")).subscribe(TestSubscriber())
            connection.disconnect()
        }.withFakeServer { server ->
            server.awaitConnected()
            server.sendNext()

            events.mustHaveItems(
                MessageSent(ConnectionInit(payload = null)),
                MessageReceived(ConnectionAck(payload = null)),
                MessageSent(Subscribe("sub-1", GraphQLRequest("some subscription"))),
                Closed(WsStatus.NORMAL)
            )
        }
    }

    @Test
    fun `connection_init and subscribe and pong messages should be ignored but visible in events`() {
        val events = LinkedBlockingQueue<GraphQLWsEvent>()
        GraphQLWsClient(onEvent = { events.add(it) }) { connection ->
            connection.newSubscription("sub-1", GraphQLRequest("some subscription")).subscribe(TestSubscriber())
        }.withFakeServer { server ->
            server.awaitConnected()
            server.sendNext()
            server.send(ConnectionInit(payload = mapOf("some" to "value")))
            server.send(Subscribe("some_id", GraphQLRequest("some query")))
            server.send(Pong(payload = mapOf("some" to "value")))
            server.sendComplete()

            events.mustHaveItems(
                MessageSent(ConnectionInit(payload = null)),
                MessageSent(Subscribe("sub-1", GraphQLRequest("some subscription"))),
                MessageReceived(ConnectionAck(payload = null)),
                MessageReceived(Next("sub-1", "1")),
                MessageReceived(Complete("sub-1")),
                MessageReceived(ConnectionInit(payload = mapOf("some" to "value"))),
                MessageReceived(Subscribe("some_id", GraphQLRequest("some query"))),
                MessageReceived(Pong(payload = mapOf("some" to "value")))
            )
        }
    }

    @Test
    fun `on invalid message returned from server the socket is closed`() {
        val events = LinkedBlockingQueue<GraphQLWsEvent>()
        GraphQLWsClient(onEvent = { events.add(it) }) {}.withFakeServer { server ->
            server.awaitConnected()
            server.sendInvalidMessage()

            events.mustHaveItems(
                MessageSent(ConnectionInit(payload = null)),
                MessageReceived(ConnectionAck(payload = null)),
                Closed(WsStatus(4400, "graphql-ws message field 'type' is required"))
            )
        }
    }

    companion object {
        private fun GraphQLWsClient.withFakeServer(
            allowConnection: Boolean = true,
            sendConnectionAck: Boolean = true,
            subscriptionAlreadyExists: Boolean = false,
            block: (FakeServer) -> Unit
        ) = use {
            val fakeServer = FakeServer(allowConnection, sendConnectionAck, subscriptionAlreadyExists)
            this(fakeServer)
            block(fakeServer)
        }

        private fun <T> LinkedBlockingQueue<T>.mustHaveItems(vararg expectedItems: T) {
            val actualItems = generateSequence { poll(50, TimeUnit.MILLISECONDS) }
                .take(expectedItems.size).toList()
            expectedItems.forEach { expectedItem ->
                when (expectedItem) {
                    is Closed -> assertThat(actualItems, anyElement(isA(has(Closed::status,
                        has(WsStatus::code, equalTo(expectedItem.status.code)) and
                            has(WsStatus::description, equalTo(expectedItem.status.description))
                    ))))
                    else -> assertThat(actualItems, hasElement(expectedItem))
                }
            }
            assertThat(actualItems, hasSize(equalTo(expectedItems.size)))
        }
    }
}

private class FakeServer(
    private val allowConnection: Boolean,
    private val sendConnectionAck: Boolean,
    private val subscriptionAlreadyExists: Boolean
) : PushPullAdaptingWebSocket(Request(Method.GET, "graphql-ws")) {

    private val connected = CountDownLatch(1)
    private val valueToSend = AtomicInteger(0)

    fun awaitConnected() = assertThat("connected", connected.await(100, TimeUnit.MILLISECONDS), equalTo(true))

    fun sendNext(subId: String = "sub-1") =
        send(Next(subId, valueToSend.incrementAndGet().toString()))

    fun sendComplete(subId: String = "sub-1") =
        send(Complete(subId))

    fun sendError(subId: String = "sub-1") =
        send(Error(subId, listOf(mapOf("message" to "boom"))))

    fun send(message: GraphQLWsMessage) = triggerMessage(message)

    fun sendInvalidMessage() = triggerMessage(WsMessage("{}"))

    override fun send(message: WsMessage) {
        when (val graphQLWsMessage = messageLens(message.body)) {
            is ConnectionInit -> {
                if (allowConnection) {
                    if (sendConnectionAck) {
                        connected.countDown()
                        triggerMessage(ConnectionAck(payload = null))
                    }
                } else {
                    triggerClose(WsStatus(4403, "Forbidden"))
                }
            }
            is Subscribe -> {
                if (subscriptionAlreadyExists) {
                    triggerClose(WsStatus(4409, "Subscriber for '${graphQLWsMessage.id}' already exists"))
                }
            }
            is Complete -> {}
            is Ping -> {}
            is Pong -> {}
            is ConnectionAck -> {}
            is Error -> {}
            is Next -> {}
        }
    }

    override fun close(status: WsStatus) {
        triggerClose(status)
    }

    private fun triggerMessage(message: GraphQLWsMessage) {
        triggerMessage(WsMessage(messageLens(message, Body.EMPTY)))
    }

    companion object {
        private val json = Jackson
        private val messageLens = GraphQLWsMessageLens(json)
    }
}

@Suppress("ReactiveStreamsSubscriberImplementation")
private class TestSubscriber(private val errorHandler: (Throwable) -> Unit = { throw it }) : Subscriber<Any> {
    private lateinit var subscription: Subscription

    val values = LinkedBlockingQueue<String>()

    override fun onSubscribe(s: Subscription) {
        subscription = s
        subscription.request(1)
    }

    override fun onNext(t: Any) {
        values.add(t.toString())
        subscription.request(1)
    }

    override fun onError(t: Throwable) {
        errorHandler(t)
    }

    fun cancel() {
        subscription.cancel()
    }

    override fun onComplete() {}
}

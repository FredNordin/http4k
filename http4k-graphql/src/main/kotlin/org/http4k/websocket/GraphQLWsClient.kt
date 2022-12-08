package org.http4k.websocket

import com.fasterxml.jackson.databind.JsonNode
import graphql.GraphQLException
import graphql.execution.reactive.DelegatingSubscription
import graphql.execution.reactive.SingleSubscriberPublisher
import org.http4k.core.Body
import org.http4k.core.Request
import org.http4k.format.AutoMarshallingJson
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
import org.http4k.lens.LensFailure
import org.reactivestreams.Publisher
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.FutureTask
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit

interface GraphQLWsConnection {
    fun newSubscription(id: String, request: GraphQLRequest): Publisher<Any>
    fun disconnect()
}

class GraphQLWsClient(
    private val connectionAckWaitTimeout: Duration = Duration.ofSeconds(3),
    private val pingHandler: (Ping) -> Pong = { Pong(payload = null) },
    private val onEvent: Request.(GraphQLWsEvent) -> Unit = {},
    private val onConnected: (GraphQLWsConnection) -> Unit
) : WsConsumer, AutoCloseable {

    private val json: AutoMarshallingJson<JsonNode> = Jackson

    private val graphqlWsMessageBody = GraphQLWsMessageLens(json)
    private val executor: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()

    override fun invoke(ws: Websocket) {
        val connection = ClientConnection(ws, connectionAckWaitTimeout, onConnected)

        ws.onMessage { wsMessage ->
            wsMessage
                .parseMessage()
                .emitMessageReceivedEvent(ws)
                .handleMessage { message ->
                    when (message) {
                        is ConnectionAck -> connection.onConnected()
                        is Next -> connection.onNext(message)
                        is Complete -> connection.onComplete(message)
                        is Error -> connection.onError(message)
                        is Ping -> ws.send(pingHandler(message))
                        is Pong -> {}
                        is ConnectionInit -> {}
                        is Subscribe -> {}
                    }
                }.onFailure {
                    when (it) {
                        is SubscriptionAlreadyExistsException -> ws.close(subscriberAlreadyExistsStatus(it.id))
                        is LensFailure -> ws.close(badRequestStatus(it))
                        else -> error("Implement failure handling - downstream error: ${it.message}") // TODO
                    }
                }
        }

        ws.onClose { connection.onClose(it) }

        connection.start()
    }

    private fun WsMessage.parseMessage(): Result<GraphQLWsMessage> =
        runCatching { graphqlWsMessageBody(body) }

    private fun Result<GraphQLWsMessage>.emitMessageReceivedEvent(ws: Websocket) =
        onSuccess { onEvent(ws.upgradeRequest, GraphQLWsEvent.MessageReceived(it)) }

    private fun Result<GraphQLWsMessage>.handleMessage(block: (GraphQLWsMessage) -> Unit) =
        mapCatching(block)

    override fun close() {
        executor.shutdown()
    }

    private fun Websocket.send(message: GraphQLWsMessage) {
        send(WsMessage(graphqlWsMessageBody(message, Body.EMPTY)))
        onEvent(upgradeRequest, GraphQLWsEvent.MessageSent(message))
    }

    private inner class ClientConnection(
        private val ws: Websocket,
        private val connectionAckWaitTimeout: Duration,
        private val onConnected: (GraphQLWsConnection) -> Unit
    ) : GraphQLWsConnection {

        private val subscriptions = ConcurrentHashMap<String, SubscriptionPublisher>()
        private val onCloseHandlers = mutableListOf({
            subscriptions.forEach { ( _, subscription) ->
                subscription.noMoreData()
            }
            subscriptions.clear()
        })
        private val connectionAckTimeoutCheck = FutureTask { ws.close(connectionInitTimeoutStatus) }

        fun start() {
            ws.send(ConnectionInit(payload = null))

            executor.schedule(connectionAckTimeoutCheck, connectionAckWaitTimeout.toMillis(), TimeUnit.MILLISECONDS)
            onCloseHandlers.add {
                if (!connectionAckTimeoutCheck.isDone) {
                    connectionAckTimeoutCheck.cancel(false)
                }
            }
        }

        override fun newSubscription(id: String, request: GraphQLRequest): Publisher<Any> {
            val publisher = SubscriptionPublisher(id, request)
            if (subscriptions.putIfAbsent(id, publisher) == null) {
                return publisher
            } else {
                throw SubscriptionAlreadyExistsException(id)
            }
        }

        override fun disconnect() {
            ws.close(WsStatus.NORMAL)
        }

        fun onConnected() {
            onConnected(this)
        }

        fun onNext(message: Next) {
            subscriptions[message.id]?.offer(message.payload)
        }

        fun onComplete(message: Complete) {
            subscriptions[message.id]?.noMoreData()
        }

        fun onError(message: Error) {
            subscriptions[message.id]?.offerError(GraphQLException(message.payload.toString())) // TODO Better exception
        }

        fun onClose(status: WsStatus) {
            onCloseHandlers.forEach { it() }
            onEvent(ws.upgradeRequest, GraphQLWsEvent.Closed(status))
        }

        private inner class SubscriptionPublisher(
            private val id: String, request: GraphQLRequest
        ) : SingleSubscriberPublisher<Any>({ ws.send(Subscribe(id, request)) }) {
            override fun noMoreData() {
                super.noMoreData()
                subscriptions.remove(id)
            }

            override fun offerError(t: Throwable) {
                super.offerError(t)
                subscriptions.remove(id)
            }

            @Suppress("ReactiveStreamsSubscriberImplementation")
            override fun subscribe(subscriber: Subscriber<in Any>) {
                super.subscribe(object : Subscriber<Any> {
                    override fun onSubscribe(s: Subscription) {
                        subscriber.onSubscribe(object : DelegatingSubscription(s) {
                            override fun cancel() {
                                super.cancel()
                                subscriptions.remove(id)?.apply {
                                    ws.send(Complete(id))
                                }
                            }
                        })
                    }
                    override fun onError(t: Throwable) = subscriber.onError(t)
                    override fun onComplete() = subscriber.onComplete()
                    override fun onNext(t: Any?) = subscriber.onNext(t)
                })
            }
        }
    }

    private class SubscriptionAlreadyExistsException(val id: String) : RuntimeException()

    companion object {
        private fun badRequestStatus(e: LensFailure) = WsStatus(4400, e.localizedMessage)
        private val connectionInitTimeoutStatus = WsStatus(4408, "Connection initialisation timeout")
        private fun subscriberAlreadyExistsStatus(id: String) = WsStatus(4409, "Subscriber for '$id' already exists")
    }
}

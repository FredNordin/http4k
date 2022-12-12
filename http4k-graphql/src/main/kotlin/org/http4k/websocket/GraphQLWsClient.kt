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
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.FutureTask
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit

interface GraphQLWsConnection {
    fun subscribe(request: GraphQLRequest, id: String = UUID.randomUUID().toString()): Publisher<Any>
    fun disconnect()
}

class GraphQLWsClientException(message: String, cause: Throwable? = null) : RuntimeException(message, cause)

open class GraphQLWsClient(
    private val connectionAckWaitTimeout: Duration = Duration.ofSeconds(3),
    private val connectionHandler: Request.() -> ConnectionInit = { ConnectionInit(payload = null) },
    private val pingHandler: (Ping) -> Pong = { Pong(payload = null) },
    private val onEvent: Request.(GraphQLWsEvent) -> Unit = {},
    private val onConnected: (GraphQLWsConnection) -> Unit
) : WsConsumer, AutoCloseable {

    private val json: AutoMarshallingJson<JsonNode> = Jackson

    private val graphqlWsMessageBody = GraphQLWsMessageLens(json)
    private val executor: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()

    override fun invoke(ws: Websocket) {
        val connection = ClientConnection(ws)

        ws.onClose {
            connection.onClose(it)
            if (it != WsStatus.NORMAL) {
                throw GraphQLWsClientException("Abnormal close of connection: $it")
            }
        }
        ws.onError { connection.close() }

        ws.onMessage { wsMessage ->
            try {
                val message = graphqlWsMessageBody(wsMessage.body)
                onEvent(ws.upgradeRequest, GraphQLWsEvent.MessageReceived(message))
                connection.handle(message)
            } catch (error: Exception) {
                connection.close()
                throw when (error) {
                    is LensFailure -> GraphQLWsClientException("Invalid graphql-ws message received", error)
                    is GraphQLWsClientException -> throw error
                    else -> GraphQLWsClientException("Error handling graphql-ws message", error)
                }
            }
        }

        connection.start()
    }

    override fun close() {
        executor.shutdown()
    }

    private fun Websocket.send(message: GraphQLWsMessage) {
        send(WsMessage(graphqlWsMessageBody(message, Body.EMPTY)))
        onEvent(upgradeRequest, GraphQLWsEvent.MessageSent(message))
    }

    private inner class ClientConnection(private val ws: Websocket) : GraphQLWsConnection {

        private val subscriptions = ConcurrentHashMap<String, SubscriptionPublisher>()
        private val cleanUpTasks = mutableListOf({
            subscriptions.forEach { ( _, subscription) ->
                subscription.noMoreData()
            }
            subscriptions.clear()
        })
        private val connectionAckTimeoutCheck = FutureTask {
            ws.close(WsStatus(4408, "Connection initialisation timeout"))
        }

        fun start() {
            ws.send(connectionHandler(ws.upgradeRequest))

            executor.schedule(connectionAckTimeoutCheck, connectionAckWaitTimeout.toMillis(), TimeUnit.MILLISECONDS)
            cleanUpTasks.add {
                if (!connectionAckTimeoutCheck.isDone) {
                    connectionAckTimeoutCheck.cancel(false)
                }
            }
        }

        fun handle(message: GraphQLWsMessage) {
            when (message) {
                is ConnectionAck -> {
                    connectionAckTimeoutCheck.cancel(false)
                    onConnected(this)
                }

                is Next -> subscriptions[message.id]?.offer(message.payload)

                is Complete -> subscriptions[message.id]?.noMoreData()

                is Error -> subscriptions[message.id]?.offerError(GraphQLException(message.payload.toString())) // TODO Better error

                is Ping -> ws.send(pingHandler(message))

                is Pong -> {}
                is ConnectionInit -> {}
                is Subscribe -> {}
            }
        }

        override fun subscribe(request: GraphQLRequest, id: String): Publisher<Any> {
            val publisher = SubscriptionPublisher(id, request)
            if (subscriptions.putIfAbsent(id, publisher) == null) {
                return publisher
            } else {
                throw GraphQLWsClientException("Subscriber for '$id' already exists")
            }
        }

        override fun disconnect() {
            ws.close(WsStatus.NORMAL)
        }

        fun onClose(status: WsStatus) {
            close()
            onEvent(ws.upgradeRequest, GraphQLWsEvent.Closed(status))
        }

        fun close() {
            cleanUpTasks.forEach { it() }
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
}

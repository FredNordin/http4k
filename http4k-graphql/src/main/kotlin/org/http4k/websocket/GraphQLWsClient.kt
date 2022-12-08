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
import org.http4k.graphql.ws.GraphQLWsMessage.Subscribe
import org.http4k.lens.GraphQLWsMessageLens
import org.reactivestreams.Publisher
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService

interface GraphQLWsConnection {
    fun newSubscription(id: String, request: GraphQLRequest): Publisher<Any>
}

class GraphQLWsClient(
    private val onEvent: Request.(GraphQLWsEvent) -> Unit = {},
    private val onConnected: (GraphQLWsConnection) -> Unit
) : WsConsumer, AutoCloseable {

    private val json: AutoMarshallingJson<JsonNode> = Jackson

    private val graphqlWsMessageBody = GraphQLWsMessageLens(json)
    private val executor: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()

    override fun invoke(ws: Websocket) {
        val connection = ClientConnection(ws, onConnected)

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
                        else -> error("Implement checks for other message types") // TODO
                    }
                }.onFailure {
                    when (it) {
                        is CloseWsException -> ws.close(it.status)
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
        private val onConnected: (GraphQLWsConnection) -> Unit
    ) : GraphQLWsConnection {

        private val subscriptions = ConcurrentHashMap<String, SubscriptionPublisher>()

        fun start() {
            ws.send(ConnectionInit(payload = null))
        }

        override fun newSubscription(id: String, request: GraphQLRequest): Publisher<Any> {
            val publisher = SubscriptionPublisher(id, request)
            if (subscriptions.putIfAbsent(id, publisher) == null) {
                return publisher
            } else {
                throw CloseWsException(subscriberAlreadyExistsStatus(id))
            }
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
            subscriptions.forEach { ( _, subscription) ->
                subscription.noMoreData()
            }
            subscriptions.clear()
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

    private class CloseWsException(val status: WsStatus) : RuntimeException()

    companion object {
        private fun subscriberAlreadyExistsStatus(id: String) = WsStatus(4409, "Subscriber for '$id' already exists")
    }
}

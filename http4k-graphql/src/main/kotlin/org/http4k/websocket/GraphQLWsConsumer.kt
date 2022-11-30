package org.http4k.websocket

import graphql.ExecutionResult
import org.http4k.format.AutoMarshalling
import org.http4k.format.Jackson
import org.http4k.graphql.GraphQLRequest
import org.http4k.lens.Invalid
import org.http4k.lens.Lens
import org.http4k.lens.LensFailure
import org.http4k.lens.Meta
import org.http4k.lens.Missing
import org.http4k.lens.ParamMeta
import org.http4k.websocket.GraphQLWsMessage.Complete
import org.http4k.websocket.GraphQLWsMessage.ConnectionAck
import org.http4k.websocket.GraphQLWsMessage.ConnectionInit
import org.http4k.websocket.GraphQLWsMessage.Error
import org.http4k.websocket.GraphQLWsMessage.Next
import org.http4k.websocket.GraphQLWsMessage.Ping
import org.http4k.websocket.GraphQLWsMessage.Pong
import org.http4k.websocket.GraphQLWsMessage.Subscribe
import org.reactivestreams.Publisher
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import java.time.Duration
import java.util.concurrent.CompletionStage
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

typealias GraphQLWsRequestExecutor = (GraphQLRequest) -> CompletionStage<ExecutionResult>

class GraphQLWsConsumer(
    private val requestExecutor: GraphQLWsRequestExecutor,
    private val onConnectionInit: (ConnectionInit) -> ConnectionAck? = { ConnectionAck(payload = null) },
    private val onPing: (Ping) -> Pong = { Pong(payload = null) },
    private val onPong: (Pong) -> Unit = {},
    private val connectionInitWaitTimeout: Duration = Duration.ofSeconds(10),
    private val json: AutoMarshalling = Jackson
) : WsConsumer, AutoCloseable {

    private val executor: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()
    private val graphqlWsMessageBody = GraphQLWsMessageLens(json)
    private val subscriptions = ConcurrentHashMap<String, DataSubscriber>()

    override fun invoke(ws: Websocket) {
        val connectionInitTimeoutCheck = ws.scheduleConnectionInitTimeoutCheck()
        val connectionInitReceived = AtomicBoolean(false)

        ws.onMessage { message ->
            try {
                when (val graphQLMessage = graphqlWsMessageBody(message)) {
                    is ConnectionInit -> {
                        if (connectionInitReceived.compareAndSet(false, true)) {
                            connectionInitTimeoutCheck.cancel(false)
                            onConnectionInit(graphQLMessage)?.let { ws.send(it) }
                                ?: ws.close(forbiddenStatus)
                        } else {
                            ws.close(multipleConnectionInitStatus)
                        }
                    }

                    is Ping -> ws.send(onPing(graphQLMessage))

                    is Pong -> onPong(graphQLMessage)

                    is Subscribe -> {
                        if (connectionInitReceived.get()) {
                            val id = graphQLMessage.id
                            val dataSubscriber = DataSubscriber(id, ws)
                            if (subscriptions.putIfAbsent(id, dataSubscriber) == null) {
                                requestExecutor(graphQLMessage.payload)
                                    .thenAccept { result ->
                                        if (result.isDataPresent) {
                                            when (val data = result.getData<Any?>()) {
                                                is Publisher<*> -> data.subscribe(dataSubscriber)
                                                else -> TODO("handle null data or not publisher")
                                            }
                                        } else {
                                            ws.send(Error(id, result.errors.map { it.toSpecification() }))
                                            subscriptions.remove(id)
                                        }
                                    }.exceptionally { error ->
                                        TODO("handle execution error")
                                    }
                            } else {
                                ws.close(subscriberAlreadyExistsStatus(id))
                            }
                        } else {
                            ws.close(unauthorizedStatus)
                        }
                    }

                    else -> {} // Ignore other messages
                }
            } catch (e: LensFailure) {
                ws.close(badRequestStatus(e))
            } catch (e: Exception) {
                ws.close(internalServerErrorStatus)
            }
        }

        ws.onClose {
            if (!connectionInitTimeoutCheck.isDone) {
                connectionInitTimeoutCheck.cancel(false)
            }
        }
    }

    override fun close() {
        executor.shutdown()
    }

    private fun Websocket.scheduleConnectionInitTimeoutCheck() =
        executor.schedule({ close(connectionInitTimeoutStatus) },
            connectionInitWaitTimeout.toMillis(), TimeUnit.MILLISECONDS)

    private fun Websocket.send(message: GraphQLWsMessage): Unit =
        try {
            send(WsMessage(json.asFormatString(message)))
        } catch (e: Exception) {
            close(internalServerErrorStatus)
        }

    @Suppress("ReactiveStreamsSubscriberImplementation")
    private inner class DataSubscriber(private val subscriptionId: String, private val ws: Websocket) : Subscriber<Any>{
        private lateinit var subscription: Subscription

        override fun onSubscribe(sub: Subscription) {
            subscription = sub
            subscription.request(1)
        }

        override fun onNext(next: Any) {
            ws.send(Next(subscriptionId, next))
            subscription.request(1)
        }

        override fun onError(error: Throwable) {
            TODO("Not yet implemented")
        }

        override fun onComplete() {
            ws.send(Complete(subscriptionId))
            subscriptions.remove(subscriptionId)
        }
    }

    companion object {
        private fun badRequestStatus(e: LensFailure) = WsStatus(4400, e.localizedMessage)
        private val unauthorizedStatus = WsStatus(4401, "Unauthorized")
        private val forbiddenStatus = WsStatus(4403, "Forbidden")
        private val connectionInitTimeoutStatus = WsStatus(4408, "Connection initialisation timeout")
        private fun subscriberAlreadyExistsStatus(id: String) = WsStatus(4409, "Subscriber for '$id' already exists")
        private val multipleConnectionInitStatus = WsStatus(4429, "Too many initialisation requests")
        private val internalServerErrorStatus = WsStatus(4500, "Internal server error")
    }
}

private class GraphQLWsMessageLens(private val json: AutoMarshalling) : Lens<WsMessage, GraphQLWsMessage>(
    Meta(true, "body", ParamMeta.ObjectParam, "graphql-ws message"),
    { wsMessage ->
        val body = wsMessage.bodyString()
        when (val type = json.asA<MessageType>(body).type) {
            "connection_init" -> json.asA<ConnectionInit>(body)
            "connection_ack" -> json.asA<ConnectionAck>(body)
            "ping" -> json.asA<Ping>(body)
            "pong" -> json.asA<Pong>(body)
            "subscribe" -> json.asA<Subscribe>(body)
            "next" -> json.asA<Next>(body)
            "error" -> json.asA<Error>(body)
            "complete" -> json.asA<Complete>(body)
            else -> {
                val typeMeta = Meta(true, "graphql-ws message field", ParamMeta.StringParam, "type")
                if (type == null) {
                    throw LensFailure(Missing(typeMeta))
                } else {
                    throw LensFailure(Invalid(typeMeta))
                }
            }
        }
    }
)

private data class MessageType(val type: String?)

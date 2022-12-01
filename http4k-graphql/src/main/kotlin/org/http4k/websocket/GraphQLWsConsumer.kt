package org.http4k.websocket

import graphql.ExecutionResult
import graphql.GraphQLError
import graphql.GraphqlErrorException
import org.http4k.core.Request
import org.http4k.format.AutoMarshalling
import org.http4k.format.Jackson
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

class GraphQLWsConsumer(
    private val onSubscribe: Request.(Subscribe) -> CompletionStage<ExecutionResult>,
    private val onConnect: Request.(ConnectionInit) -> ConnectionAck? = { ConnectionAck(payload = null) },
    private val onPing: Request.(Ping) -> Pong = { Pong(payload = null) },
    private val onPong: Request.(Pong) -> Unit = {},
    private val onClose: Request.(WsStatus) -> Unit = {},
    private val connectionInitWaitTimeout: Duration = Duration.ofSeconds(3)
) : WsConsumer, AutoCloseable {

    private val json: AutoMarshalling = Jackson

    private val executor: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()
    private val graphqlWsMessageBody = GraphQLWsMessageLens(json)

    override fun invoke(ws: Websocket) {
        val connectionInitReceived = AtomicBoolean(false)
        val onCloseTasks = mutableListOf(onClose)
        val subscriptions = ConcurrentHashMap<String, GraphQLWsDataSubscriber>()

        fun sendNext(id: String, payload: Any?) {
            ws.send(Next(id, payload))
        }

        fun sendError(id: String, errors: List<GraphQLError>) {
            subscriptions.remove(id)
            ws.send(Error(id, errors.map { it.toSpecification() }))
        }

        fun sendComplete(id: String) {
            subscriptions.remove(id)
            ws.send(Complete(id))
        }

        fun close(status: WsStatus) {
            subscriptions.forEach { ( _, subscriber) ->
                subscriber.cancel()
            }
            subscriptions.clear()
            ws.close(status)
            onCloseTasks.forEach { it(ws.upgradeRequest, status) }
        }

        val connectionInitTimeoutCheck = executor.schedule(
            { close(connectionInitTimeoutStatus) },
            connectionInitWaitTimeout.toMillis(), TimeUnit.MILLISECONDS
        )
        onCloseTasks.add(0) {
            if (!connectionInitTimeoutCheck.isDone) {
                connectionInitTimeoutCheck.cancel(false)
            }
        }

        ws.onMessage { message ->
            try {
                when (val graphQLMessage = graphqlWsMessageBody(message)) {
                    is ConnectionInit -> {
                        if (connectionInitReceived.compareAndSet(false, true)) {
                            connectionInitTimeoutCheck.cancel(false)
                            onConnect(ws.upgradeRequest, graphQLMessage)?.let { ws.send(it) }
                                ?: close(forbiddenStatus)
                        } else {
                            close(multipleConnectionInitStatus)
                        }
                    }

                    is Ping -> ws.send(onPing(ws.upgradeRequest, graphQLMessage))

                    is Pong -> onPong(ws.upgradeRequest, graphQLMessage)

                    is Subscribe -> {
                        if (connectionInitReceived.get()) {
                            val id = graphQLMessage.id
                            val dataSubscriber = GraphQLWsDataSubscriber(id, ::sendNext, ::sendComplete, ::sendError)
                            if (subscriptions.putIfAbsent(id, dataSubscriber) == null) {
                                onSubscribe(ws.upgradeRequest, graphQLMessage).handle { result, exception: Throwable? ->
                                    if (exception != null) {
                                        sendError(id, listOf(exception.toGraphQLError()))
                                    } else {
                                        if (result.isDataPresent) {
                                            when (val data = result.getData<Any?>()) {
                                                is Publisher<*> -> data.subscribe(dataSubscriber)
                                                else -> TODO("handle null data or not publisher")
                                            }
                                        } else {
                                            sendError(id, result.errors)
                                        }
                                    }
                                }
                            } else {
                                close(subscriberAlreadyExistsStatus(id))
                            }
                        } else {
                            close(unauthorizedStatus)
                        }
                    }

                    is Complete -> {
                        subscriptions.remove(graphQLMessage.id)?.cancel()
                    }

                    is ConnectionAck -> ignored
                    is Next -> ignored
                    is Error -> ignored
                }
            } catch (e: LensFailure) {
                close(badRequestStatus(e))
            } catch (e: Exception) {
                close(internalServerErrorStatus)
            }
        }

        ws.onClose { close(it) }
    }

    override fun close() {
        executor.shutdown()
    }

    private fun Websocket.send(message: GraphQLWsMessage): Unit = send(WsMessage(json.asFormatString(message)))

    companion object {
        private fun badRequestStatus(e: LensFailure) = WsStatus(4400, e.localizedMessage)
        private val unauthorizedStatus = WsStatus(4401, "Unauthorized")
        private val forbiddenStatus = WsStatus(4403, "Forbidden")
        private val connectionInitTimeoutStatus = WsStatus(4408, "Connection initialisation timeout")
        private fun subscriberAlreadyExistsStatus(id: String) = WsStatus(4409, "Subscriber for '$id' already exists")
        private val multipleConnectionInitStatus = WsStatus(4429, "Too many initialisation requests")
        private val internalServerErrorStatus = WsStatus(4500, "Internal server error")

        private val ignored: () -> Unit = {}
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
) {
    private data class MessageType(val type: String?)
}

@Suppress("ReactiveStreamsSubscriberImplementation")
private class GraphQLWsDataSubscriber(
    private val subscriptionId: String,
    private val sendNext: (String, Any?) -> Unit,
    private val sendComplete: (String) -> Unit,
    private val sendError: (String, List<GraphQLError>) -> Unit
) : Subscriber<Any?> {
    private var subscription: Subscription? = null

    override fun onSubscribe(sub: Subscription) {
        subscription = sub
        subscription?.request(1)
    }

    override fun onNext(next: Any?) = doSafely {
        sendNext(subscriptionId, next)
        subscription?.request(1)
    }

    override fun onError(error: Throwable) = doSafely {
        subscription?.cancel()
        sendError(subscriptionId, listOf(error.toGraphQLError()))
    }

    override fun onComplete() = doSafely {
        sendComplete(subscriptionId)
    }

    fun cancel() {
        subscription?.cancel()
    }

    private fun doSafely(block: () -> Unit) {
        try {
            block()
        } catch (e: Throwable) {
            subscription?.cancel()
            throw e
        }
    }
}

private fun Throwable.toGraphQLError(): GraphQLError =
    if (this is GraphQLError) {
        this
    } else {
        GraphqlErrorException.newErrorException()
            .cause(this)
            .message(this.localizedMessage)
            .build()
    }

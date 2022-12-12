package org.http4k.websocket

import com.fasterxml.jackson.databind.JsonNode
import graphql.ExecutionResult
import graphql.GraphQLError
import graphql.GraphqlErrorException
import graphql.execution.reactive.CompletionStageMappingPublisher
import graphql.execution.reactive.SingleSubscriberPublisher
import org.http4k.core.Body
import org.http4k.core.Request
import org.http4k.format.AutoMarshallingJson
import org.http4k.format.Jackson
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
import org.reactivestreams.Subscription as ReactiveSubscription
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.FutureTask
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

class GraphQLWsServer(
    private val connectionInitWaitTimeout: Duration = Duration.ofSeconds(3),
    private val connectionHandler: Request.(ConnectionInit) -> ConnectionAck? = { ConnectionAck(payload = null) },
    private val pingHandler: Request.(Ping) -> Pong = { Pong(payload = null) },
    private val onEvent: Request.(GraphQLWsEvent) -> Unit = {},
    private val subscribeHandler: Request.(Subscribe) -> CompletionStage<ExecutionResult>
) : WsConsumer, AutoCloseable {

    private val json: AutoMarshallingJson<JsonNode> = Jackson

    private val graphqlWsMessageBody = GraphQLWsMessageLens(json)
    private val executor: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()

    override fun invoke(ws: Websocket) {
        val connection = ServerConnection(ws)

        ws.onClose(connection::onClose)
        ws.onError { connection.close() }

        ws.onMessage { wsMessage ->
            try {
                val message = graphqlWsMessageBody(wsMessage.body)
                onEvent(ws.upgradeRequest, GraphQLWsEvent.MessageReceived(message))
                connection.handle(message)
            } catch (error: Exception) {
                ws.close(
                    when (error) {
                        is LensFailure -> badRequestStatus(error)
                        else -> internalServerErrorStatus
                    }
                )
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

    private inner class ServerConnection(private val ws: Websocket) {

        private val subscriptions = ConcurrentHashMap<String, Subscription>()
        private val cleanUpTasks = mutableListOf({
            subscriptions.forEach { (_, subscription) ->
                subscription.cancel()
            }
            subscriptions.clear()
        })

        private val connected = AtomicBoolean(false)
        private val connectionInitTimeoutCheck = FutureTask {
            ws.close(connectionInitTimeoutStatus)
        }

        fun start() {
            executor.schedule(connectionInitTimeoutCheck, connectionInitWaitTimeout.toMillis(), TimeUnit.MILLISECONDS)
            cleanUpTasks.add {
                if (!connectionInitTimeoutCheck.isDone) {
                    connectionInitTimeoutCheck.cancel(false)
                }
            }
        }

        fun onClose(status: WsStatus) {
            close()
            onEvent(ws.upgradeRequest, GraphQLWsEvent.Closed(status))
        }

        fun close() {
            cleanUpTasks.forEach { it() }
        }

        fun handle(message: GraphQLWsMessage) {
            when (message) {
                is ConnectionInit -> {
                    connectionHandler(ws.upgradeRequest, message)
                        ?.let { connectionAck ->
                            if (connected.compareAndSet(false, true)) {
                                connectionInitTimeoutCheck.cancel(false)
                                ws.send(connectionAck)
                            } else {
                                ws.close(multipleConnectionInitStatus)
                            }
                        }
                        ?: ws.close(forbiddenStatus)
                }

                is Ping -> ws.send(pingHandler(ws.upgradeRequest, message))

                is Subscribe -> {
                    if (connected.get()) {
                        val id = message.id

                        newSubscription(id, onExists = { ws.close(subscriberAlreadyExistsStatus(it)) }) { subscriber ->
                            subscribeHandler(ws.upgradeRequest, message).handle { result, exception: Throwable? ->
                                if (exception != null) {
                                    sendError(id, exception)
                                } else {
                                    val publisher: Publisher<ExecutionResult> =
                                        when (val data = result.getData<Any?>()) {
                                            is Publisher<*> -> data.asResultPublisher()
                                            else -> result.asSingleResultPublisher()
                                        }

                                    publisher.subscribe(subscriber)
                                }
                            }
                        }
                    } else {
                        ws.close(unauthorizedStatus)
                    }
                }

                is Complete -> subscriptions.remove(message.id)?.cancel()

                is ConnectionAck -> {}
                is Pong -> {}
                is Next -> {}
                is Error -> {}
            }
        }

        private fun newSubscription(id: String, onExists: (String) -> Unit, block: (Subscription) -> Unit) {
            val subscription = Subscription(id)
            if (subscriptions.putIfAbsent(id, subscription) == null) {
                block(subscription)
            } else {
                onExists(id)
            }
        }

        private fun Publisher<*>.asResultPublisher(): Publisher<ExecutionResult> =
            CompletionStageMappingPublisher(this) {
                val resultFuture = CompletableFuture<ExecutionResult>()
                runCatching { it as ExecutionResult }.fold(
                    resultFuture::complete,
                    resultFuture::completeExceptionally
                )
                resultFuture
            }

        private fun ExecutionResult.asSingleResultPublisher(): Publisher<ExecutionResult> =
            SingleSubscriberPublisher<ExecutionResult>().apply {
                offer(this@asSingleResultPublisher)
                noMoreData()
            }

        private fun sendNext(id: String, payload: Any?) {
            val next = Next(id, payload)
            ws.send(next)
        }

        private fun sendComplete(id: String) {
            subscriptions.remove(id)
            val complete = Complete(id)
            ws.send(complete)
        }

        private fun sendError(id: String, exception: Throwable) =
            sendError(id, listOf(exception.toGraphQLError()))

        private fun sendError(id: String, errors: List<GraphQLError>) {
            subscriptions.remove(id)
            val error = Error(id, errors.map { it.toSpecification() })
            ws.send(error)
        }

        @Suppress("ReactiveStreamsSubscriberImplementation")
        private inner class Subscription(private val id: String) : Subscriber<ExecutionResult> {
            private var subscription: ReactiveSubscription? = null

            override fun onSubscribe(sub: ReactiveSubscription) {
                subscription = sub
                subscription?.request(1)
            }

            override fun onNext(next: ExecutionResult) = doSafely {
                if (next.isDataPresent) {
                    sendNext(id, next.getData())
                    subscription?.request(1)
                } else {
                    sendError(next.errors)
                }
            }

            override fun onError(error: Throwable) {
                sendError(listOf(error.toGraphQLError()))
            }

            private fun sendError(errors: List<GraphQLError>) = doSafely {
                subscription?.cancel()
                sendError(id, errors)
            }

            override fun onComplete() = doSafely {
                sendComplete(id)
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

private fun Throwable.toGraphQLError(): GraphQLError =
    if (this is GraphQLError) {
        this
    } else {
        GraphqlErrorException.newErrorException()
            .cause(this)
            .message(this.localizedMessage)
            .build()
    }


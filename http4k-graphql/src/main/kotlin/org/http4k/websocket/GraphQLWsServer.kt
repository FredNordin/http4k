package org.http4k.websocket

import graphql.ExecutionResult
import graphql.GraphQLError
import org.http4k.core.Request
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
import org.http4k.lens.LensFailure
import org.reactivestreams.Publisher
import java.time.Duration
import java.util.concurrent.CompletionStage
import java.util.concurrent.ScheduledExecutorService

class GraphQLWsServer(
    private val connectionInitWaitTimeout: Duration = Duration.ofSeconds(3),
    private val onConnect: Request.(ConnectionInit) -> ConnectionAck? = { ConnectionAck(payload = null) },
    private val onPing: Request.(Ping) -> Pong = { Pong(payload = null) },
    private val onPong: Request.(Pong) -> Unit = {},
    private val onNext: Request.(Next) -> Unit = {},
    private val onComplete: Request.(Complete) -> Unit = {},
    private val onError: Request.(Error, List<GraphQLError>) -> Unit = { _, _ -> },
    private val onClose: Request.(WsStatus) -> Unit = {},
    private val onSubscribe: Request.(Subscribe) -> CompletionStage<ExecutionResult>
) : GraphQLWsProtocolHandler<GraphQLWsServer.Session>(Jackson), AutoCloseable {

    override fun Websocket.createSession(executor: ScheduledExecutorService, send: (GraphQLWsMessage) -> Unit): Session =
        Session(this, executor, send, connectionInitWaitTimeout, onConnect, onPing, onPong, onSubscribe, onNext, onComplete, onError, onClose)

    override fun Throwable.toStatus(): WsStatus =
        when (this) {
            is LensFailure -> badRequestStatus(this)
            else -> internalServerErrorStatus
        }

    class Session(
        ws: Websocket,
        executor: ScheduledExecutorService,
        send: (GraphQLWsMessage) -> Unit,
        connectionInitWaitTimeout: Duration,
        private val onConnect: Request.(ConnectionInit) -> ConnectionAck?,
        private val onPing: Request.(Ping) -> Pong,
        private val onPong: Request.(Pong) -> Unit,
        private val onSubscribe: Request.(Subscribe) -> CompletionStage<ExecutionResult>,
        onNext: Request.(Next) -> Unit,
        onComplete: Request.(Complete) -> Unit,
        onError: Request.(Error, List<GraphQLError>) -> Unit,
        private val onClose: Request.(WsStatus) -> Unit
    ) : GraphQLWsSession(ws, executor, send) {

        private val connectionInitTimeoutCheck = { close(connectionInitTimeoutStatus) }.scheduleAfter(connectionInitWaitTimeout)

        init {
            onNext(onNext)
            onComplete(onComplete)
            onError(onError)
            onClose {
                if (!connectionInitTimeoutCheck.isDone) {
                    connectionInitTimeoutCheck.cancel(false)
                }
                onClose(originalRequest, it)
            }
        }

        override fun handle(message: GraphQLWsMessage) {
            when (message) {
                is ConnectionInit -> {
                    onConnect(originalRequest, message)
                        ?.let { connectionAck ->
                            if (markAsConnected()) {
                                connectionInitTimeoutCheck.cancel(false)
                                send(connectionAck)
                            } else {
                                close(multipleConnectionInitStatus)
                            }
                        }
                        ?: close(forbiddenStatus)
                }

                is Ping -> send(onPing(originalRequest, message))

                is Pong -> onPong(originalRequest, message)

                is Subscribe -> {
                    if (connected) {
                        val id = message.id

                        newSubscription(id, onExists = { close(subscriberAlreadyExistsStatus(it)) }) { subscriber ->
                            onSubscribe(originalRequest, message).handle { result, exception: Throwable? ->
                                if (exception != null) {
                                    sendError(id, exception)
                                } else {
                                    if (result.isDataPresent) {
                                        when (val data = result.getData<Any?>()) {
                                            is Publisher<*> -> {
                                                data.subscribe(subscriber)
                                            }
                                            else -> {
                                                sendNext(id, data)
                                                sendComplete(id)
                                            }
                                        }
                                    } else {
                                        sendError(id, result.errors)
                                    }
                                }
                            }
                        }
                    } else {
                        close(unauthorizedStatus)
                    }
                }

                is Complete -> {
                    cancelSubscription(message.id)
                }

                is ConnectionAck -> ignored
                is Next -> ignored
                is Error -> ignored
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

        private val ignored: () -> Unit = {}
    }
}

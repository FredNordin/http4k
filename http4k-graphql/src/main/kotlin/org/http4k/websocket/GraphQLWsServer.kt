package org.http4k.websocket

import com.fasterxml.jackson.databind.JsonNode
import graphql.ExecutionResult
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
    private val connectionHandler: Request.(ConnectionInit) -> ConnectionAck? = { ConnectionAck(payload = null) },
    private val pingHandler: Request.(Ping) -> Pong = { Pong(payload = null) },
    onEvent: Request.(GraphQLWsEvent) -> Unit = {},
    private val subscribeHandler: Request.(Subscribe) -> CompletionStage<ExecutionResult>
) : GraphQLWsProtocolHandler<GraphQLWsServer.Session, JsonNode>(Jackson, onEvent), AutoCloseable {

    override fun Websocket.createSession(executor: ScheduledExecutorService,
                                         send: (GraphQLWsMessage) -> Unit,
                                         onEvent: Request.(GraphQLWsEvent) -> Unit): Session =
        Session(
            this,
            executor,
            send,
            onEvent,
            connectionInitWaitTimeout,
            connectionHandler,
            pingHandler,
            subscribeHandler
        )

    override fun Throwable.toStatus(): WsStatus =
        when (this) {
            is LensFailure -> badRequestStatus(this)
            else -> internalServerErrorStatus
        }

    class Session(
        ws: Websocket,
        executor: ScheduledExecutorService,
        send: (GraphQLWsMessage) -> Unit,
        onEvent: Request.(GraphQLWsEvent) -> Unit,
        connectionInitWaitTimeout: Duration,
        private val connectHandler: Request.(ConnectionInit) -> ConnectionAck?,
        private val pingHandler: Request.(Ping) -> Pong,
        private val subscribeHandler: Request.(Subscribe) -> CompletionStage<ExecutionResult>
    ) : GraphQLWsSession(ws, executor, send, onEvent) {

        private val connectionInitTimeoutCheck = { close(connectionInitTimeoutStatus) }.scheduleAfter(connectionInitWaitTimeout)

        override fun handle(message: GraphQLWsMessage) {
            when (message) {
                is ConnectionInit -> {
                    connectHandler(originalRequest, message)
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

                is Ping -> send(pingHandler(originalRequest, message))

                is Subscribe -> {
                    if (connected) {
                        val id = message.id

                        newSubscription(id, onExists = { close(subscriberAlreadyExistsStatus(it)) }) { subscriber ->
                            subscribeHandler(originalRequest, message).handle { result, exception: Throwable? ->
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
                is Pong -> ignored
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

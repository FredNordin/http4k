package org.http4k.websocket

import graphql.GraphQLError
import graphql.GraphqlErrorException
import org.http4k.core.Request
import org.http4k.format.AutoMarshalling
import org.http4k.graphql.ws.GraphQLWsMessage
import org.http4k.graphql.ws.GraphQLWsMessage.Complete
import org.http4k.graphql.ws.GraphQLWsMessage.Error
import org.http4k.graphql.ws.GraphQLWsMessage.Next
import org.http4k.lens.GraphQLWsMessageLens
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import org.reactivestreams.Subscriber as ReactiveSubscriber
import org.reactivestreams.Subscription as ReactiveSubscription

abstract class GraphQLWsProtocolHandler<S : GraphQLWsSession>(
    private val json: AutoMarshalling
) : WsConsumer, AutoCloseable {

    private val graphqlWsMessageBody = GraphQLWsMessageLens(json)
    private val executor: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()

    override fun invoke(ws: Websocket) {
        val session = ws.createSession(executor, send = { ws.send(WsMessage(json.asFormatString(it))) })

        ws.onMessage { wsMessage ->
            runCatching {
                session.handle(graphqlWsMessageBody(wsMessage))
            }.onFailure {
                session.close(it.toStatus())
            }
        }

        ws.onClose(session::close)
    }

    abstract fun Websocket.createSession(executor: ScheduledExecutorService, send: (GraphQLWsMessage) -> Unit): S

    abstract fun Throwable.toStatus(): WsStatus

    override fun close() {
        executor.shutdown()
    }
}

abstract class GraphQLWsSession(private val ws: Websocket, private val executor: ScheduledExecutorService,
                                val send: (GraphQLWsMessage) -> Unit) {
    private val connectedState = AtomicBoolean(false)
    private val subscriptions = ConcurrentHashMap<String, Subscription>()
    private val onNextHandlers = mutableListOf<Request.(Next) -> Unit>()
    private val onCompleteHandlers = mutableListOf<Request.(Complete) -> Unit>()
    private val onErrorHandlers = mutableListOf<Request.(Error, List<GraphQLError>) -> Unit>()
    private val onCloseHandlers = mutableListOf<Request.(WsStatus) -> Unit>()

    abstract fun handle(message: GraphQLWsMessage)

    val connected: Boolean get() = connectedState.get()

    fun markAsConnected(): Boolean = connectedState.compareAndSet(false, true)

    val originalRequest: Request = ws.upgradeRequest

    fun newSubscription(id: String, onExists: (String) -> Unit, block: (Subscription) -> Unit) {
        val subscription = Subscription(id, this)
        if (subscriptions.putIfAbsent(id, subscription) == null) {
            block(subscription)
        } else {
            onExists(id)
        }
    }

    fun cancelSubscription(id: String) {
        subscriptions.remove(id)?.cancel()
    }

    fun sendNext(id: String, payload: Any?) {
        val next = Next(id, payload)
        onNextHandlers.forEach { it(originalRequest, next) }
        send(next)
    }

    fun sendComplete(id: String) {
        subscriptions.remove(id)
        val complete = Complete(id)
        onCompleteHandlers.forEach { it(originalRequest, complete) }
        send(complete)
    }

    fun sendError(id: String, exception: Throwable) = sendError(id, listOf(exception.toGraphQLError()))

    fun sendError(id: String, errors: List<GraphQLError>) {
        subscriptions.remove(id)
        val error = Error(id, errors.map { it.toSpecification() })
        onErrorHandlers.forEach { it(originalRequest, error, errors) }
        send(error)
    }

    fun close(status: WsStatus) {
        subscriptions.forEach { ( _, subscription) ->
            subscription.cancel()
        }
        subscriptions.clear()
        ws.close(status)
        onCloseHandlers.forEach { it(ws.upgradeRequest, status) }
    }

    fun onNext(fn: Request.(Next) -> Unit) {
        onNextHandlers.add(fn)
    }

    fun onComplete(fn: Request.(Complete) -> Unit) {
        onCompleteHandlers.add(fn)
    }

    fun onError(fn: Request.(Error, List<GraphQLError>) -> Unit) {
        onErrorHandlers.add(fn)
    }

    fun onClose(fn: Request.(WsStatus) -> Unit) {
        onCloseHandlers.add(fn)
    }

    fun (() -> Unit).scheduleAfter(duration: Duration): ScheduledFuture<*> =
        executor.schedule(this, duration.toMillis(), TimeUnit.MILLISECONDS)
}

@Suppress("ReactiveStreamsSubscriberImplementation")
class Subscription(private val id: String, private val session: GraphQLWsSession) : ReactiveSubscriber<Any?> {
    private var subscription: ReactiveSubscription? = null

    override fun onSubscribe(sub: ReactiveSubscription) {
        subscription = sub
        subscription?.request(1)
    }

    override fun onNext(next: Any?) = doSafely {
        session.sendNext(id, next)
        subscription?.request(1)
    }

    override fun onError(error: Throwable) = doSafely {
        subscription?.cancel()
        session.sendError(id, listOf(error.toGraphQLError()))
    }

    override fun onComplete() = doSafely {
        session.sendComplete(id)
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

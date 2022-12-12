package org.http4k.websocket

import com.expediagroup.graphql.generator.SchemaGeneratorConfig
import com.expediagroup.graphql.generator.TopLevelObject
import com.expediagroup.graphql.generator.toSchema
import com.natpryce.hamkrest.allOf
import com.natpryce.hamkrest.and
import com.natpryce.hamkrest.anyElement
import com.natpryce.hamkrest.assertion.assertThat
import com.natpryce.hamkrest.equalTo
import com.natpryce.hamkrest.has
import com.natpryce.hamkrest.hasSize
import graphql.GraphQL
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.flow.filterIsInstance
import kotlinx.coroutines.flow.flatMapConcat
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.mapNotNull
import kotlinx.coroutines.flow.single
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.asPublisher
import org.http4k.client.OkHttpWebsocketClient
import org.http4k.core.Uri
import org.http4k.format.Jackson
import org.http4k.graphql.GraphQLRequest
import org.http4k.routing.bind
import org.http4k.routing.websockets
import org.http4k.server.Undertow
import org.http4k.server.asServer
import org.junit.jupiter.api.Test
import org.reactivestreams.Publisher
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit

class GraphQLWsIntegrationTest {

    @Test
    fun `client connects to server and create subscriptions`() {
        val result = CompletableFuture<Pair<List<Widget>, List<WidgetEvent>>>()
        val widgetServer = createGraphql().asGraphQLWsServer()
        val widgetClient = WidgetClient(result)

        websockets("graphql-ws" bind widgetServer).asServer(Undertow(0)).start().use { server ->
            GraphQLWsClient(onConnected = widgetClient).use { graphQLWsClient ->
                OkHttpWebsocketClient.nonBlocking(
                    Uri.of("ws://localhost:${server.port()}/graphql-ws"),
                    onConnect = graphQLWsClient
                )

                assertThat(
                    result.get(5, TimeUnit.SECONDS),
                    allOf(
                        has(
                            Pair<List<Widget>, List<WidgetEvent>>::first,
                            allOf(
                                anyElement(has(Widget::value, equalTo("first"))),
                                anyElement(has(Widget::value, equalTo("second"))),
                                anyElement(has(Widget::value, equalTo("third updated"))),
                                hasSize(equalTo(3))
                            )
                        ),
                        has(
                            Pair<List<Widget>, List<WidgetEvent>>::second,
                            allOf(
                                anyElement(
                                    has(WidgetEvent::type, equalTo(WidgetEvent.Type.Created))
                                        and has(WidgetEvent::widget, has(Widget::value, equalTo("first")))
                                ),
                                anyElement(
                                    has(WidgetEvent::type, equalTo(WidgetEvent.Type.Created))
                                        and has(WidgetEvent::widget, has(Widget::value, equalTo("second")))
                                ),
                                anyElement(
                                    has(WidgetEvent::type, equalTo(WidgetEvent.Type.Created))
                                        and has(WidgetEvent::widget, has(Widget::value, equalTo("third")))
                                ),
                                anyElement(
                                    has(WidgetEvent::type, equalTo(WidgetEvent.Type.Updated))
                                        and has(WidgetEvent::widget, has(Widget::value, equalTo("third updated")))
                                ),
                                hasSize(equalTo(4))
                            )
                        )
                    )
                )
            }
        }
    }
}

private class WidgetClient(private val result: CompletableFuture<Pair<List<Widget>, List<WidgetEvent>>>) : (GraphQLWsConnection) -> Unit,
    CoroutineScope by CoroutineScope(Dispatchers.Default) {

    override fun invoke(connection: GraphQLWsConnection) {
        launch {
            runCatching {
                val events = mutableListOf<WidgetEvent>()
                launch(start = CoroutineStart.UNDISPATCHED) {
                    connection.widgetEvents().toList(events)
                }

                connection.createWidget("first")
                connection.createWidget("second")
                val third = connection.createWidget("third")
                connection.updateWidget(third.id, "third updated")

                val all = connection.allWidgets()

                connection.disconnect()

                all to events

            }.fold(result::complete, result::completeExceptionally)
        }
    }

    private suspend fun GraphQLWsConnection.createWidget(value: String): Widget =
        subscribe(GraphQLRequest("mutation { createWidget(value: \"$value\") { id, value } }"))
            .asFlow()
            .filterIsInstance<Map<String, Map<String, Any>>>()
            .mapNotNull { it["createWidget"] }
            .map<Map<String, Any>, Widget> { Jackson.convert(it) }
            .single()

    private suspend fun GraphQLWsConnection.updateWidget(id: String, value: String): Widget =
        subscribe(GraphQLRequest("mutation { updateWidget(id: \"$id\", value: \"$value\") { id, value } }"))
            .asFlow()
            .filterIsInstance<Map<String, Map<String, Any>>>()
            .mapNotNull { it["updateWidget"] }
            .map<Map<String, Any>, Widget> { Jackson.convert(it) }
            .single()

    @OptIn(FlowPreview::class)
    private suspend fun GraphQLWsConnection.allWidgets(): List<Widget> =
        subscribe(GraphQLRequest("query { allWidgets { id, value } }"))
            .asFlow()
            .filterIsInstance<Map<String, List<Map<String, Any>>>>()
            .mapNotNull { it["allWidgets"] }
            .flatMapConcat { it.asFlow() }
            .map<Map<String, Any>, Widget> { Jackson.convert(it) }
            .toList()

    private fun GraphQLWsConnection.widgetEvents(): Flow<WidgetEvent> =
        subscribe(GraphQLRequest("subscription { widgetEvents { type, widget { id, value } } }"))
            .asFlow()
            .filterIsInstance<Map<String, Map<String, Any>>>()
            .mapNotNull { it["widgetEvents"] }
            .map { Jackson.convert(it) }
}

private fun createGraphql(): GraphQL {
    val store = WidgetStore()
    val schema = toSchema(
        config = SchemaGeneratorConfig(
            supportedPackages = listOf("org.http4k.websocket")
        ),
        queries = listOf(TopLevelObject(WidgetQuery(store))),
        mutations = listOf(TopLevelObject(WidgetMutation(store))),
        subscriptions = listOf(TopLevelObject(WidgetSubscription(store)))
    )
    return GraphQL.newGraphQL(schema)
        .build()
}

private fun GraphQL.asGraphQLWsServer(): GraphQLWsServer = GraphQLWsServer { subscribe ->
    executeAsync { builder ->
        builder
            .query(subscribe.payload.query)
            .operationName(subscribe.payload.operationName)
            .variables(subscribe.payload.variables)
    }
}

data class Widget(val id: String, val value: String)

data class WidgetEvent(val type: Type, val widget: Widget) {
    enum class Type {
        Created,
        Updated
    }
}

class WidgetStore {
    private val store = mutableMapOf<String, Widget>()
    private val eventFlow = MutableSharedFlow<WidgetEvent>(replay = 10)

    fun save(widget: Widget) {
        if (store.containsKey(widget.id)) {
            eventFlow.tryEmit(WidgetEvent(WidgetEvent.Type.Updated, widget))
        } else {
            eventFlow.tryEmit(WidgetEvent(WidgetEvent.Type.Created, widget))
        }
        store[widget.id] = widget
    }

    operator fun get(id: String): Widget? = store[id]

    fun all(): List<Widget> = store.values.toList()

    fun events(): Flow<WidgetEvent> = eventFlow.asSharedFlow()
}


@Suppress("unused")
class WidgetMutation(private val store: WidgetStore) {
    fun createWidget(value: String): Widget = Widget(UUID.randomUUID().toString(), value).also(store::save)
    fun updateWidget(id: String, value: String): Widget? = store[id]?.copy(value = value)?.also(store::save)
}

@Suppress("unused")
class WidgetQuery(private val store: WidgetStore) {
    fun widgetById(id: String): Widget? = store[id]
    fun allWidgets(): List<Widget> = store.all()
}

@Suppress("unused")
class WidgetSubscription(private val store: WidgetStore) {
    fun widgetEvents(): Publisher<WidgetEvent> = store.events().asPublisher()
}

package org.http4k.graphql.ws

import org.http4k.core.Request
import org.http4k.graphql.GraphQLRequest
import org.http4k.websocket.WsStatus

sealed class GraphQLWsMessage(val type: String) {
    data class ConnectionInit(val payload: Map<String, Any>?) : GraphQLWsMessage("connection_init")
    data class ConnectionAck(val payload: Map<String, Any>?) : GraphQLWsMessage("connection_ack")

    data class Ping(val payload: Map<String, Any>?) : GraphQLWsMessage("ping")
    data class Pong(val payload: Map<String, Any>?) : GraphQLWsMessage("pong")

    data class Subscribe(val id: String, val payload: GraphQLRequest) : GraphQLWsMessage("subscribe")
    data class Next(val id: String, val payload: Any?) : GraphQLWsMessage("next")
    data class Error(val id: String, val payload: List<Map<String, Any>>) : GraphQLWsMessage("error")
    data class Complete(val id: String) : GraphQLWsMessage("complete")
}

sealed interface GraphQLWsEvent {
    data class MessageSent(val message: GraphQLWsMessage) : GraphQLWsEvent
    data class MessageReceived(val message: GraphQLWsMessage) : GraphQLWsEvent
    data class Closed(val status: WsStatus) : GraphQLWsEvent
}

typealias GraphQLWsEventHandler = Request.(GraphQLWsEvent) -> Unit

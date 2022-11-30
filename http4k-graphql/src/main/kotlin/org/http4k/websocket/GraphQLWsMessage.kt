package org.http4k.websocket

import org.http4k.graphql.GraphQLRequest

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

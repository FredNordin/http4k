package org.http4k.websocket

import org.http4k.graphql.GraphQLRequest

internal sealed class GraphQLWsMessage(val type: String) {
    internal data class ConnectionInit(val payload: Map<String, Any>?) : GraphQLWsMessage("connection_init")
    internal data class ConnectionAck(val payload: Map<String, Any>?) : GraphQLWsMessage("connection_ack")

    internal data class Ping(val payload: Map<String, Any>?) : GraphQLWsMessage("ping")
    internal data class Pong(val payload: Map<String, Any>?) : GraphQLWsMessage("pong")

    internal data class Subscribe(val id: String, val payload: GraphQLRequest) : GraphQLWsMessage("subscribe")
    internal data class Next(val id: String, val payload: Any?) : GraphQLWsMessage("next")
    internal data class Error(val id: String, val payload: List<Map<String, Any>>) : GraphQLWsMessage("error")
    internal data class Complete(val id: String) : GraphQLWsMessage("complete")
}

package org.http4k.websocket

import org.http4k.graphql.ws.GraphQLWsMessage

sealed interface GraphQLWsEvent {
    data class MessageSent(val message: GraphQLWsMessage) : GraphQLWsEvent
    data class MessageReceived(val message: GraphQLWsMessage) : GraphQLWsEvent
    data class Closed(val status: WsStatus) : GraphQLWsEvent
}

package org.http4k.websocket

import org.http4k.format.AutoMarshalling
import org.http4k.lens.Invalid
import org.http4k.lens.Lens
import org.http4k.lens.LensFailure
import org.http4k.lens.Meta
import org.http4k.lens.Missing
import org.http4k.lens.ParamMeta

internal class GraphQLWsMessageLens(private val json: AutoMarshalling) : Lens<WsMessage, GraphQLWsMessage>(
    Meta(true, "body", ParamMeta.ObjectParam, "graphql-ws message"),
    { wsMessage ->
        val body = wsMessage.bodyString()
        when (val type = json.asA<MessageType>(body).type) {
            "connection_init" -> json.asA<GraphQLWsMessage.ConnectionInit>(body)
            "connection_ack" -> json.asA<GraphQLWsMessage.ConnectionAck>(body)
            "ping" -> json.asA<GraphQLWsMessage.Ping>(body)
            "pong" -> json.asA<GraphQLWsMessage.Pong>(body)
            "subscribe" -> json.asA<GraphQLWsMessage.Subscribe>(body)
            "next" -> json.asA<GraphQLWsMessage.Next>(body)
            "error" -> json.asA<GraphQLWsMessage.Error>(body)
            "complete" -> json.asA<GraphQLWsMessage.Complete>(body)
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

package org.http4k.lens

import org.http4k.asString
import org.http4k.core.Body
import org.http4k.format.AutoMarshalling
import org.http4k.graphql.ws.GraphQLWsMessage
import org.http4k.graphql.ws.GraphQLWsMessage.Complete
import org.http4k.graphql.ws.GraphQLWsMessage.ConnectionAck
import org.http4k.graphql.ws.GraphQLWsMessage.ConnectionInit
import org.http4k.graphql.ws.GraphQLWsMessage.Error
import org.http4k.graphql.ws.GraphQLWsMessage.Next
import org.http4k.graphql.ws.GraphQLWsMessage.Ping
import org.http4k.graphql.ws.GraphQLWsMessage.Pong
import org.http4k.graphql.ws.GraphQLWsMessage.Subscribe

internal object GraphQLWsMessageLens {
    operator fun invoke(json: AutoMarshalling): BiDiLens<Body, GraphQLWsMessage> = BiDiLens(
        Meta(true, "body", ParamMeta.ObjectParam, "graphql-ws message"),
        {
            val body: String = it.payload.asString()
            when (val type = json.asA<MessageType>(body).type) {
                "connection_init" -> json.asA<ConnectionInit>(body)
                "connection_ack" -> json.asA<ConnectionAck>(body)
                "ping" -> json.asA<Ping>(body)
                "pong" -> json.asA<Pong>(body)
                "subscribe" -> json.asA<Subscribe>(body)
                "next" -> json.asA<Next>(body)
                "error" -> json.asA<Error>(body)
                "complete" -> json.asA<Complete>(body)
                else -> {
                    val typeMeta = Meta(true, "graphql-ws message field", ParamMeta.StringParam, "type")
                    if (type == null) {
                        throw LensFailure(Missing(typeMeta))
                    } else {
                        throw LensFailure(Invalid(typeMeta))
                    }
                }
            }
        },
        { message, _ ->
            Body(json.asFormatString(message))
        }
    )

    private data class MessageType(val type: String?)
}

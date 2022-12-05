package org.http4k.lens

import org.http4k.asString
import org.http4k.core.Body
import org.http4k.format.AutoMarshallingJson
import org.http4k.format.JsonType
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
    operator fun <NODE : Any> invoke(json: AutoMarshallingJson<NODE>): BiDiLens<Body, GraphQLWsMessage> = BiDiLens(
        Meta(true, "body", ParamMeta.ObjectParam, "graphql-ws message"),
        {
            val messageJson = json.parse(it.payload.asString())
            require(json.typeOf(messageJson) == JsonType.Object)
            when (val type = json.textValueOf(messageJson, "type")) {
                "connection_init" -> json.asA(messageJson, ConnectionInit::class)
                "connection_ack" -> json.asA(messageJson, ConnectionAck::class)
                "ping" -> json.asA(messageJson, Ping::class)
                "pong" -> json.asA(messageJson, Pong::class)
                "subscribe" -> json.asA(messageJson, Subscribe::class)
                "next" -> json.asA(messageJson, Next::class)
                "error" -> json.asA(messageJson, Error::class)
                "complete" -> json.asA(messageJson, Complete::class)
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
}

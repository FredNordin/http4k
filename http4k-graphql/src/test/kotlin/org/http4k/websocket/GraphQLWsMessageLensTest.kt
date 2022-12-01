package org.http4k.websocket

import com.fasterxml.jackson.databind.JsonNode
import com.natpryce.hamkrest.assertion.assertThat
import com.natpryce.hamkrest.equalTo
import com.natpryce.hamkrest.has
import com.natpryce.hamkrest.throws
import org.http4k.format.Jackson
import org.http4k.graphql.GraphQLRequest
import org.http4k.lens.Invalid
import org.http4k.lens.LensFailure
import org.http4k.lens.Meta
import org.http4k.lens.Missing
import org.http4k.lens.ParamMeta.ObjectParam
import org.http4k.lens.ParamMeta.StringParam
import org.http4k.websocket.GraphQLWsMessage.Complete
import org.http4k.websocket.GraphQLWsMessage.ConnectionAck
import org.http4k.websocket.GraphQLWsMessage.ConnectionInit
import org.http4k.websocket.GraphQLWsMessage.Error
import org.http4k.websocket.GraphQLWsMessage.Next
import org.http4k.websocket.GraphQLWsMessage.Ping
import org.http4k.websocket.GraphQLWsMessage.Pong
import org.http4k.websocket.GraphQLWsMessage.Subscribe
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource

class GraphQLWsMessageLensTest {

    private val lens = GraphQLWsMessageLens(json)

    @ParameterizedTest(name = "{displayName} {0}")
    @MethodSource("allMessageTypes")
    @DisplayName("can create")
    fun `check type can be created`(testData: TestData) {
        assertThat(lens(testData.wsMessage), equalTo(testData.expected))
    }

    @Test
    fun `fails when type is unknown`() {
        val wsMessage = wsMessage { obj("type" to string("garbage")) }

        assertThat({ lens(wsMessage) },
            throws(
                has(
                    LensFailure::failures,
                    equalTo(listOf(Invalid(Meta(true, "graphql-ws message field", StringParam, "type"))))
                )
            )
        )
    }

    @Test
    fun `fails when type is missing`() {
        val wsMessage = wsMessage { obj("not-type" to string("garbage")) }

        assertThat({ lens(wsMessage) },
            throws(
                has(
                    LensFailure::failures,
                    equalTo(listOf(Missing(Meta(true, "graphql-ws message field", StringParam, "type"))))
                )
            )
        )
    }

    @Test
    fun `fails when body is not json`() {
        assertThat({ lens(WsMessage("not json")) },
            throws(
                has(
                    LensFailure::failures,
                    equalTo(listOf(Invalid(Meta(true, "body", ObjectParam, "graphql-ws message"))))
                )
            )
        )
    }

    companion object {
        private val json = Jackson

        private fun wsMessage(creator: Jackson.() -> JsonNode) = WsMessage(json.compact(creator(json)))

        @JvmStatic
        fun allMessageTypes(): List<TestData> = listOf(
            TestData(
                wsMessage {
                    obj("type" to string("connection_init"), "payload" to obj("some" to string("value")))
                },
                ConnectionInit(payload = mapOf("some" to "value"))
            ),
            TestData(
                wsMessage {
                    obj("type" to string("connection_ack"), "payload" to obj("some" to string("value")))
                },
                ConnectionAck(payload = mapOf("some" to "value"))
            ),
            TestData(
                wsMessage {
                    obj("type" to string("ping"), "payload" to obj("some" to string("value")))
                },
                Ping(payload = mapOf("some" to "value"))
            ),
            TestData(
                wsMessage {
                    obj("type" to string("pong"), "payload" to obj("some" to string("value")))
                },
                Pong(payload = mapOf("some" to "value"))
            ),
            TestData(
                wsMessage {
                    obj(
                        "type" to string("subscribe"),
                        "id" to string("an-id"),
                        "payload" to obj("query" to string("a query"))
                    )
                },
                Subscribe(id = "an-id", payload = GraphQLRequest(query = "a query"))
            ),
            TestData(
                wsMessage {
                    obj(
                        "type" to string("next"),
                        "id" to string("an-id"),
                        "payload" to string("result")
                    )
                },
                Next(id = "an-id", payload = "result")
            ),
            TestData(
                wsMessage {
                    obj(
                        "type" to string("error"),
                        "id" to string("an-id"),
                        "payload" to array(obj("message" to string("boom")))
                    )
                },
                Error(id = "an-id", payload = listOf(mapOf("message" to "boom")))
            ),
            TestData(
                wsMessage {
                    obj(
                        "type" to string("complete"),
                        "id" to string("an-id")
                    )
                },
                Complete(id = "an-id")
            )
        )
    }

    class TestData(val wsMessage: WsMessage, val expected: GraphQLWsMessage) {
        override fun toString(): String = expected::class.simpleName.orEmpty()
    }
}

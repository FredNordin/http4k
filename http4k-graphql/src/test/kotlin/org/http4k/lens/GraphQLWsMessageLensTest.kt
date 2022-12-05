package org.http4k.lens

import com.fasterxml.jackson.databind.JsonNode
import com.natpryce.hamkrest.assertion.assertThat
import com.natpryce.hamkrest.equalTo
import com.natpryce.hamkrest.has
import com.natpryce.hamkrest.throws
import org.http4k.core.Body
import org.http4k.format.Jackson
import org.http4k.graphql.GraphQLRequest
import org.http4k.graphql.ws.GraphQLWsMessage
import org.http4k.graphql.ws.GraphQLWsMessage.Complete
import org.http4k.graphql.ws.GraphQLWsMessage.ConnectionAck
import org.http4k.graphql.ws.GraphQLWsMessage.ConnectionInit
import org.http4k.graphql.ws.GraphQLWsMessage.Error
import org.http4k.graphql.ws.GraphQLWsMessage.Next
import org.http4k.graphql.ws.GraphQLWsMessage.Ping
import org.http4k.graphql.ws.GraphQLWsMessage.Pong
import org.http4k.graphql.ws.GraphQLWsMessage.Subscribe
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource

class GraphQLWsMessageLensTest {

    private val lens = GraphQLWsMessageLens(json)

    @ParameterizedTest(name = "{displayName} {0}")
    @MethodSource("allMessageTypes")
    @DisplayName("can create from json")
    fun `check message can be created from json`(testData: TestData) {
        assertThat(lens(testData.body), equalTo(testData.message))
    }

    @ParameterizedTest(name = "{displayName} {0}")
    @MethodSource("allMessageTypes")
    @DisplayName("can create json from")
    fun `check json can be created from message`(testData: TestData) {
        assertThat(lens(testData.message, Body.EMPTY), equalTo(testData.body))
    }

    @Test
    fun `fails creation when type is unknown`() {
        assertThat(
            { lens(body { obj("type" to string("garbage")) }) },
            throws(
                has(
                    LensFailure::failures,
                    equalTo(listOf(Invalid(Meta(true, "graphql-ws message field", ParamMeta.StringParam, "type"))))
                )
            )
        )
    }

    @Test
    fun `fails creation when type is missing`() {
        assertThat(
            { lens(body { obj("not-type" to string("connection_init")) }) },
            throws(
                has(
                    LensFailure::failures,
                    equalTo(listOf(Missing(Meta(true, "graphql-ws message field", ParamMeta.StringParam, "type"))))
                )
            )
        )
    }

    @Test
    fun `fails when body is not json`() {
        assertThat(
            { lens(Body("not json")) },
            throws(
                has(
                    LensFailure::failures,
                    equalTo(listOf(Invalid(Meta(true, "body", ParamMeta.ObjectParam, "graphql-ws message"))))
                )
            )
        )
    }

    companion object {
        private val json = Jackson

        private fun body(creator: Jackson.() -> JsonNode) = Body(json.compact(creator(json)))

        @JvmStatic
        fun allMessageTypes(): List<TestData> = listOf(
            TestData(
                body {
                    obj(
                        "payload" to obj("some" to string("value")),
                        "type" to string("connection_init")
                    )
                },
                ConnectionInit(payload = mapOf("some" to "value"))
            ),
            TestData(
                body {
                    obj(
                        "payload" to obj("some" to string("value")),
                        "type" to string("connection_ack")
                    )
                },
                ConnectionAck(payload = mapOf("some" to "value"))
            ),
            TestData(
                body {
                    obj(
                        "payload" to obj("some" to string("value")),
                        "type" to string("ping")
                    )
                },
                Ping(payload = mapOf("some" to "value"))
            ),
            TestData(
                body {
                    obj(
                        "payload" to obj("some" to string("value")),
                        "type" to string("pong")
                    )
                },
                Pong(payload = mapOf("some" to "value"))
            ),
            TestData(
                body {
                    obj(
                        "id" to string("an-id"),
                        "payload" to obj(
                            "query" to string("a query"),
                            "operationName" to nullNode(),
                            "variables" to obj()
                        ),
                        "type" to string("subscribe")
                    )
                },
                Subscribe(id = "an-id", payload = GraphQLRequest(query = "a query"))
            ),
            TestData(
                body {
                    obj(
                        "id" to string("an-id"),
                        "payload" to string("result"),
                        "type" to string("next")
                    )
                },
                Next(id = "an-id", payload = "result")
            ),
            TestData(
                body {
                    obj(
                        "id" to string("an-id"),
                        "payload" to array(obj("message" to string("boom"))),
                        "type" to string("error")
                    )
                },
                Error(id = "an-id", payload = listOf(mapOf("message" to "boom")))
            ),
            TestData(
                body {
                    obj(
                        "id" to string("an-id"),
                        "type" to string("complete")
                    )
                },
                Complete(id = "an-id")
            )
        )
    }

    class TestData(val body: Body, val message: GraphQLWsMessage) {
        override fun toString(): String = message::class.simpleName.orEmpty()
    }
}

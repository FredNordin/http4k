package org.http4k.lens

import com.natpryce.hamkrest.absent
import com.natpryce.hamkrest.assertion.assertThat
import com.natpryce.hamkrest.equalTo
import com.natpryce.hamkrest.throws
import org.http4k.core.Method.GET
import org.http4k.core.Request
import org.http4k.core.Uri.Companion.of
import org.http4k.core.with
import org.junit.jupiter.api.Test

class QueryTest {
    private val request = "/?hello=world&hello=world2".withQueryOf()

    @Test
    fun `value present`() {
        assertThat(Query.optional("hello")(request), equalTo("world"))
        assertThat(Query.required("hello")(request), equalTo("world"))
        assertThat(Query.map { it.length }.required("hello")(request), equalTo(5))
        assertThat(Query.map { it.length }.optional("hello")(request), equalTo(5))

        val expected: List<String?> = listOf("world", "world2")
        assertThat(Query.multi.required("hello")(request), equalTo(expected))
        assertThat(Query.multi.optional("hello")(request), equalTo(expected))
    }

    @Test
    fun `value missing`() {
        assertThat(Query.optional("world")(request), absent())

        val requiredQuery = Query.required("world")
        assertThat({ requiredQuery(request) }, throws(lensFailureWith<Request>(Missing(requiredQuery.meta), overallType = Failure.Type.Missing)))

        assertThat(Query.multi.optional("world")(request), absent())
        val requiredMultiQuery = Query.multi.required("world")
        assertThat({ requiredMultiQuery(request) }, throws(lensFailureWith<Request>(Missing(requiredMultiQuery.meta), overallType = Failure.Type.Missing)))
    }

    @Test
    fun `value replaced`() {
        val single = Query.required("world")
        assertThat(single("value2", single("value1", request)), equalTo(request.query("world", "value2")))

        val multi = Query.multi.required("world")
        assertThat(multi(listOf("value3", "value4"), multi(listOf("value1", "value2"), request)),
            equalTo(request.query("world", "value3").query("world", "value4")))
    }

    @Test
    fun `invalid value`() {
        val requiredQuery = Query.map(String::toInt).required("hello")
        assertThat({ requiredQuery(request) }, throws(lensFailureWith<Request>(Invalid(requiredQuery.meta), overallType = Failure.Type.Invalid)))

        val optionalQuery = Query.map(String::toInt).optional("hello")
        assertThat({ optionalQuery(request) }, throws(lensFailureWith<Request>(Invalid(optionalQuery.meta), overallType = Failure.Type.Invalid)))

        val requiredMultiQuery = Query.map(String::toInt).multi.required("hello")
        assertThat({ requiredMultiQuery(request) }, throws(lensFailureWith<Request>(Invalid(requiredMultiQuery.meta), overallType = Failure.Type.Invalid)))

        val optionalMultiQuery = Query.map(String::toInt).multi.optional("hello")
        assertThat({ optionalMultiQuery(request) }, throws(lensFailureWith<Request>(Invalid(optionalMultiQuery.meta), overallType = Failure.Type.Invalid)))
    }

    @Test
    fun `sets value on request`() {
        val query = Query.required("bob")
        val withQuery = request.with(query of "hello")
        assertThat(query(withQuery), equalTo("hello"))
    }

    @Test
    fun `can create a custom type and get and set on request`() {
        val custom = Query.map(::MyCustomType) { it.value }.required("bob")

        val instance = MyCustomType("hello world!")
        val reqWithQuery = custom(instance, Request(GET, ""))

        assertThat(reqWithQuery.query("bob"), equalTo("hello world!"))

        assertThat(custom(reqWithQuery), equalTo(MyCustomType("hello world!")))
    }

    private fun String.withQueryOf() = Request(GET, of(this))

    @Test
    fun `toString is ok`() {
        assertThat(Query.required("hello").toString(), equalTo("Required query 'hello'"))
        assertThat(Query.optional("hello").toString(), equalTo("Optional query 'hello'"))
        assertThat(Query.multi.required("hello").toString(), equalTo("Required query 'hello'"))
        assertThat(Query.multi.optional("hello").toString(), equalTo("Optional query 'hello'"))
    }
}
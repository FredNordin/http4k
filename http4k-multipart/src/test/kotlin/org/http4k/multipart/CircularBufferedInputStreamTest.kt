package org.http4k.multipart

import com.natpryce.hamkrest.assertion.assertThat
import com.natpryce.hamkrest.containsSubstring
import com.natpryce.hamkrest.equalTo
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.fail
import java.io.ByteArrayInputStream
import java.io.InputStream
import java.nio.InvalidMarkException

class CircularBufferedInputStreamTest {

    @Test
    fun `returns a byte at a time`() {
        val bytes = "hello my name is Tiest".toByteArray()
        val inputStream = createInputStream(bytes, 3)

        for (b in bytes) {
            val read = inputStream.read()
            assertThat(read, equalTo(b.toInt()))
        }
        assertThat(inputStream.read(), equalTo(-1))
    }

    @Test
    fun `returns bytes`() {
        val bytes = "hello my name is Tiest".toByteArray()
        val inputStream = createInputStream(bytes, 3)

        var buffer = ByteArray(2)
        var read = inputStream.read(buffer, 0, 2)

        assertThat(read, equalTo(2))
        assertThat(buffer[0].toInt().toChar(), equalTo('h'))
        assertThat(buffer[1].toInt().toChar(), equalTo('e'))

        buffer = ByteArray(5)
        read = inputStream.read(buffer, 0, 5)

        assertThat(read, equalTo(5))
        assertThat(buffer[0].toInt().toChar(), equalTo('l'))
        assertThat(buffer[1].toInt().toChar(), equalTo('l'))
        assertThat(buffer[2].toInt().toChar(), equalTo('o'))
        assertThat(buffer[3].toInt().toChar(), equalTo(' '))
        assertThat(buffer[4].toInt().toChar(), equalTo('m'))

        buffer = ByteArray(50)
        read = inputStream.read(buffer, 10, 40)
        assertThat(read, equalTo(15))
        assertThat(buffer[10].toInt().toChar(), equalTo('y'))
        assertThat(buffer[24].toInt().toChar(), equalTo('t'))

        read = inputStream.read(buffer, 10, 40)
        assertThat(read, equalTo(-1))
    }

    @Test
    fun `cannot mark further then buffer size`() {
        val bytes = "hello my name is Tiest".toByteArray()
        val inputStream = createInputStream(bytes, 3)

        try {
            inputStream.mark(5)
            fail("can't have readlimit larger than buffer")
        } catch (e: ArrayIndexOutOfBoundsException) {
            assertThat(e.localizedMessage, containsSubstring("Readlimit (5) cannot be bigger than buffer size (4)"))
        }
    }

    @Test
    fun `marks and resets`() {
        val bytes = "My name is Tiest don't you know".toByteArray()
        val inputStream = createInputStream(bytes, 7)

        inputStream.read() // M
        inputStream.read() // y
        inputStream.read() // ' '

        inputStream.mark(8)

        val marked = inputStream.read() // n
        inputStream.read() // a
        inputStream.read() // m
        inputStream.read() // e

        inputStream.reset()

        val secondMark = inputStream.read() // n
        assertThat(secondMark.toChar(), equalTo(marked.toChar()))

        inputStream.read() // a
        inputStream.read() // m
        inputStream.read() // e

        inputStream.reset()

        assertThat(inputStream.read().toChar(), equalTo(secondMark.toChar()))
        inputStream.read() // a
        inputStream.read() // m
        inputStream.read() // e
        inputStream.read() // ' '
        inputStream.read() // i
        inputStream.read() // s

        inputStream.mark(8)

        val thirdMark = inputStream.read() // ' '
        inputStream.read() // T
        inputStream.read() // i
        inputStream.read() // e

        inputStream.reset()

        assertThat(inputStream.read().toChar(), equalTo(thirdMark.toChar()))
    }

    @Test
    fun `resetting after reading past readlimit fails`() {
        val bytes = "My name is Tiest don't you know".toByteArray()
        val inputStream = createInputStream(bytes, 7)

        inputStream.read() // M
        inputStream.read() // y
        inputStream.read() // ' '

        inputStream.mark(2)

        inputStream.read() // n
        inputStream.read() // a
        inputStream.read() // m
        inputStream.read() // e
        inputStream.read() //
        inputStream.read() // i - reads new values into buffer, reseting the leftBound/mark

        try {
            inputStream.reset()
            fail("Have read past readlimit, should fail")
        } catch (e: InvalidMarkException) {
            assertThat(e.message, equalTo(null))
        }
    }

    @Test
    fun `resetting after reading past readlimit fails 2`() {
        val bytes = "My name is Tiest don't you know".toByteArray()
        val inputStream = createInputStream(bytes, 7)

        inputStream.read() // M
        inputStream.read() // y
        inputStream.read() // ' '
        inputStream.read() // n
        inputStream.read() // a
        inputStream.read() // m

        inputStream.mark(2)
        val marked = inputStream.read() // e - reads new values into buffer, reseting the leftBound/mark
        inputStream.read() // ' '

        inputStream.reset()
        assertThat(inputStream.read().toChar(), equalTo(marked.toChar()))

        inputStream.read() // ' '
        inputStream.read() // i
        inputStream.read() // s
        try {
            inputStream.reset()
            fail("Have read past readlimit, should fail")
        } catch (e: InvalidMarkException) {
            assertThat(e.message, equalTo(null))
        }
    }

    private fun createInputStream(bytes: ByteArray, bufSize: Int): InputStream = CircularBufferedInputStream(ByteArrayInputStream(bytes), bufSize)
    //        return new BufferedInputStream(new ByteArrayInputStream(bytes), bufSize);
}

package org.http4k.lens

import org.http4k.core.Body
import org.http4k.core.ContentType
import org.http4k.core.ContentType.Companion.MULTIPART_FORM_DATA
import org.http4k.core.ContentType.Companion.MultipartFormWithBoundary
import org.http4k.core.HttpMessage
import org.http4k.core.MultipartEntity
import org.http4k.core.MultipartFormBody
import org.http4k.core.MultipartFormBody.Companion.DEFAULT_DISK_THRESHOLD
import org.http4k.core.with
import org.http4k.lens.ContentNegotiation.Companion.Strict
import org.http4k.lens.Header.CONTENT_TYPE
import org.http4k.lens.Validator.Ignore
import org.http4k.multipart.DiskLocation
import java.io.Closeable
import java.util.UUID

data class MultipartForm(
    val fields: Map<String, List<MultipartFormField>> = emptyMap(),
    val files: Map<String, List<MultipartFormFile>> = emptyMap(),
    val errors: List<Failure> = emptyList(),
    val onClose: List<Closeable> = emptyList()
) : Closeable {

    override fun close() {
        files.values.flatten().forEach(MultipartFormFile::close)
        onClose.forEach(Closeable::close)
    }

    operator fun plus(kv: Pair<String, String>) =
        copy(fields = fields + (kv.first to fields.getOrDefault(kv.first, emptyList()) + MultipartFormField(kv.second)))

    @JvmName("plusField")
    operator fun plus(kv: Pair<String, MultipartFormField>) =
        copy(fields = fields + (kv.first to fields.getOrDefault(kv.first, emptyList()) + kv.second))

    @JvmName("plusFile")
    operator fun plus(kv: Pair<String, MultipartFormFile>) =
        copy(files = files + (kv.first to files.getOrDefault(kv.first, emptyList()) + kv.second))

    fun minusField(name: String) = copy(fields = fields - name)
    fun minusFile(name: String) = copy(files = files - name)

    override fun equals(other: Any?): Boolean {
        if (other !is MultipartForm) return false

        if (fields != other.fields) return false
        if (files != other.files) return false
        if (errors != other.errors) return false

        return true
    }

    override fun hashCode(): Int {
        var result = fields.hashCode()
        result = 31 * result + files.hashCode()
        result = 31 * result + errors.hashCode()
        return result
    }
}

val MULTIPART_BOUNDARY = UUID.randomUUID().toString()

/**
 * Convenience function to write the MultipartForm to the message body and set the content type.
 */
fun <R : HttpMessage> R.multipartForm(t: MultipartForm): R = with(Body.multipartForm(Ignore).toLens() of t)

fun Body.Companion.multipartForm(
    validator: Validator,
    vararg parts: Lens<MultipartForm, *>,
    defaultBoundary: String = MULTIPART_BOUNDARY,
    diskThreshold: Int = DEFAULT_DISK_THRESHOLD,
    contentTypeFn: (String) -> ContentType = ::MultipartFormWithBoundary,
    getDiskLocation: () -> DiskLocation = { DiskLocation.Temp() }
): BiDiBodyLensSpec<MultipartForm> =
    BiDiBodyLensSpec(parts.map { it.meta }, MULTIPART_FORM_DATA,
        LensGet { _, target ->
            listOf(MultipartFormBody.from(target, diskThreshold, getDiskLocation()).apply {
                Strict(contentTypeFn(boundary), CONTENT_TYPE(target))
            })
        },
        LensSet { _: String, values: List<Body>, target: HttpMessage ->
            values.fold(target) { a, b ->
                a.body(b)
                    .with(CONTENT_TYPE of contentTypeFn(defaultBoundary))
            }
        })
        .map({ it.toMultipartForm() }, { it.toMultipartFormBody(defaultBoundary) })
        .map({ it.copy(errors = validator(it, parts.toList())) }, { it.copy(errors = validator(it, parts.toList())) })

internal fun Body.toMultipartForm(): MultipartForm = (this as MultipartFormBody).let {
    it.formParts.fold(MultipartForm(onClose = listOf(this))) { memo, next ->
        when (next) {
            is MultipartEntity.File -> memo + (next.name to next.file)
            is MultipartEntity.Field -> memo + (next.name to next.value)
        }
    }
}

internal fun MultipartForm.toMultipartFormBody(boundary: String): MultipartFormBody {
    val withFields = fields.toList()
        .fold(MultipartFormBody(boundary = boundary)) { body, (name, values) ->
            values.fold(body) { bodyMemo, fieldValue ->
                bodyMemo + (name to fieldValue)
            }
        }

    return files.toList()
        .fold(withFields) { body, (name, values) ->
            values.fold(body) { bodyMemo, file ->
                bodyMemo + (name to file)
            }
        }
}

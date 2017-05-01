package org.reekwest.http.contract.util

import argo.jdom.JsonNode
import org.reekwest.http.contract.util.ParamType.BooleanParamType
import org.reekwest.http.contract.util.ParamType.IntegerParamType
import org.reekwest.http.contract.util.ParamType.NumberParamType
import org.reekwest.http.contract.util.ParamType.StringParamType
import org.reekwest.http.formats.Argo
import org.reekwest.http.formats.JsonType

class IllegalSchemaException(message: String) : Exception(message)

data class JsonSchema<out NODE>(val node: NODE, val definitions: Iterable<Pair<String, NODE>>)

private val json = Argo

fun JsonNode.toSchema(): JsonSchema<JsonNode> = toSchema(JsonSchema(this, emptyList()))

private fun toSchema(input: JsonSchema<JsonNode>): JsonSchema<JsonNode> =
    when (json.typeOf(input.node)) {
        JsonType.Object -> objectSchema(input)
        JsonType.Array -> arraySchema(input)
        JsonType.String -> JsonSchema(paramTypeSchema(StringParamType), input.definitions)
        JsonType.Number -> numberSchema(input)
        JsonType.Boolean -> JsonSchema(paramTypeSchema(BooleanParamType), input.definitions)
        JsonType.Null -> throw IllegalSchemaException("Cannot use a null value in a schema!")
        else -> throw IllegalSchemaException("unknown type")
    }

private fun paramTypeSchema(paramType: ParamType): JsonNode = json.obj("type" to json.string(paramType.name))

private fun numberSchema(input: JsonSchema<JsonNode>): JsonSchema<JsonNode> =
    JsonSchema(paramTypeSchema(if (input.node.text.contains(".")) NumberParamType else IntegerParamType), input.definitions)

private fun arraySchema(input: JsonSchema<JsonNode>): JsonSchema<JsonNode> {
    val (node, definitions) = input.node.elements.getOrNull(0)?.let { toSchema(JsonSchema(it, input.definitions)) } ?: throw
    IllegalSchemaException("Cannot use an empty list to generate a schema!")
    return JsonSchema(json.obj("type" to json.string("array"), "items" to node), definitions)
}

private fun objectSchema(input: JsonSchema<JsonNode>): JsonSchema<JsonNode> {
    val (fields, subDefinitions) = input.node.fieldList.fold(listOf<Pair<String, JsonNode>>() to input.definitions, {
        (memoFields, memoDefinitions), nextField ->
        val next = toSchema(JsonSchema(nextField.value, memoDefinitions))
        memoFields.plus(nextField.name.text to next.node) to next.definitions
    })

    val newDefinition = json.obj("type" to json.string("object"), "properties" to json.obj(fields))
    val definitionId = "object" + newDefinition.hashCode()
    val allDefinitions = subDefinitions.plus(definitionId to newDefinition)
    return JsonSchema(json.obj("\$ref" to json.string("#/definitions/$definitionId")), allDefinitions)
}



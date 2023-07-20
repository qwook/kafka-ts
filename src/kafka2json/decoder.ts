import { SmartBuffer } from "smart-buffer"
import { Primitive, Schema } from "."

function decodeVarInt(buffer: SmartBuffer) {
	let value = 0
	let shift = 0
	let byte

	do {
		byte = buffer.readUInt8()
		value |= (byte & 0x7f) << shift
		shift += 7
	} while (byte & 0x80)

	return (value >>> 1) ^ -(value & 1)
}

/**
 * Welcome to the worst part of the codebase. Jk, the encoder is actually the worst part.
 * 
 * @param schema 
 * @param buffer 
 * @param readLength 
 * @returns 
 */
export function decode<T>(
	schema: Schema,
	buffer: Buffer | SmartBuffer,
	readLength: boolean = true
): T {
	const object: {
		[key: string]: any
	} = {}
	let smartBuffer: SmartBuffer
	if (buffer instanceof SmartBuffer) {
		smartBuffer = buffer
	} else {
		smartBuffer = SmartBuffer.fromBuffer(buffer)
	}
	if (readLength) {
		const byteLength = smartBuffer.readInt32BE()
		if (byteLength === 0) {
			return null as T
		}
		smartBuffer = SmartBuffer.fromBuffer(smartBuffer.readBuffer(byteLength))
	}
	for (const schemaValue of schema) {
		const key = Object.keys(schemaValue)[0]
		try {
			switch (schemaValue[key]) {
				case Primitive.Boolean:
					object[key] = smartBuffer.readBuffer(1).at(0) === 0
					break
				case Primitive.Int8:
					object[key] = smartBuffer.readInt8()
					break
				case Primitive.Int16:
					object[key] = smartBuffer.readInt16BE()
					break
				case Primitive.Int32:
					object[key] = smartBuffer.readInt32BE()
					break
				case Primitive.Int64:
					object[key] = smartBuffer.readBigInt64BE()
					break
				case Primitive.VarInt:
				case Primitive.VarLong:
					object[key] = decodeVarInt(smartBuffer)
					break
				case Primitive.String:
					const stringLength = smartBuffer.readInt16BE()
					object[key] = smartBuffer.readString(stringLength)
					break
				case Primitive.NullableString:
					const nullableStringLength = smartBuffer.readInt16BE()
					if (nullableStringLength === -1) {
						object[key] = null
					} else {
						object[key] = smartBuffer.readString(nullableStringLength)
					}
					break
				case Primitive.Bytes:
					const bytesLength = smartBuffer.readInt32BE()
					object[key] = smartBuffer.readBuffer(bytesLength)
					break
				case Primitive.VarIntBytes:
					const varIntBytesLength = decodeVarInt(smartBuffer)
					object[key] = smartBuffer.readBuffer(varIntBytesLength)
					break
				default:
					if (schemaValue[key] instanceof Array) {
						if (schemaValue[key][0] instanceof Array) {
							const schema = (schemaValue[key] as Schema[])[0] as Schema
							const arrayLength = smartBuffer.readInt32BE()
							const array: any[] = []
							for (let i = 0; i < arrayLength; i++) {
								array.push(decode(schema, smartBuffer, false))
							}
							object[key] = array
							break
						} else {
							const schema = schemaValue[key] as Schema
							object[key] = decode(schema, smartBuffer, true)
						}
					}
			}
		} catch (e) {
			console.error("Failed to decode", schemaValue, key)
			console.error(e)
		}
	}
	return object as T
}

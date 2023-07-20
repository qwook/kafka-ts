/**
 * KafkaClient provides a raw TCP connection to Kafka brokers / coordinators.
 */

import net from "node:net"
import { encode } from "./kafka2json/encoder"
import { decode } from "./kafka2json/decoder"
import {
	AllRequests,
	HeaderRequest,
	HeaderResponse,
	HeaderResponseSchema,
	apiMapToEverything,
} from "./kafka2json"

// Todo: This should also handle reconnecting in case of error.
// Todo: Should timeout and disconnect if there is no activity for a while.
export default class KafkaClient {
	private client!: net.Socket
	private clientId: string = ""
	private timeout: number = 0
	private static correlationTracker = 0
	private correlationMap = new Map<
		number,
		{ resolve: (buffer: Buffer) => void; timeout: NodeJS.Timeout }
	>()
	private bytesLeft = 0
	private chunks: Buffer[] = []

	async connect({ host, port }: { host: string; port: number }) {
		const promise = new Promise<void>((resolve) => {
			const client = net.createConnection(
				{
					host,
					port,
				},
				() => {
					console.log(`Connected to server ${host}:${port}`)
					resolve()
				}
			)
			client.on("data", (data) => {
				// Todo: This is not a great way to handle chunking.
				// We should probably use a buffer stream.
				if (this.bytesLeft > 0) {
					this.chunks.push(data)
					this.bytesLeft -= data.byteLength
					if (this.bytesLeft > 0) {
						return
					} else {
						data = Buffer.concat(this.chunks)
					}
				} else {
					const length = data.readInt32BE()
					if (data.byteLength - 4 < length) {
						this.chunks = [data]
						this.bytesLeft = length - (data.byteLength - 4)
						return
					}
				}
				const response = decode<HeaderResponse>(HeaderResponseSchema, data)
				const correlationId = response.correlationId
				const correlation = this.correlationMap.get(correlationId)
				if (correlation) {
					correlation.resolve(data)
					this.correlationMap.delete(correlationId)
				}
			})
			client.on("end", () => {
				console.log("disconnected from server")
			})
			this.client = client
		})
		await promise
	}

	async disconnect() {
		await new Promise<void>((resolve) => {
			this.client.end(() => {
				resolve()
			})
		})
		for (const correlation of this.correlationMap.values()) {
			correlation.resolve(Buffer.from(""))
		}
	}

	async fetch<RequestData extends AllRequests, ResponseData>(
		requestArgs: Omit<RequestData, keyof HeaderRequest>
	) {
		const correlationId = ++KafkaClient.correlationTracker
		const encoded = encode(apiMapToEverything[requestArgs.name].schema, {
			...requestArgs,
			apiKey: apiMapToEverything[requestArgs.name].requestKey,
			correlationId,
			clientId: this.clientId,
		})
		this.client.write(encoded)
		const timeout = setTimeout(() => {
			const correlation = this.correlationMap.get(correlationId)
			if (correlation) {
				correlation.resolve(Buffer.from(""))
				this.correlationMap.delete(correlationId)
			}
			console.log(requestArgs.name)
			console.log("Timed out!")
		}, this.timeout)
		const response = await new Promise<Buffer>((resolve) => {
			this.correlationMap.set(correlationId, {
				resolve: (buffer) => {
					clearTimeout(timeout)
					return resolve(buffer)
				},
				timeout,
			})
		})
		const decoded = decode<ResponseData>(
			apiMapToEverything[requestArgs.name].responseSchema,
			response
		)
		return decoded
	}

	constructor({
		clientId,
		timeout = 3 * 60 * 1000,
	}: {
		clientId: string
		timeout?: number
	}) {
		this.clientId = clientId
		this.timeout = timeout
	}
}

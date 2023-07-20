/**
 * KafkaClientPool keeps a pool of Kafka clients active.
 *
 * A great aspect of Kafka is that it doesn't care about what TCP session you
 * use to talk to Kafka, it just cares about the correlationId that you send.
 *
 * This means that you don't have to have a bunch of TCP sockets open to Kafka,
 * you can just have one and reuse it.
 *
 * Sometimes the Broker is the same exact server as the Coordinator.
 */

import KafkaClient from "./kafkaClient"

export default class KafkaClientPool {
	private clientId: string
	private clients: Map<string, KafkaClient> = new Map()

	constructor({ clientId = "" }: { clientId: string }) {
		this.clientId = clientId
	}

	async connect({ host, port }: { host: string; port: number }) {
		const key = `${host}:${port}`
		const client = this.clients.get(key)
		if (client) {
			return client
		}
		const newClient = new KafkaClient({ clientId: this.clientId })
		await newClient.connect({ host, port })
		this.clients.set(key, newClient)
		return newClient
	}

	async disconnect({ host, port }: { host: string; port: number }) {
		const key = `${host}:${port}`
		const client = this.clients.get(key)
		if (!client) {
			return
		}
		await client.disconnect()
		this.clients.delete(key)
	}

	async cleanup() {
		for (const client of this.clients.values()) {
			await client.disconnect()
		}
	}
}

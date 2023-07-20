import hmr from "node-hmr"
import KafkaConsumerEngine from "./src/kafkaConsumerEngine"
import { setLoopedFunction, looper } from "./src/looper"
import KafkaClientPool from "./src/kafkaClientPool"
import minimist from "minimist"

const argv = minimist(process.argv.slice(2))

const kafkaClientPool = new KafkaClientPool({
	clientId: argv["client-id"],
})


let kafkaConsumerEngine: KafkaConsumerEngine | undefined
let state = {}

// HMR to make development faster and much more fun.
hmr(async () => {
	try {
		console.log("(Re)Starting...")
		const { default: KafkaConsumerEngine } = await import(
			"./src/kafkaConsumerEngine"
		)
		const { allToLeaderAssigner } = await import(
			"./src/assigners/allToLeaderAssigner"
		)

		kafkaClientPool.resetAll()

		kafkaConsumerEngine = new KafkaConsumerEngine({
			clientPool: kafkaClientPool,
			groupId: argv["group-id"],
			topics: [argv["topic"]],
			brokers: ["localhost:9092"],
			state,
			assigner: allToLeaderAssigner,
			recoveryFilePath: argv["recovery-file-path"],
		})
		kafkaConsumerEngine.start()
		setLoopedFunction(async () => await kafkaConsumerEngine?.main())
	} catch (e) {
		console.log(e)
	}
})

looper()

async function cleanup() {
	if (kafkaConsumerEngine) {
		kafkaConsumerEngine.cleanup()
	}
	if (kafkaClientPool) {
		kafkaClientPool.cleanup()
	}
	process.exit()
}

process.on("exit", cleanup)
process.on("SIGINT", cleanup)

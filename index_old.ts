import {
	ApiVersionsRequest,
	ApiVersionsResponse,
	FetchV11Request,
	FetchV11Response,
	FindCoordinatorV1Request,
	FindCoordinatorV1Response,
	HeartBeatV3Request,
	JoinGroupV5Request,
	JoinGroupV5Response,
	ListOffsetsV3Request,
	ListOffsetsV3Response,
	MetadataV7Request,
	MetadataV7Response,
	OffsetFetchV5Request,
	OffsetFetchV5Response,
	SyncGroupV1Request,
	SyncGroupV1Response,
} from "./src/kafka2json"
import { KafkaErrorCode } from "./src/errorCodes"
import heartbeat from "./src/heartbeat"
import KafkaClient from "./src/kafkaClient"

void (async () => {
	const clientId = "local-webhook-relay-worker-client"
	const groupId = "local-webhook-relay-worker-cg"
	const topics = ["hen-confluent-sl-app-event-stream"]

	const broker = new KafkaClient({ clientId })
	await broker.connect({ host: "127.0.0.1", port: 9092 })

	const apiVersions = await broker.fetch<
		ApiVersionsRequest,
		ApiVersionsResponse
	>({
		name: "ApiVersions",
		apiVersion: 1,
	})

	const coordinatorData = await broker.fetch<
		FindCoordinatorV1Request,
		FindCoordinatorV1Response
	>({
		name: "FindCoordinatorV1",
		apiVersion: 1,
		key: groupId,
		keyType: 0,
	})

	if (coordinatorData.errorCode !== 0) {
		throw new Error(`Failed to find coordinator for group ${groupId}`)
	}

	const groupCoordinator = new KafkaClient({ clientId })
	await groupCoordinator.connect({
		host: coordinatorData.host,
		port: coordinatorData.port,
	})

	let memberId = ""
	let groupData: JoinGroupV5Response
	while (true) {
		groupData = await groupCoordinator.fetch<
			JoinGroupV5Request,
			JoinGroupV5Response
		>({
			name: "JoinGroupV5",
			apiVersion: 5,
			groupId,
			sessionTimeoutMs: 30000,
			rebalanceTimeoutMs: 60000,
			memberId,
			groupInstanceId: null,
			protocolType: "consumer",
			protocols: [
				{
					protocolName: "RoundRobinAssigner",
					metadata: {
						version: 0,
						topics: topics.map((topicName) => {
							return { topicName }
						}),
						userData: Buffer.alloc(0),
					},
				},
			],
		})
		if (groupData.errorCode === KafkaErrorCode.MemberIdRequiredCode) {
			memberId = groupData.memberId
		} else if (groupData.errorCode === KafkaErrorCode.None) {
			break
		} else {
			throw new Error(`Failed to join group ${groupId}`)
		}
	}

	const metaData = await broker.fetch<MetadataV7Request, MetadataV7Response>({
		name: "MetadataV7",
		apiVersion: 7,
		topics: topics.map((topicName) => {
			return { topicName }
		}),
		allowAutoTopicCreation: true,
	})
	console.log(JSON.stringify(metaData, null, 2))

	let assignments: SyncGroupV1Request["assignments"] = []
	if (groupData.memberId === groupData.leader) {
		assignments = [
			{
				memberId: groupData.memberId,
				memberAssignment: {
					version: 0,
					partitionAssignment: [
						{
							partitions: metaData.topics[0].partitions.map((partition) => {
								return {
									partitionIndex: partition.partitionIndex,
								}
							}),
							topicName: metaData.topics[0].topicName,
						},
					],
					userData: Buffer.alloc(0),
				},
			},
		]
	}

	const syncGroup = await groupCoordinator.fetch<
		SyncGroupV1Request,
		SyncGroupV1Response
	>({
		name: "SyncGroupV1",
		apiVersion: 1,
		groupId,
		generationId: groupData.generationId,
		memberId: groupData.memberId,
		assignments,
	})
	console.log("sync")
	console.log(JSON.stringify(syncGroup, null, 2))

	let offsetToFetch = BigInt(-1)

	const { resume: resumeHeartbeat, pause: pauseHeartbeat } = heartbeat(
		async () => {
			const heartbeatResponse = await groupCoordinator.fetch<
				HeartBeatV3Request,
				HeartBeatV3Request
			>({
				name: "HeartBeatV3",
				apiVersion: 3,
				groupId,
				generationId: groupData.generationId,
				memberId: groupData.memberId,
				groupInstanceId: null,
			})
			console.log(heartbeatResponse)
		},
		3000
	)

	// OffsetFetch
	const offsetFetch = await groupCoordinator.fetch<
		OffsetFetchV5Request,
		OffsetFetchV5Response
	>({
		name: "OffsetFetchV5",
		apiVersion: 5,
		groupId,
		topics: [
			{
				topicName: metaData.topics[0].topicName,
				partitions: [
					{
						partitionIndex: metaData.topics[0].partitions[0].partitionIndex,
					},
				],
			},
		],
	})
	offsetToFetch = offsetFetch.topics[0].partitions[0].committedOffset

	// fetchTopicOffsets for unresolved partitions
	const listOffsets = await groupCoordinator.fetch<
		ListOffsetsV3Request,
		ListOffsetsV3Response
	>({
		name: "ListOffsetsV3",
		apiVersion: 3,
		replicaId: -1,
		isolationLevel: 1,
		topics: [
			{
				topicName: metaData.topics[0].topicName,
				partitions: [
					{
						partitionIndex: metaData.topics[0].partitions[0].partitionIndex,
						timestamp: BigInt(-1),
					},
				],
			},
		],
	})
	offsetToFetch = listOffsets.topics[0].partitions[0].offset

	while (true) {
		const fetchedData = await groupCoordinator.fetch<
			FetchV11Request,
			FetchV11Response
		>({
			name: "FetchV11",
			apiVersion: 11,
			replicaId: -1,
			maxWaitTime: 5000,
			minBytes: 1,
			maxBytes: 10485760,
			isolationLevel: 1, // READ_COMMITTED
			sessionId: 0,
			sessionEpoch: -1,
			topics: [
				{
					topicName: metaData.topics[0].topicName,
					partitions: [
						{
							partitionIndex: metaData.topics[0].partitions[0].partitionIndex,
							currentLeaderEpoch: -1,
							fetchOffset: offsetToFetch,
							logStartOffset: BigInt(-2),
							partitionMaxBytes: 1048576,
						},
					],
				},
			],
			forgottenTopicsData: [],
			rackId: "",
		})
		offsetToFetch = fetchedData.responses[0].partitions[0].lastStableOffset

		if (fetchedData.responses[0].partitions[0].recordSet) {
			for (const { value } of fetchedData.responses[0].partitions[0].recordSet
				?.records) {
				const event = JSON.parse(value.toString("utf8"))
				console.log({
					id: event.id,
					type: event.type,
				})
			}
		}
	}
})()

// this.consumerGroupId = `${config.serverConfig.env}-${config.clientId}-cg`
// groupId: this.consumerGroupId,
// rebalanceTimeout: 60000,
// sessionTimeout: 45000,
// allowAutoTopicCreation: true,

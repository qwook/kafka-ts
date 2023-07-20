import { group } from "console"
import { KafkaErrorCode } from "./errorCodes"
import {
	FetchV11Request,
	FetchV11Response,
	FindCoordinatorV1Request,
	FindCoordinatorV1Response,
	HeartBeatV3Request,
	HeartBeatV3Response,
	JoinGroupV5Request,
	JoinGroupV5Response,
	LeaveGroupV2Request,
	LeaveGroupV2Response,
	DescribeGroupsV3Request,
	DescribeGroupsV3Response,
	ListOffsetsV3Request,
	ListOffsetsV3Response,
	MetadataV7Request,
	MetadataV7Response,
	OffsetFetchV5Request,
	OffsetFetchV5Response,
	SyncGroupV1Request,
	SyncGroupV1Response,
	OffsetCommitV5Request,
} from "./kafka2json"
import KafkaClient from "./kafkaClient"
import KafkaClientPool from "./kafkaClientPool"
import { AssignerFunction } from "./assigners"
import fs from "fs"
import _ from "lodash"

export default class KakfaConsumerEngine {
	clientPool: KafkaClientPool
	groupId: string
	topics: string[]
	brokers: string[]
	assigner: AssignerFunction
	recoveryFilePath?: string

	// For HMR, this is the state that will be preserved between reloads.
	state: {
		joined?: boolean
		synced?: boolean
		coordinator?: KafkaClient
		joining?: boolean
		group?: {
			memberId: string
			generationId: number
			leader: string
			members: JoinGroupV5Response["members"]
		}
		brokers?: {
			nodeId: number
			host: string
			port: number
			rackId?: string
		}[]
		topics?: MetadataV7Response["topics"]
		assignment?: SyncGroupV1Response["memberAssignment"]
		offsetsToFetch?: OffsetFetchV5Response["topics"]

		// Maybe replace with real offset management later.
		currentOffset?: bigint
	}

	/**
	 * The constructor :)
	 * @param
	 */
	constructor({
		clientPool,
		groupId,
		topics,
		brokers,
		state,
		assigner,
		recoveryFilePath,
	}: {
		clientPool: KafkaClientPool
		groupId: string
		topics: string[]
		brokers: string[]
		state: {}
		assigner: AssignerFunction
		recoveryFilePath?: string
	}) {
		this.clientPool = clientPool
		this.groupId = groupId
		this.topics = topics
		this.brokers = brokers
		this.state = state
		this.assigner = assigner
		this.recoveryFilePath = recoveryFilePath
	}

	/**
	 * Start the consumer engine.
	 */
	async start() {
		try {
			if (!this.state.coordinator) await this.findCoordinator()
			if (!this.state.group) await this.recoverGroup()
			if (!this.state.group) await this.joinGroup()
			if (!this.state.topics || !this.state.brokers)
				await this.refreshMetadata()
			if (!this.state.synced) {
				await this.syncGroup()
			}
			if (!this.state.offsetsToFetch) await this.retrieveOffsetsToFetch()
		} catch (e) {
			console.log(e)
		}
	}

	/**
	 * Main loop.
	 */
	async main() {
		try {
			let heartbeat = await this.heartbeatThrottled()
			if (heartbeat) {
				if (heartbeat.errorCode === KafkaErrorCode.RebalanceInProgressCode) {
					console.log("Rebalance in progress. Rejoining group.")
					this.state.group = undefined
					await this.joinGroup()
					await this.refreshMetadata()
					await this.syncGroup()
					await new Promise((resolve) => setTimeout(resolve, 1000))
					return
				} else if (
					heartbeat.errorCode === KafkaErrorCode.NotCoordinatorForConsumerCode
				) {
					await this.findCoordinator()
				}
			}
		} catch (e) {
			console.log("Error heartbeating! Waiting a bit.")
			await new Promise((resolve) => setTimeout(resolve, 1000))
		}

		try {
			await this.fetchMessages()
			await this.commitOffsets()
		} catch (e) {
			console.log("Error fetching! Waiting a bit.")
			await new Promise((resolve) => setTimeout(resolve, 1000))
		}
	}

	async cleanup() {
		if (!this.recoverGroup && this.state.group) await this.leaveGroup()
	}

	/**
	 * It's like a heartbeat but you can only do it a little bit.
	 */
	private lastHeartbeat = 0
	async heartbeatThrottled() {
		const now = Date.now()
		if (now - this.lastHeartbeat < 1000) {
			return
		}
		this.lastHeartbeat = now
		return await this.heartbeat()
	}

	/**
	 * Used to tell Kafka that we're still alive. Also lets us know if we need to rejoin.
	 * @returns
	 */
	async heartbeat() {
		const { coordinator, group } = this.state

		if (!coordinator || !group) {
			return
		}

		const heartbeatResponse = await coordinator.fetch<
			HeartBeatV3Request,
			HeartBeatV3Response
		>({
			name: "HeartBeatV3",
			apiVersion: 3,
			groupId: this.groupId,
			generationId: group.generationId,
			memberId: group.memberId,
			groupInstanceId: null,
		})

		return heartbeatResponse
	}

	/**
	 * Find the coordinator for the group.
	 */
	async findCoordinator() {
		const broker = await this.findBroker()
		if (!broker) {
			throw new Error("No broker found")
		}

		const coordinatorData = await broker.fetch<
			FindCoordinatorV1Request,
			FindCoordinatorV1Response
		>({
			name: "FindCoordinatorV1",
			apiVersion: 1,
			key: this.groupId,
			keyType: 0,
		})

		if (coordinatorData.errorCode !== 0) {
			throw new Error(`Failed to find coordinator for group ${this.groupId}`)
		}

		this.state.coordinator = await this.clientPool.connect({
			host: coordinatorData.host,
			port: coordinatorData.port,
		})

		console.log("Successfully found coordinator.")
	}

	/**
	 * Finds a broker that we can connect to.
	 * @returns A KafkaClient that is connected to a broker.
	 */
	async findBroker() {
		// Use a broker from metadata.
		if (this.state.brokers) {
			for (const broker of this.state.brokers) {
				const client = await this.clientPool.connect({
					host: broker.host,
					port: broker.port,
				})
				if (client) {
					return client
				}
			}
		}

		// Get brokers from our initial list.
		for (const broker of this.brokers) {
			const client = await this.clientPool.connect({
				host: broker.split(":")[0],
				port: parseInt(broker.split(":")[1]),
			})
			if (client) {
				return client
			}
		}
	}

	/**
	 * Instead of a group join, check to see if we're already a member and recover the
	 * state.
	 */
	async recoverGroup() {
		const { coordinator } = this.state
		if (!coordinator) {
			return
		}

		if (!this.recoveryFilePath || !fs.existsSync(this.recoveryFilePath)) {
			return
		}

		const { recoveryGroup, topics } = JSON.parse(
			await fs.promises.readFile(this.recoveryFilePath, "utf8")
		)
		if (!_.isEqual(_.sortedUniq(topics), _.sortedUniq(this.topics))) {
			console.log("Topics have changed, rejoining.")
			return
		}

		const groupsData = await coordinator.fetch<
			DescribeGroupsV3Request,
			DescribeGroupsV3Response
		>({
			name: "DescribeGroupsV3",
			apiVersion: 3,
			groups: [{ groupId: this.groupId }],
			includeAuthorizedOperations: false,
		})

		const group = groupsData.groups[0]
		if (
			group.members.some(
				(member) => member.memberId === recoveryGroup?.memberId
			)
		) {
			this.state.group = recoveryGroup
			console.log(
				`Recovered group ${this.groupId} as ${recoveryGroup?.memberId}`
			)
		} else {
			console.log("Member no longer exists in group, rejoining.")
		}
	}

	/**
	 * Does a group join.
	 */
	async joinGroup() {
		const { coordinator } = this.state
		if (!coordinator) {
			return
		}

		if (this.state.joining) {
			return
		}

		console.log("Attempting to join.")
		this.state.joining = true

		let memberId = ""
		let groupData: JoinGroupV5Response
		while (true) {
			groupData = await coordinator.fetch<
				JoinGroupV5Request,
				JoinGroupV5Response
			>({
				name: "JoinGroupV5",
				apiVersion: 5,
				groupId: this.groupId,
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
							topics: this.topics.map((topicName) => {
								return { topicName }
							}),
							userData: Buffer.alloc(0),
						},
					},
				],
			})

			if (groupData.errorCode === KafkaErrorCode.MemberIdRequiredCode) {
				memberId = groupData.memberId
			} else if (groupData.errorCode === KafkaErrorCode.UnknownMemberIdCode) {
				console.log("Unknown member, getting a new memberId.")
				memberId = ""
			} else if (groupData.errorCode === KafkaErrorCode.None) {
				break
			} else {
				this.state.joining = false
				throw new Error(
					`Failed to join group ${this.groupId} error code ${groupData.errorCode}`
				)
			}
		}

		console.log(`Joined a group ${this.groupId} as ${groupData.memberId}`)

		this.state.group = {
			generationId: groupData.generationId,
			leader: groupData.leader,
			members: groupData.members,
			memberId: groupData.memberId,
		}
		this.state.joining = false

		if (this.recoveryFilePath) {
			await fs.promises.writeFile(
				this.recoveryFilePath,
				JSON.stringify({
					recoveryGroup: this.state.group,
					topics: this.topics,
				})
			)
		}
	}

	/**
	 * Gets the partitions assigned to this consumer.
	 * If this is the leader, then it will assign partitions to other consumers.
	 */
	async syncGroup() {
		const { group, topics, coordinator } = this.state

		if (!group || !topics || !coordinator) {
			return
		}

		let assignments: SyncGroupV1Request["assignments"] = []
		if (this.isLeader()) {
			assignments = this.assigner(group.leader, group.members, topics)
		}

		const syncGroup = await coordinator.fetch<
			SyncGroupV1Request,
			SyncGroupV1Response
		>({
			name: "SyncGroupV1",
			apiVersion: 1,
			groupId: this.groupId,
			generationId: group.generationId,
			memberId: group.memberId,
			assignments,
		})

		this.state.assignment = syncGroup.memberAssignment
		this.state.synced = true
	}

	async leaveGroup() {
		const { coordinator, group } = this.state

		if (!coordinator || !group) {
			return
		}

		await coordinator.fetch<LeaveGroupV2Request, LeaveGroupV2Response>({
			name: "LeaveGroupV2",
			apiVersion: 2,
			groupId: this.groupId,
			memberId: group.memberId,
		})

		console.log(`Left group ${this.groupId}`)

		this.state.group = undefined
	}

	async refreshMetadata() {
		const broker = await this.findBroker()
		if (!broker) {
			throw new Error("No broker found")
		}

		const metadata = await broker.fetch<MetadataV7Request, MetadataV7Response>({
			name: "MetadataV7",
			apiVersion: 7,
			topics: this.topics.map((topicName) => {
				return { topicName }
			}),
			allowAutoTopicCreation: true,
		})

		this.state.brokers = metadata.brokers
		this.state.topics = metadata.topics
	}

	/**
	 * Gets the offset to fetch.
	 */
	async retrieveOffsetsToFetch() {
		const { coordinator, group, assignment } = this.state

		if (!coordinator || !group || !assignment) {
			return
		}

		assignment.partitionAssignment

		// First, use the Offset API to get the last committed offset.
		const offsetFetch = await coordinator.fetch<
			OffsetFetchV5Request,
			OffsetFetchV5Response
		>({
			name: "OffsetFetchV5",
			apiVersion: 5,
			groupId: this.groupId,
			topics: assignment.partitionAssignment,
		})

		this.state.offsetsToFetch = offsetFetch.topics

		// If there are some offsets uncommitted yet, use ListOffsets to fill in the gaps.
		// This crazy snippet down here figures out all the offsets that have an offset
		// of -1, which is the default value for partitions with uncommitted offsets.
		const topicsWithMissingOffsets: ListOffsetsV3Request["topics"] =
			offsetFetch.topics
				.map((topics) => {
					return {
						topicName: topics.topicName,
						partitions: topics.partitions
							.filter((partition) => {
								return partition.committedOffset === BigInt(-1)
							})
							.map((partition) => {
								return {
									partitionIndex: partition.partitionIndex,
									timestamp: BigInt(-2),
								}
							}),
					}
				})
				.filter((topic) => {
					return topic.partitions.length > 0
				})

		if (topicsWithMissingOffsets.length > 0) {
			const listOffsets = await coordinator.fetch<
				ListOffsetsV3Request,
				ListOffsetsV3Response
			>({
				name: "ListOffsetsV3",
				apiVersion: 3,
				replicaId: -1,
				isolationLevel: 1,
				topics: topicsWithMissingOffsets,
			})
			listOffsets.topics.forEach((topic) => {
				const currentTopic = this.state.offsetsToFetch?.find(
					(offset) => offset.topicName === topic.topicName
				)
				if (currentTopic) {
					topic.partitions.forEach((partition) => {
						const currentPartition = currentTopic.partitions.find(
							(currentPartition) =>
								currentPartition.partitionIndex === partition.partitionIndex
						)
						if (currentPartition) {
							currentPartition.committedOffset = partition.offset
						}
					})
				}
			})
		}

		await new Promise((resolve) => setTimeout(resolve, 1000))
	}

	private lastCommit = 0
	async commitOffsetsThrottled() {
		const now = Date.now()
		if (now - this.lastCommit < 3000) {
			return
		}
		this.lastCommit = now
		return await this.commitOffsets()
	}

	/**
	 * Okay this isn't exactly what the final version of commitOffsets should look
	 * like. What is should look like, is that it takes in a list of offsets to
	 * commit, and then it commits them.
	 *
	 * For now, for everything we fetch, we instantly commit the offset.
	 * @returns
	 */
	async commitOffsets() {
		const { coordinator, group, offsetsToFetch } = this.state

		if (!coordinator || !group || !offsetsToFetch) {
			return
		}

		const commitOffsets = await coordinator.fetch<
			OffsetCommitV5Request,
			OffsetFetchV5Response
		>({
			name: "OffsetCommitV5",
			apiVersion: 5,
			groupId: this.groupId,
			generationId: group.generationId,
			memberId: group.memberId,
			topics: offsetsToFetch.map((topic) => {
				return {
					topicName: topic.topicName,
					partitions: topic.partitions.map((partition) => {
						return {
							partitionIndex: partition.partitionIndex,
							committedOffset: partition.committedOffset,
							committedMetadata: null,
						}
					}),
				}
			}),
		})
	}

	async fetchMessages() {
		const { coordinator, offsetsToFetch } = this.state

		if (!coordinator || !offsetsToFetch) {
			return
		}

		const fetchedData = await coordinator.fetch<
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
			topics: offsetsToFetch.map((topic) => {
				return {
					topicName: topic.topicName,
					partitions: topic.partitions.map((partition) => {
						return {
							partitionIndex: partition.partitionIndex,
							currentLeaderEpoch: -1,
							fetchOffset: partition.committedOffset + BigInt(1),
							logStartOffset: BigInt(-1),
							partitionMaxBytes: 1048576,
						}
					}),
				}
			}),
			forgottenTopicsData: [],
			rackId: "",
		})

		// This big chunk of code allows us to move the commits forward in a dumb
		// way. All of this is to modify this.state.offsetsToFetch.
		// We also commit these fetched offsets in the future.
		for (const topic of fetchedData.responses) {
			for (const partition of topic.partitions) {
				if (!partition.recordSet) {
					return
				}
				const currentTopic = this.state.offsetsToFetch?.find(
					(currentTopic) => currentTopic.topicName === topic.topicName
				)
				if (currentTopic) {
					const currentPartition = currentTopic.partitions.find(
						(currentPartition) =>
							currentPartition.partitionIndex === partition.partitionIndex
					)
					if (currentPartition) {
						currentPartition.committedOffset =
							partition.recordSet.firstOffset +
							BigInt(partition.recordSet.lastOffsetDelta)
					}
				}
			}
		}

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

		return fetchedData
	}

	isLeader() {
		return this.state.group?.memberId === this.state.group?.leader
	}
}

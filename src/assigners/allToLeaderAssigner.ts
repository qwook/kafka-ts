import {
	JoinGroupV5Response,
	MetadataV7Response,
	SyncGroupV1Request,
} from "../kafka2json"

/**
 * This is a simple assigner that assigns all partitions to the leader.
 *
 * @param leader The id of the leader of the group.
 * @param members A list of members and their metadata.
 * @param topics A list of topics and their partitions.
 * @returns The assignment of partitions and topics to members.
 */
export function allToLeaderAssigner(
	leader: string,
	members: JoinGroupV5Response["members"],
	topics: MetadataV7Response["topics"]
): SyncGroupV1Request["assignments"] {
	const assignAllToMe = topics.map((topic) => {
		return {
			partitions: topic.partitions.map((partition) => {
				return {
					partitionIndex: partition.partitionIndex,
				}
			}),
			topicName: topic.topicName,
		}
	})

	return [
		{
			memberId: leader,
			memberAssignment: {
				version: 0,
				partitionAssignment: assignAllToMe,
				userData: Buffer.alloc(0),
			},
		},
	]
}

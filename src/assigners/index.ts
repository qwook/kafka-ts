import {
	JoinGroupV5Response,
	MetadataV7Response,
	SyncGroupV1Request,
} from "../kafka2json"

export type AssignerFunction = (
	leader: string,
	members: JoinGroupV5Response["members"],
	topics: MetadataV7Response["topics"]
) => SyncGroupV1Request["assignments"]

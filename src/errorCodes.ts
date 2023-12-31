export enum KafkaErrorCode {
	Unknown = -1,
	None = 0,
	OffsetOutOfRange = 1,
	CorruptMessage = 2,
	UnknownTopicOrPartition = 3,
	InvalidFetchSize = 4,
	LeaderNotAvailable = 5,
	NotLeaderForPartition = 6,
	RequestTimedOut = 7,
	BrokerNotAvailable = 8,
	ReplicaNotAvailable = 9,
	MessageSizeTooLarge = 10,
	StaleControllerEpochCode = 11,
	OffsetMetadataTooLargeCode = 12,
	OffsetsLoadInProgressCode = 14,
	ConsumerCoordinatorNotAvailableCode = 15,
	NotCoordinatorForConsumerCode = 16,
	InvalidTopicCode = 17,
	RecordListTooLargeCode = 18,
	NotEnoughReplicasCode = 19,
	NotEnoughReplicasAfterAppendCode = 20,
	InvalidRequiredAcksCode = 21,
	IllegalGenerationCode = 22,
	InconsistentGroupProtocolCode = 23,
	InvalidGroupIdCode = 24,
	UnknownMemberIdCode = 25,
	InvalidSessionTimeoutCode = 26,
	RebalanceInProgressCode = 27,
	InvalidCommitOffsetSizeCode = 28,
	TopicAuthorizationFailedCode = 29,
	GroupAuthorizationFailedCode = 30,
	ClusterAuthorizationFailedCode = 31,
	InvalidTimestampCode = 32,
	UnsupportedSaslMechanismCode = 33,
	IllegalSaslStateCode = 34,
	UnsupportedVersionCode = 35,
	TopicAlreadyExistsCode = 36,
	InvalidPartitionsCode = 37,
	InvalidReplicationFactorCode = 38,
	InvalidReplicaAssignmentCode = 39,
	InvalidConfigCode = 40,
	NotControllerCode = 41,
	InvalidRequestCode = 42,
	UnsupportedForMessageFormatCode = 43,
	PolicyViolationCode = 44,
	OutOfOrderSequenceNumberCode = 45,
	DuplicateSequenceNumberCode = 46,
	InvalidProducerEpochCode = 47,
	InvalidTxnStateCode = 48,
	InvalidProducerIdMappingCode = 49,
	InvalidTransactionTimeoutCode = 50,
	ConcurrentTransactionsCode = 51,
	TransactionCoordinatorFencedCode = 52,
	TransactionalIdAuthorizationFailedCode = 53,
	SecurityDisabledCode = 54,
	OperationNotAttemptedCode = 55,
	KafkaStorageError = 56,
	LogDirNotFoundCode = 57,
	SaslAuthenticationFailedCode = 58,
	UnknownProducerIdCode = 59,
	ReassignmentInProgressCode = 60,
	DelegationTokenAuthDisabledCode = 61,
	DelegationTokenNotFoundCode = 62,
	DelegationTokenOwnerMismatchCode = 63,
	DelegationTokenRequestNotAllowedCode = 64,
	DelegationTokenAuthorizationFailedCode = 65,
	DelegationTokenExpiredCode = 66,
	InvalidPrincipalTypeCode = 67,
	NonEmptyGroupCode = 68,
	GroupIdNotFoundCode = 69,
	FetchSessionIdNotFoundCode = 70,
	InvalidFetchSessionEpochCode = 71,
	ListenerNotFoundCode = 72,
	TopicDeletionDisabledCode = 73,
	FencedLeaderEpochCode = 74,
	UnknownLeaderEpochCode = 75,
	UnsupportedCompressionTypeCode = 76,
	StaleBrokerEpochCode = 77,
	OffsetNotAvailableCode = 78,
	MemberIdRequiredCode = 79,
	PreferredLeaderNotAvailableCode = 80,
	GroupMaxSizeReachedCode = 81,
	FencedInstanceIdCode = 82,
	EligibleLeadersNotAvailableCode = 83,
	ElectionNotNeededCode = 84,
	NoReassignmentInProgressCode = 85,
	GroupSubscribedToTopicCode = 86,
	InvalidRecordCode = 87,
	UnstableOffsetCommitCode = 88,
}

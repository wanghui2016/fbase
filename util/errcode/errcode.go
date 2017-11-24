package errcode

const (
	SUCCESS                                                          int32 = 0
	ER_NOT_LEADER                                                          = 1
	ER_SERVER_BUSY                                                         = 2
	ER_SERVER_STOP                                                         = 3
	ER_READ_ONLY                                                           = 4
	ER_ENTITY_NOT_EXIT                                                     = 5
	ER_UNKNOWN                                                             = 6

	// SQL ERROR CODE from 1000
)
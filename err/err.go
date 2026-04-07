package err

import "errors"

var (
	ErrNoNodeWithRequiredEndOfKeyRange = errors.New("no node in storage nodes has key < node.endOfKeyRange")
	ErrRedirectPostKeyValue            = errors.New("err in redirecting post key value")
	ErrReplicatePostKeyValue           = errors.New("err in replicating post key value")
	ErrRequestingReplica               = errors.New("err in requesting replica")
	ErrRedirectGetKeyValue             = errors.New("err in redirecting get key value")
)

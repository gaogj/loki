package memberlist

import "time"

// Values that can be stored into gossiping KV Client must implement this interface.
// It allows merging of different states, obtained via gossiping or CAS function.
type Mergeable interface {
	// Merge with other value in place. Returns change, that can be sent to other clients.
	// If merge doesn't result in any change, returns nil.
	// Error can be returned if merging with given 'other' value is not possible.
	//
	// In order for state merging to work correctly, Merge function must have some properties. When talking about the
	// result of the merge in the following text, we don't mean the return value ("change"), but the
	// end-state of receiver. That means Result of A.Merge(B) is end-state of A.
	//
	// Idempotency:
	// 		Result of applying the same state "B" to state "A" (A.Merge(B)) multiple times has the same effect as
	// 		applying it only once. Only first Merge will return non-empty change.
	//
	// Commutativity:
	//		A.Merge(B) has the same result as B.Merge(A) (result being the end state of A or B respectively)
	//
	// Associativity:
	//		calling B.Merge(C), and then A.Merge(B) has the same result as
	//		calling A.Merge(B) first, and then calling A.Merge(C),
	//		that is, A will end up in the same state in both cases.
	Merge(other Mergeable) (change Mergeable, error error)

	// Describes the content of this mergeable value. Used by memberlist client to decide if
	// one change-value can invalidate some other value, that was received previously. This will be true, only
	// if first value's slice returned from this method covers entire slice returned from the other value.
	MergeContent() []string

	// Remove tombstones older than given limit from this mergeable.
	RemoveTombstones(limit time.Time)
}

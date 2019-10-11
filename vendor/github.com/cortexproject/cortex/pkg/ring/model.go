package ring

import (
	"fmt"
	"sort"
	"time"

	"github.com/golang/protobuf/proto"

	"github.com/cortexproject/cortex/pkg/ring/kv/codec"
	"github.com/cortexproject/cortex/pkg/ring/kv/memberlist"
)

// ByToken is a sortable list of TokenDescs
type ByToken []TokenDesc

func (ts ByToken) Len() int           { return len(ts) }
func (ts ByToken) Swap(i, j int)      { ts[i], ts[j] = ts[j], ts[i] }
func (ts ByToken) Less(i, j int) bool { return ts[i].Token < ts[j].Token }

// ProtoDescFactory makes new Descs
func ProtoDescFactory() proto.Message {
	return NewDesc()
}

// GetCodec returns the codec used to encode and decode data being put by ring.
func GetCodec() codec.Codec {
	return codec.Proto{Factory: ProtoDescFactory}
}

// NewDesc returns an empty ring.Desc
func NewDesc() *Desc {
	return &Desc{
		Ingesters: map[string]IngesterDesc{},
	}
}

// AddIngester adds the given ingester to the ring.
func (d *Desc) AddIngester(id, addr string, tokens []uint32, state IngesterState, normaliseTokens bool) {
	if d.Ingesters == nil {
		d.Ingesters = map[string]IngesterDesc{}
	}

	ingester := IngesterDesc{
		Addr:      addr,
		Timestamp: time.Now().Unix(),
		State:     state,
	}

	if normaliseTokens {
		ingester.Tokens = tokens
	} else {
		for _, token := range tokens {
			d.Tokens = append(d.Tokens, TokenDesc{
				Token:    token,
				Ingester: id,
			})
		}
		sort.Sort(ByToken(d.Tokens))
	}

	d.Ingesters[id] = ingester
}

// RemoveIngester removes the given ingester and all its tokens.
// Deprecated: Use Leave instead. Remove ingester from the ring cannot be gossiped properly.
func (d *Desc) RemoveIngester(id string) {
	delete(d.Ingesters, id)
	output := []TokenDesc{}
	for i := 0; i < len(d.Tokens); i++ {
		if d.Tokens[i].Ingester != id {
			output = append(output, d.Tokens[i])
		}
	}
	d.Tokens = output
}

func (d *Desc) Leave(id string, leaveTime time.Time) {
	ing := d.Ingesters[id]
	ing.State = LEFT
	ing.Timestamp = leaveTime.Unix()
	ing.Tokens = nil

	d.Ingesters[id] = ing

	output := []TokenDesc{}
	for i := 0; i < len(d.Tokens); i++ {
		if d.Tokens[i].Ingester != id {
			output = append(output, d.Tokens[i])
		}
	}
	d.Tokens = output
}

// ClaimTokens transfers all the tokens from one ingester to another,
// returning the claimed token.
func (d *Desc) ClaimTokens(from, to string, normaliseTokens bool) []uint32 {
	var result []uint32

	// If the ingester we are claiming from is normalising, get its tokens then erase them from the ring.
	if fromDesc, found := d.Ingesters[from]; found {
		result = fromDesc.Tokens
		fromDesc.Tokens = nil
		d.Ingesters[from] = fromDesc
	}

	if len(result) > 0 && !normaliseTokens {
		// claiming tokens from normalized ingester to unnormalized. We need to add them to d.Tokens
		for _, t := range result {
			d.Tokens = append(d.Tokens, TokenDesc{
				Token:    t,
				Ingester: to,
			})
		}

		sort.Sort(ByToken(d.Tokens))
	}

	// move tokens in non-normalized list
	for i := 0; i < len(d.Tokens); {
		if d.Tokens[i].Ingester == from {
			result = append(result, d.Tokens[i].Token)

			if normaliseTokens {
				d.Tokens = append(d.Tokens[:i], d.Tokens[i+1:]...)
				continue
			} else {
				d.Tokens[i].Ingester = to
			}
		}
		i++
	}

	// let's get rid of duplicates in global tokens
	if len(d.Tokens) > 0 {
		prev := d.Tokens[0]
		for i := 1; i < len(d.Tokens); {
			td := d.Tokens[i]
			if td.Equal(prev) {
				// get rid of duplicates. may happen if 'from' ingester had
				// tokens both in its descriptor and in global token list
				d.Tokens = append(d.Tokens[:i], d.Tokens[i+1:]...)
				continue
			}
			prev = td
			i++
		}
	}

	// Simplifies tests (assert.Equal sees empty and nil slice as different)
	if len(d.Tokens) == 0 {
		d.Tokens = nil
	}

	// sort result, and remove duplicates
	sort.Sort(uint32s(result))
	if len(result) > 0 {
		prev := result[0]
		for i := 1; i < len(result); {
			t := result[i]
			if t == prev {
				result = append(result[:i], result[i+1:]...)
				continue
			}
			prev = t
			i++
		}
	}

	if normaliseTokens {
		ing := d.Ingesters[to]
		ing.Tokens = result
		d.Ingesters[to] = ing
	}

	return result
}

// FindIngestersByState returns the list of ingesters in the given state
func (d *Desc) FindIngestersByState(state IngesterState) []IngesterDesc {
	var result []IngesterDesc
	for _, ing := range d.Ingesters {
		if ing.State == state {
			result = append(result, ing)
		}
	}
	return result
}

// Ready returns no error when all ingesters are active and healthy.
func (d *Desc) Ready(heartbeatTimeout time.Duration) error {
	numTokens := len(d.Tokens)
	for id, ingester := range d.Ingesters {
		if time.Now().Sub(time.Unix(ingester.Timestamp, 0)) > heartbeatTimeout {
			return fmt.Errorf("ingester %s past heartbeat timeout", id)
		} else if ingester.State != ACTIVE {
			return fmt.Errorf("ingester %s in state %v", id, ingester.State)
		}
		numTokens += len(ingester.Tokens)
	}

	if numTokens == 0 {
		return fmt.Errorf("Not ready: no tokens in ring")
	}
	return nil
}

// TokensFor partitions the tokens into those for the given ID, and those for others.
func (d *Desc) TokensFor(id string) (tokens, other []uint32) {
	var takenTokens, myTokens []uint32
	for _, token := range migrateRing(d) {
		takenTokens = append(takenTokens, token.Token)
		if token.Ingester == id {
			myTokens = append(myTokens, token.Token)
		}
	}
	return myTokens, takenTokens
}

// Merges other ring into this one. Returns sub-ring that represents the change,
// and can be sent out to other clients.
//
// This merge function prefers tokens from more-recent update for each ingester.
// It also does token conflict resolution.
func (d *Desc) Merge(mergeable memberlist.Mergeable) (memberlist.Mergeable, error) {
	if mergeable == nil {
		return nil, nil
	}

	other, ok := mergeable.(*Desc)
	if !ok {
		// This method only deals with non-nil rings.
		return nil, fmt.Errorf("expected *ring.Desc, got %T", mergeable)
	}

	if other == nil {
		return nil, nil
	}

	thisIngesterMap := buildNormalizedIngestersMap(d)
	otherIngesterMap := buildNormalizedIngestersMap(other)

	var updated []string

	for name, oing := range otherIngesterMap {
		ting := thisIngesterMap[name]
		// firstIng.Timestamp will be 0, if there was no such ingester in 'first' version
		if oing.Timestamp > ting.Timestamp {
			oing.Tokens = append([]uint32(nil), oing.Tokens...) // make a copy of tokens
			thisIngesterMap[name] = oing
			updated = append(updated, name)
		}
	}

	// No updated ingesters
	if len(updated) == 0 {
		return nil, nil
	}

	// resolveConflicts allocates lot of memory, so if we can avoid it, do that.
	if conflictingTokensExist(thisIngesterMap) {
		resolveConflicts(thisIngesterMap)
	}

	// Before we do de-normalization (if needed), let's build a "change" for returning
	out := NewDesc()
	for _, u := range updated {
		ing := thisIngesterMap[u]
		out.Ingesters[u] = ing
	}

	d.Ingesters = thisIngesterMap

	emptyIngester := IngesterDesc{}

	if len(d.Tokens) > 0 {
		// let's do denormalization :-(
		d.Tokens = nil
		d.Tokens = migrateRing(d)

		// and remove tokens from ingesters to avoid confusion
		for n, ing := range d.Ingesters {
			ing.Tokens = nil

			// remove empty ingesters
			if emptyIngester.Equal(ing) {
				delete(d.Ingesters, n)
			} else {
				d.Ingesters[n] = ing
			}
		}
	}

	return out, nil
}

// MergeContent describes content of this Mergeable.
// Ring simply returns list of ingesters that it includes.
func (d *Desc) MergeContent() []string {
	result := []string(nil)
	for k, _ := range d.Ingesters {
		result = append(result, k)
	}
	return result
}

// buildNormalizedIngestersMap will do the following:
// - moves all tokens from r.Tokens into individual ingesters
// - sorts tokens and removes duplicates (only within single ingester)
// - it doesn't modify input ring
func buildNormalizedIngestersMap(inputRing *Desc) map[string]IngesterDesc {
	out := map[string]IngesterDesc{}

	// Make sure LEFT ingesters have no tokens
	for n, ing := range inputRing.Ingesters {
		if ing.State == LEFT {
			ing.Tokens = nil
		}
		out[n] = ing
	}

	for _, t := range inputRing.Tokens {
		// if ingester doesn't exist, we will add empty one (with tokens only)
		ing := out[t.Ingester]

		// don't add tokens to the LEFT ingesters. We skip such tokens.
		if ing.State != LEFT {
			ing.Tokens = append(ing.Tokens, t.Token)
			out[t.Ingester] = ing
		}
	}

	// Sort tokens, and remove duplicates
	for name, ing := range out {
		if ing.Tokens == nil {
			continue
		}

		if !sort.IsSorted(sortableUint32(ing.Tokens)) {
			sort.Sort(sortableUint32(ing.Tokens))
		}

		seen := make(map[uint32]bool)

		n := 0
		for _, v := range ing.Tokens {
			if !seen[v] {
				seen[v] = true
				ing.Tokens[n] = v
				n++
			}
		}
		ing.Tokens = ing.Tokens[:n]

		// write updated value back to map
		out[name] = ing
	}

	return out
}

func conflictingTokensExist(normalizedIngesters map[string]IngesterDesc) bool {
	count := 0
	for _, ing := range normalizedIngesters {
		count += len(ing.Tokens)
	}

	tokensMap := make(map[uint32]bool, count)
	for _, ing := range normalizedIngesters {
		for _, t := range ing.Tokens {
			if tokensMap[t] {
				return true
			}
			tokensMap[t] = true
		}
	}
	return false
}

// This function resolves token conflicts, if there are any.
//
// We deal with two possibilities:
// 1) if one node is LEAVING or LEFT and the other node is not, LEVING/LEFT one loses the token
// 2) otherwise node names are compared, and node with "lower" name wins the token
//
// Modifies ingesters map with updated tokens.
func resolveConflicts(normalizedIngesters map[string]IngesterDesc) {
	size := 0
	for _, ing := range normalizedIngesters {
		size += len(ing.Tokens)
	}
	tokens := make([]uint32, 0, size)
	tokenToIngester := make(map[uint32]string, size)

	for ingKey, ing := range normalizedIngesters {
		if ing.State == LEFT {
			// LEFT ingesters don't use tokens anymore
			continue
		}

		for _, token := range ing.Tokens {
			prevKey, found := tokenToIngester[token]
			if !found {
				tokens = append(tokens, token)
				tokenToIngester[token] = ingKey
			} else {
				// there is already ingester for this token, let's do conflict resolution
				prevIng := normalizedIngesters[prevKey]

				winnerKey := ingKey
				switch {
				case ing.State == LEAVING && prevIng.State != LEAVING:
					winnerKey = prevKey
				case prevIng.State == LEAVING && ing.State != LEAVING:
					winnerKey = ingKey
				case ingKey < prevKey:
					winnerKey = ingKey
				case prevKey < ingKey:
					winnerKey = prevKey
				}

				tokenToIngester[token] = winnerKey
			}
		}
	}

	sort.Sort(sortableUint32(tokens))

	// let's store the resolved result back
	newTokenLists := map[string][]uint32{}
	for key, _ := range normalizedIngesters {
		// make sure that all ingesters start with empty list
		// especially ones that will no longer have any tokens
		newTokenLists[key] = []uint32{}
	}

	// build list of tokens for each ingester
	for _, token := range tokens {
		key := tokenToIngester[token]
		newTokenLists[key] = append(newTokenLists[key], token)
	}

	// write tokens back
	for key, tokens := range newTokenLists {
		ing := normalizedIngesters[key]
		ing.Tokens = tokens
		normalizedIngesters[key] = ing
	}
}

// Removes LEFT ingesters older than given limit.
func (d *Desc) removeLeftIngesters(limit time.Time) int {
	removed := 0
	for n, ing := range d.Ingesters {
		if ing.State == LEFT && time.Unix(ing.Timestamp, 0).Before(limit) {
			// remove it
			delete(d.Ingesters, n)
			removed++
		}
	}
	return removed
}

func (d *Desc) RemoveTombstones(limit time.Time) {
	d.removeLeftIngesters(limit)
}

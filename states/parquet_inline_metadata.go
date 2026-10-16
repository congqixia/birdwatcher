package states

import (
	"bytes"
	"context"
	"fmt"
	"sort"
)

// Exact comparison with the original footer ColumnChunk bytes proves a duplicate
// without relying on size similarity, reserialization, or writer identification.
type parquetInlineCandidate struct {
	offset                 int64
	footerStart, footerEnd int
}
type parquetInlineVerification struct {
	Candidates                 int     `json:"candidates"`
	Matched                    int     `json:"matched"`
	Unmatched                  int     `json:"unmatched"`
	ReadErrors                 int     `json:"read_errors"`
	OriginalUnclassifiedBytes  int64   `json:"original_unclassified_bytes"`
	VerifiedBytes              int64   `json:"verified_bytes"`
	RemainingUnclassifiedBytes int64   `json:"remaining_unclassified_bytes"`
	VerifiedPercent            float64 `json:"verified_percent"`
}

func verifyParquetInlineMetadata(ctx context.Context, reader v3RangeReader, key string, size int64, footer []byte, intervals []parquetInterval, candidates []parquetInlineCandidate) ([]parquetInterval, *parquetInlineVerification, error) {
	// Validate and snapshot occupied ranges before probing. Every probe is wholly
	// inside an unclassified gap, never inside data pages, indexes or the footer.
	parts, err := accountParquetIntervals(size, intervals)
	if err != nil {
		return nil, nil, err
	}
	v := &parquetInlineVerification{Candidates: len(candidates)}
	for _, part := range parts {
		if part.Name == "unclassified" {
			v.OriginalUnclassifiedBytes = part.Bytes
		}
	}
	occupied := append([]parquetInterval(nil), intervals...)
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].offset < candidates[j].offset })
	var acceptedEnd int64
	for _, candidate := range candidates {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		length := int64(candidate.footerEnd - candidate.footerStart)
		if length <= 0 || candidate.footerStart < 0 || candidate.footerEnd > len(footer) {
			return nil, nil, fmt.Errorf("invalid footer ColumnChunk span")
		}
		offset := candidate.offset
		next := sort.Search(len(occupied), func(i int) bool { return occupied[i].end > offset })
		// If an occupied interval contains offset, or the entire expected duplicate
		// cannot fit before the next occupied interval, no safe match is possible.
		if offset < 4 || offset < acceptedEnd || next == len(occupied) || offset >= occupied[next].start || length > occupied[next].start-offset {
			v.Unmatched++
			continue
		}
		raw, err := reader.ReadRange(ctx, key, offset, length)
		if err != nil {
			if ctx.Err() != nil {
				return nil, nil, ctx.Err()
			}
			v.ReadErrors++
			continue
		}
		if !bytes.Equal(raw, footer[candidate.footerStart:candidate.footerEnd]) {
			v.Unmatched++
			continue
		}
		intervals = append(intervals, parquetInterval{offset, offset + length, "inline_column_metadata"})
		v.Matched++
		v.VerifiedBytes += length
		acceptedEnd = offset + length
	}
	v.RemainingUnclassifiedBytes = v.OriginalUnclassifiedBytes - v.VerifiedBytes
	if v.OriginalUnclassifiedBytes > 0 {
		v.VerifiedPercent = 100 * float64(v.VerifiedBytes) / float64(v.OriginalUnclassifiedBytes)
	}
	return intervals, v, nil
}
func inlineVerificationFinding(v *parquetInlineVerification) string {
	conclusion := "Exact byte matches confirm duplicated metadata in an Arrow-compatible writer layout; they do not uniquely identify the writer library."
	if v.Matched == 0 {
		conclusion = "No exact inline copies were confirmed; this does not identify the writer or rule out differently serialized metadata."
	}
	return fmt.Sprintf("Verified inline ColumnChunk metadata copies: %d/%d chunks, %s of %s previously unclassified bytes (%.4f%%); remaining=%s, unmatched=%d, read errors=%d. %s Unmatched bytes remain unclassified.", v.Matched, v.Candidates, formatFooterSize(float64(v.VerifiedBytes)), formatFooterSize(float64(v.OriginalUnclassifiedBytes)), v.VerifiedPercent, formatFooterSize(float64(v.RemainingUnclassifiedBytes)), v.Unmatched, v.ReadErrors, conclusion)
}

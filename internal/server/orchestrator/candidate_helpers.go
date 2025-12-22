package orchestrator

import (
	"context"
	"slices"

	"github.com/samber/lo"

	"github.com/looplj/axonhub/internal/server/biz"
)

// filterCandidatesByChannelIDs filters candidates by allowed channel IDs.
func filterCandidatesByChannelIDs(candidates []*ChannelModelCandidate, allowedIDs []int) []*ChannelModelCandidate {
	if len(allowedIDs) == 0 {
		return candidates
	}

	return lo.Filter(candidates, func(c *ChannelModelCandidate, _ int) bool {
		return lo.Contains(allowedIDs, c.Channel.ID)
	})
}

// filterCandidatesByChannelTags filters candidates by channel tags (OR logic).
func filterCandidatesByChannelTags(candidates []*ChannelModelCandidate, allowedTags []string) []*ChannelModelCandidate {
	if len(allowedTags) == 0 {
		return candidates
	}

	return lo.Filter(candidates, func(c *ChannelModelCandidate, _ int) bool {
		for _, tag := range c.Channel.Tags {
			if lo.Contains(allowedTags, tag) {
				return true
			}
		}

		return false
	})
}

// sortCandidatesByPriorityAndScore sorts candidates by priority first, then by load balancer score within each priority group.
func sortCandidatesByPriorityAndScore(ctx context.Context, candidates []*ChannelModelCandidate, lb *LoadBalancer) []*ChannelModelCandidate {
	if len(candidates) <= 1 {
		return candidates
	}

	// Group by priority
	groups := make(map[int][]*ChannelModelCandidate)
	for _, c := range candidates {
		groups[c.Priority] = append(groups[c.Priority], c)
	}

	// Get sorted priority keys (lower priority value = higher priority)
	priorities := lo.Keys(groups)
	slices.Sort(priorities)

	// Sort each group by LoadBalancer score, then concatenate
	result := make([]*ChannelModelCandidate, 0, len(candidates))

	for _, p := range priorities {
		group := groups[p]

		// Sort group by load balancer score
		sortedGroup := sortCandidatesByScore(ctx, group, lb)
		result = append(result, sortedGroup...)
	}

	return result
}

// sortCandidatesByScore sorts candidates by load balancer score.
func sortCandidatesByScore(ctx context.Context, candidates []*ChannelModelCandidate, lb *LoadBalancer) []*ChannelModelCandidate {
	if len(candidates) <= 1 {
		return candidates
	}

	// Calculate scores for each candidate
	type candidateWithScore struct {
		candidate *ChannelModelCandidate
		score     float64
	}

	scored := make([]candidateWithScore, len(candidates))
	for i, c := range candidates {
		// For now, we score based on channel only
		// In the future, we could extend LoadBalanceStrategy to support channel+model scoring
		score := lb.ScoreChannel(ctx, c.Channel)
		scored[i] = candidateWithScore{
			candidate: c,
			score:     score,
		}
	}

	// Sort by score (descending)
	slices.SortFunc(scored, func(a, b candidateWithScore) int {
		if a.score > b.score {
			return -1
		}

		if a.score < b.score {
			return 1
		}

		return 0
	})

	// Extract sorted candidates
	result := make([]*ChannelModelCandidate, len(scored))
	for i, s := range scored {
		result[i] = s.candidate
	}

	return result
}

// extractChannelsFromCandidates extracts unique channels from candidates while preserving order.
func extractChannelsFromCandidates(candidates []*ChannelModelCandidate) []*biz.Channel {
	if len(candidates) == 0 {
		return nil
	}

	seen := make(map[int]bool)
	channels := make([]*biz.Channel, 0, len(candidates))

	for _, c := range candidates {
		if !seen[c.Channel.ID] {
			channels = append(channels, c.Channel)
			seen[c.Channel.ID] = true
		}
	}

	return channels
}

// findCandidateForChannel finds the first candidate matching the given channel.
// Returns nil if no candidate is found for the channel.
func findCandidateForChannel(candidates []*ChannelModelCandidate, channel *biz.Channel) *ChannelModelCandidate {
	for _, c := range candidates {
		if c.Channel.ID == channel.ID {
			return c
		}
	}

	return nil
}

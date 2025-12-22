package orchestrator

import (
	"context"
	"fmt"

	"github.com/samber/lo"

	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/ent/model"
	"github.com/looplj/axonhub/internal/log"
	"github.com/looplj/axonhub/internal/objects"
	"github.com/looplj/axonhub/internal/server/biz"
)

// ChannelModelCandidate represents a resolved channel and model pair.
type ChannelModelCandidate struct {
	Channel      *biz.Channel
	RequestModel string
	ActualModel  string
	Priority     int
}

// ModelResolver resolves AxonHub Models to channel+model candidates.
type ModelResolver struct {
	ModelService   *biz.ModelService
	ChannelService *biz.ChannelService
}

// NewModelResolver creates a new ModelResolver.
func NewModelResolver(modelService *biz.ModelService, channelService *biz.ChannelService) *ModelResolver {
	return &ModelResolver{
		ModelService:   modelService,
		ChannelService: channelService,
	}
}

// Resolve attempts to resolve a model name to AxonHub Model associations.
// Returns nil if no AxonHub Model is found (fallback to legacy behavior).
func (r *ModelResolver) Resolve(ctx context.Context, modelName string) ([]*ChannelModelCandidate, *ent.Model, error) {
	// Try to find an enabled AxonHub Model matching the model name
	axonhubModel, err := r.ModelService.GetModelByModelID(ctx, modelName, model.StatusEnabled)
	if err != nil {
		if ent.IsNotFound(err) {
			// No AxonHub Model found, return nil to indicate fallback to legacy
			return nil, nil, nil
		}

		return nil, nil, fmt.Errorf("failed to query AxonHub Model: %w", err)
	}

	// Model found, resolve associations to candidates
	if axonhubModel.Settings == nil || len(axonhubModel.Settings.Associations) == 0 {
		log.Debug(ctx, "AxonHub Model has no associations",
			log.String("model", modelName))

		return nil, axonhubModel, nil
	}

	// Convert []ModelAssociation to []*ModelAssociation
	associations := make([]*objects.ModelAssociation, len(axonhubModel.Settings.Associations))
	for i := range axonhubModel.Settings.Associations {
		associations[i] = &axonhubModel.Settings.Associations[i]
	}

	candidates, err := r.resolveAssociations(ctx, associations)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to resolve associations: %w", err)
	}

	if len(candidates) == 0 {
		log.Debug(ctx, "No candidates found for AxonHub Model",
			log.String("model", modelName))
	}

	return candidates, axonhubModel, nil
}

// resolveAssociations uses biz.MatchAssociations to resolve model associations
// and converts the results to ChannelModelCandidate.
func (r *ModelResolver) resolveAssociations(ctx context.Context, associations []*objects.ModelAssociation) ([]*ChannelModelCandidate, error) {
	// Get all enabled channels
	enabledChannels := r.ChannelService.EnabledChannels
	if len(enabledChannels) == 0 {
		return []*ChannelModelCandidate{}, nil
	}

	// Convert []*biz.Channel to []biz.Channel for the matching function
	channels := lo.Map(enabledChannels, func(ch *biz.Channel, _ int) biz.Channel {
		return *ch
	})

	// Use the shared MatchAssociations function
	connections, err := biz.MatchAssociations(ctx, associations, channels)
	if err != nil {
		return nil, fmt.Errorf("failed to match associations: %w", err)
	}

	// Convert ModelChannelConnection to ChannelModelCandidate
	candidates := make([]*ChannelModelCandidate, 0, len(connections))
	for _, conn := range connections {
		bizCh, found := lo.Find(enabledChannels, func(c *biz.Channel) bool {
			return c.ID == conn.Channel.ID
		})
		if !found || bizCh == nil {
			continue
		}

		entries := bizCh.GetModelEntries()
		for _, modelID := range conn.ModelIds {
			entry, found := lo.Find(entries, func(e biz.ChannelModelEntry) bool {
				return e.RequestModel == modelID
			})
			if found {
				candidates = append(candidates, &ChannelModelCandidate{
					Channel:      bizCh,
					RequestModel: entry.RequestModel,
					ActualModel:  entry.ActualModel,
					Priority:     conn.Priority,
				})
			}
		}
	}

	return candidates, nil
}

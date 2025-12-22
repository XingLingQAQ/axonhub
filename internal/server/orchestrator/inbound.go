package orchestrator

import (
	"context"
	"fmt"

	"github.com/looplj/axonhub/internal/dumper"
	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/llm"
	"github.com/looplj/axonhub/internal/llm/pipeline"
	"github.com/looplj/axonhub/internal/llm/transformer"
	"github.com/looplj/axonhub/internal/log"
	"github.com/looplj/axonhub/internal/pkg/httpclient"
	"github.com/looplj/axonhub/internal/pkg/streams"
	"github.com/looplj/axonhub/internal/server/biz"
)

// InboundPersistentStream wraps a stream and tracks all responses for final saving to database.
// It implements the streams.Stream interface and handles persistence in the Close method.
//
//nolint:containedctx // Checked.
type InboundPersistentStream struct {
	ctx            context.Context
	stream         streams.Stream[*httpclient.StreamEvent]
	request        *ent.Request
	requestExec    *ent.RequestExecution
	requestService *biz.RequestService
	transformer    transformer.Inbound
	perf           *biz.PerformanceRecord
	responseChunks []*httpclient.StreamEvent
	closed         bool
}

var _ streams.Stream[*httpclient.StreamEvent] = (*InboundPersistentStream)(nil)

func NewInboundPersistentStream(
	ctx context.Context,
	stream streams.Stream[*httpclient.StreamEvent],
	request *ent.Request,
	requestExec *ent.RequestExecution,
	requestService *biz.RequestService,
	transformer transformer.Inbound,
	perf *biz.PerformanceRecord,
) *InboundPersistentStream {
	return &InboundPersistentStream{
		ctx:            ctx,
		stream:         stream,
		request:        request,
		requestExec:    requestExec,
		requestService: requestService,
		transformer:    transformer,
		perf:           perf,
		responseChunks: make([]*httpclient.StreamEvent, 0),
		closed:         false,
	}
}

func (ts *InboundPersistentStream) Next() bool {
	return ts.stream.Next()
}

func (ts *InboundPersistentStream) Current() *httpclient.StreamEvent {
	event := ts.stream.Current()
	if event != nil {
		ts.responseChunks = append(ts.responseChunks, event)

		err := ts.requestService.AppendRequestChunk(
			ts.ctx,
			ts.request.ID,
			event,
		)
		if err != nil {
			log.Warn(ts.ctx, "Failed to append request chunk", log.Cause(err))
		}
	}

	return event
}

func (ts *InboundPersistentStream) Err() error {
	return ts.stream.Err()
}

func (ts *InboundPersistentStream) Close() error {
	if ts.closed {
		return nil
	}

	ts.closed = true
	ctx := ts.ctx

	log.Debug(ctx, "Closing persistent stream", log.Int("chunk_count", len(ts.responseChunks)))

	streamErr := ts.stream.Err()
	if streamErr != nil {
		// Stream had an error - update both request execution and main request
		log.Warn(ctx, "Stream completed with error", log.Cause(streamErr))

		// Use context without cancellation to ensure persistence even if client canceled
		if ts.request != nil {
			persistCtx := context.WithoutCancel(ctx)
			if err := ts.requestService.UpdateRequestStatusFromError(persistCtx, ts.request.ID, streamErr); err != nil {
				log.Warn(persistCtx, "Failed to update request status from error", log.Cause(err))
			}
		}

		return ts.stream.Close()
	}

	// Stream completed successfully - perform final persistence
	log.Debug(ctx, "Stream completed successfully, performing final persistence")

	ts.persistResponseChunks(ctx)

	return ts.stream.Close()
}

func (ts *InboundPersistentStream) persistResponseChunks(ctx context.Context) {
	defer func() {
		if cause := recover(); cause != nil {
			log.Warn(ctx, "Failed to persist inbound response chunks", log.Any("cause", cause))
		}
	}()

	// Update main request with aggregated response
	// Use context without cancellation to ensure persistence even if client canceled
	if ts.request != nil {
		persistCtx := context.WithoutCancel(ctx)

		responseBody, meta, err := ts.transformer.AggregateStreamChunks(persistCtx, ts.responseChunks)
		if err != nil {
			log.Warn(persistCtx, "Failed to aggregate chunks for main request", log.Cause(err))

			dumper.DumpStreamEvents(persistCtx, ts.responseChunks, "response_chunks.json")
		}

		// Build latency metrics from performance record
		var metrics *biz.LatencyMetrics

		if ts.perf != nil {
			firstTokenLatencyMs, requestLatencyMs, _ := ts.perf.Calculate()

			metrics = &biz.LatencyMetrics{
				LatencyMs: &requestLatencyMs,
			}
			if ts.perf.Stream && ts.perf.FirstTokenTime != nil {
				metrics.FirstTokenLatencyMs = &firstTokenLatencyMs
			}
		}

		err = ts.requestService.UpdateRequestCompleted(persistCtx, ts.request.ID, meta.ID, responseBody, metrics)
		if err != nil {
			log.Warn(persistCtx, "Failed to update request status to completed", log.Cause(err))
		}
	}
}

// PersistentInboundTransformer wraps an inbound transformer with enhanced capabilities.
type PersistentInboundTransformer struct {
	wrapped transformer.Inbound
	state   *PersistenceState
}

func (p *PersistentInboundTransformer) APIFormat() llm.APIFormat {
	return p.wrapped.APIFormat()
}

func (p *PersistentInboundTransformer) TransformError(ctx context.Context, rawErr error) *httpclient.Error {
	return p.wrapped.TransformError(ctx, rawErr)
}

func (p *PersistentInboundTransformer) TransformRequest(ctx context.Context, request *httpclient.Request) (*llm.Request, error) {
	llmRequest, err := p.wrapped.TransformRequest(ctx, request)
	if err != nil {
		return nil, err
	}

	llmRequest.RawRequest = request
	p.state.RawRequest = request
	p.state.LlmRequest = llmRequest

	return llmRequest, nil
}

func (p *PersistentInboundTransformer) TransformResponse(ctx context.Context, response *llm.Response) (*httpclient.Response, error) {
	return p.wrapped.TransformResponse(ctx, response)
}

func (p *PersistentInboundTransformer) TransformStream(ctx context.Context, stream streams.Stream[*llm.Response]) (streams.Stream[*httpclient.StreamEvent], error) {
	channelStream, err := p.wrapped.TransformStream(ctx, stream)
	if err != nil {
		return nil, err
	}

	persistentStream := NewInboundPersistentStream(
		ctx,
		channelStream,
		p.state.Request,
		p.state.RequestExec,
		p.state.RequestService,
		p, // Use the PersistentInboundTransformer as the transformer
		p.state.Perf,
	)

	return persistentStream, nil
}

func (p *PersistentInboundTransformer) AggregateStreamChunks(ctx context.Context, chunks []*httpclient.StreamEvent) ([]byte, llm.ResponseMeta, error) {
	return p.wrapped.AggregateStreamChunks(ctx, chunks)
}

// applyApiKeyModelMapping creates a middleware that applies model mapping from API key profiles.
// This is the first step in the inbound pipeline.
func applyApiKeyModelMapping(inbound *PersistentInboundTransformer) pipeline.Middleware {
	return pipeline.OnLlmRequest("apply-model-mapping", func(ctx context.Context, llmRequest *llm.Request) (*llm.Request, error) {
		if llmRequest.Model == "" {
			return nil, fmt.Errorf("%w: request model is empty", biz.ErrInvalidModel)
		}

		// Apply model mapping from API key profiles if active profile exists
		if inbound.state.APIKey == nil {
			return llmRequest, nil
		}

		originalModel := llmRequest.Model
		mappedModel := inbound.state.ModelMapper.MapModel(ctx, inbound.state.APIKey, originalModel)

		if mappedModel != originalModel {
			llmRequest.Model = mappedModel
			log.Debug(ctx, "applied model mapping from API key profile",
				log.String("api_key_name", inbound.state.APIKey.Name),
				log.String("original_model", originalModel),
				log.String("mapped_model", mappedModel))
		}

		// Save the model for later use, e.g. retry from next channels, should use the original model to choose channel model.
		// This should be done after the api key level model mapping.
		// This should be done before the request is created.
		// The outbound transformer will restore the original model if it was mapped.
		if inbound.state.OriginalModel == "" {
			inbound.state.OriginalModel = llmRequest.Model
		} else {
			// Restore original model if it was mapped
			// This should not happen, the inbound should not be called twice.
			// Just in case, restore the original model.
			llmRequest.Model = inbound.state.OriginalModel
		}

		return llmRequest, nil
	})
}

// resolveAxonHubModel creates a middleware that resolves AxonHub Models to channel+model candidates.
// This runs after API key model mapping and before channel selection.
func resolveAxonHubModel(inbound *PersistentInboundTransformer) pipeline.Middleware {
	return pipeline.OnLlmRequest("resolve-axonhub-model", func(ctx context.Context, llmRequest *llm.Request) (*llm.Request, error) {
		// Skip if already resolved
		if inbound.state.AxonHubModel != nil || len(inbound.state.ChannelModelCandidates) > 0 {
			return llmRequest, nil
		}

		// Skip if ModelResolver is not available
		if inbound.state.ModelResolver == nil {
			return llmRequest, nil
		}

		// Try to resolve AxonHub Model
		candidates, axonhubModel, err := inbound.state.ModelResolver.Resolve(ctx, llmRequest.Model)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve AxonHub Model: %w", err)
		}

		// If no AxonHub Model found, continue with legacy flow
		if axonhubModel == nil {
			log.Debug(ctx, "No AxonHub Model found, using legacy channel selection",
				log.String("model", llmRequest.Model))

			return llmRequest, nil
		}

		// Store AxonHub Model and candidates in state
		inbound.state.AxonHubModel = axonhubModel
		inbound.state.ChannelModelCandidates = candidates

		if len(candidates) > 0 {
			log.Debug(ctx, "Resolved AxonHub Model to candidates",
				log.String("model", llmRequest.Model),
				log.Int("candidate_count", len(candidates)))
		}

		return llmRequest, nil
	})
}

// selectChannels creates a middleware that selects available channels for the model.
// This is the second step in the inbound pipeline, moved from outbound transformer.
// If no valid channels are found, it returns ErrInvalidModel to fail fast.
func selectChannels(inbound *PersistentInboundTransformer) pipeline.Middleware {
	return pipeline.OnLlmRequest("select-channels", func(ctx context.Context, llmRequest *llm.Request) (*llm.Request, error) {
		// Only select channels once
		if len(inbound.state.Channels) > 0 {
			return llmRequest, nil
		}

		// If we have AxonHub Model candidates, use them instead of legacy selection
		if len(inbound.state.ChannelModelCandidates) > 0 {
			return selectChannelsFromCandidates(ctx, inbound, llmRequest)
		}

		// Legacy channel selection
		return selectChannelsLegacy(ctx, inbound, llmRequest)
	})
}

// selectChannelsFromCandidates handles channel selection when AxonHub Model candidates are available.
func selectChannelsFromCandidates(ctx context.Context, inbound *PersistentInboundTransformer, llmRequest *llm.Request) (*llm.Request, error) {
	candidates := inbound.state.ChannelModelCandidates

	// Apply API Key Profile filtering
	if profile := GetActiveProfile(inbound.state.APIKey); profile != nil {
		// Filter by ChannelIDs
		if len(profile.ChannelIDs) > 0 {
			candidates = filterCandidatesByChannelIDs(candidates, profile.ChannelIDs)
		}

		// Filter by ChannelTags
		if len(profile.ChannelTags) > 0 {
			candidates = filterCandidatesByChannelTags(candidates, profile.ChannelTags)
		}
	}

	if len(candidates) == 0 {
		return nil, fmt.Errorf("%w: no valid candidates after profile filtering for model %s", biz.ErrInvalidModel, llmRequest.Model)
	}

	// Sort candidates by priority and load balancer score
	if inbound.state.LoadBalancer != nil {
		candidates = sortCandidatesByPriorityAndScore(ctx, candidates, inbound.state.LoadBalancer)
	}

	// Store sorted candidates
	inbound.state.ChannelModelCandidates = candidates

	// Extract channels for compatibility with existing retry logic
	channels := extractChannelsFromCandidates(candidates)
	inbound.state.Channels = channels

	log.Debug(ctx, "selected channels from AxonHub Model candidates",
		log.Int("candidate_count", len(candidates)),
		log.Int("channel_count", len(channels)),
		log.String("model", llmRequest.Model))

	return llmRequest, nil
}

// selectChannelsLegacy handles legacy channel selection when no AxonHub Model is found.
func selectChannelsLegacy(ctx context.Context, inbound *PersistentInboundTransformer, llmRequest *llm.Request) (*llm.Request, error) {
	selector := inbound.state.ChannelSelector

	if profile := GetActiveProfile(inbound.state.APIKey); profile != nil {
		// 先应用 ChannelIDs 过滤
		if len(profile.ChannelIDs) > 0 {
			selector = NewSelectedChannelsSelector(selector, profile.ChannelIDs)
		}

		// 再应用 ChannelTags 过滤（链式装饰器，与 IDs 取交集）
		if len(profile.ChannelTags) > 0 {
			selector = NewTagsFilterSelector(selector, profile.ChannelTags)
		}
	}

	// 应用 Google 原生工具过滤（仅对 Gemini 原生 API 格式生效）
	if inbound.APIFormat() == llm.APIFormatGeminiContents {
		selector = NewGoogleNativeToolsSelector(selector)
	}

	// 应用 Anthropic 原生工具过滤（对所有 API 格式生效）
	// 无论通过 OpenAI 还是 Anthropic 格式入口，只要包含 web_search 工具，
	// 都需要优先路由到支持 Anthropic 原生工具的渠道
	selector = NewAnthropicNativeToolsSelector(selector)

	if inbound.state.LoadBalancer != nil {
		selector = NewLoadBalancedSelector(selector, inbound.state.LoadBalancer)
	}

	channels, err := selector.Select(ctx, llmRequest)
	if err != nil {
		return nil, err
	}

	log.Debug(ctx, "selected channels (legacy)",
		log.Any("channels", channels),
		log.Any("model", llmRequest.Model),
	)

	if len(channels) == 0 {
		return nil, fmt.Errorf("%w: no valid channels found for model %s", biz.ErrInvalidModel, llmRequest.Model)
	}

	inbound.state.Channels = channels

	return llmRequest, nil
}

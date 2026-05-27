package dingtalk

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/chenhg5/cc-connect/core"
)

const (
	cardFlowProcessing = "1"
	cardFlowInputing   = "2"
	cardFlowFinished   = "3"
)

// aiCard implements core.StreamingCard for DingTalk AI Card streaming.
type aiCard struct {
	cardInstanceId string
	outTrackId     string
	templateKey    string
	defaultTpl     bool
	platform       *Platform

	mu              sync.Mutex
	state           string // "processing" | "finished" | "failed"
	lastSentContent string
	lastSentAt      time.Time
	inputingStarted bool

	// 节流控制（single-flight + latest-wins 语义）
	throttleMs     int
	pendingContent string
	timer          *time.Timer
	inFlight       bool
	done           chan struct{} // closed when finalized or failed
}

// Ensure aiCard implements core.StreamingCard
var _ core.StreamingCard = (*aiCard)(nil)
var _ core.StreamingCardFailure = (*aiCard)(nil)

// generateOutTrackID generates a unique outTrackId for AI Card.
func generateOutTrackID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return fmt.Sprintf("card_%d_%s", time.Now().UnixMilli(), hex.EncodeToString(b))
}

// generateGUID generates a UUID-like string for API requests.
func generateGUID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	// Set version (4) and variant bits
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// createAICard creates a new AI Card instance and delivers it to the conversation.
func (p *Platform) createAICard(ctx context.Context, rc replyContext) (*aiCard, error) {
	token, err := p.getAccessToken()
	if err != nil {
		return nil, fmt.Errorf("get access token: %w", err)
	}

	outTrackId := generateOutTrackID()
	isGroup := rc.isGroup
	openSpaceId := openSpaceIDFor(rc)

	cardParamMap := map[string]string{
		"config":          `{"autoLayout":true,"enableForward":true}`,
		p.cardTemplateKey: "",
	}
	if p.useDefaultTemplate {
		cardParamMap = map[string]string{
			"flowStatus":        cardFlowProcessing,
			p.cardTemplateKey:   "",
			"staticMsgContent":  "",
			"sys_full_json_obj": `{"order":["` + p.cardTemplateKey + `"]}`,
			"config":            `{"autoLayout":true}`,
		}
	}

	payload := map[string]any{
		"cardTemplateId": p.cardTemplateID,
		"outTrackId":     outTrackId,
		"cardData": map[string]any{
			"cardParamMap": cardParamMap,
		},
		"callbackType":          "STREAM",
		"imGroupOpenSpaceModel": map[string]any{"supportForward": true},
		"imRobotOpenSpaceModel": map[string]any{"supportForward": true},
		"openSpaceId":           openSpaceId,
		"userIdType":            1,
	}

	// Set delivery model based on conversation type
	if isGroup {
		payload["imGroupOpenDeliverModel"] = map[string]any{
			"robotCode": p.robotCode,
		}
	} else {
		payload["imRobotOpenDeliverModel"] = map[string]any{
			"spaceType": "IM_ROBOT",
			"robotCode": p.robotCode,
		}
	}

	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal payload: %w", err)
	}

	reqCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost,
		"https://api.dingtalk.com/v1.0/card/instances/createAndDeliver",
		bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-acs-dingtalk-access-token", token)

	slog.Debug("dingtalk: creating AI card",
		"outTrackId", outTrackId,
		"isGroup", isGroup,
		"defaultTpl", p.useDefaultTemplate)

	if err := cardLimiter.wait(reqCtx); err != nil {
		return nil, fmt.Errorf("rate limit wait: %w", err)
	}
	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)

	slog.Debug("dingtalk: createAndDeliver response",
		"status", resp.StatusCode,
		"body", string(respBody))

	if resp.StatusCode != http.StatusOK {
		slog.Error("dingtalk: create AI card failed",
			"status", resp.StatusCode,
			"body", string(respBody))
		// Check if we should trigger degrade
		if resp.StatusCode == 403 || resp.StatusCode == 429 || resp.StatusCode >= 500 {
			p.activateCardDegrade(fmt.Sprintf("card.create:%d", resp.StatusCode))
		}
		return nil, fmt.Errorf("create AI card: status=%d, body=%s", resp.StatusCode, string(respBody))
	}

	// Parse response to get cardInstanceId
	var result struct {
		Result struct {
			CardInstanceId  string `json:"cardInstanceId"`
			OutTrackId      string `json:"outTrackId"`
			ProcessQueryKey string `json:"processQueryKey"`
		} `json:"result"`
		CardInstanceId string `json:"cardInstanceId"`
		OutTrackId     string `json:"outTrackId"`
	}
	if err := json.Unmarshal(respBody, &result); err != nil {
		slog.Warn("dingtalk: failed to parse createAndDeliver response", "error", err, "body", string(respBody))
	}

	cardInstanceId := result.Result.CardInstanceId
	if cardInstanceId == "" {
		cardInstanceId = result.CardInstanceId
	}
	if cardInstanceId == "" {
		cardInstanceId = outTrackId
	}

	resolvedOutTrackId := result.Result.OutTrackId
	if resolvedOutTrackId == "" {
		resolvedOutTrackId = result.OutTrackId
	}
	if resolvedOutTrackId == "" {
		resolvedOutTrackId = outTrackId
	}

	// Check deliverResults for actual delivery success
	var deliverCheck struct {
		Result struct {
			DeliverResults []struct {
				Success  bool   `json:"success"`
				ErrorMsg string `json:"errorMsg"`
			} `json:"deliverResults"`
		} `json:"result"`
	}
	if err := json.Unmarshal(respBody, &deliverCheck); err == nil {
		for _, dr := range deliverCheck.Result.DeliverResults {
			if !dr.Success {
				slog.Warn("dingtalk: AI card delivery failed",
					"errorMsg", dr.ErrorMsg,
					"outTrackId", outTrackId,
					"isGroup", isGroup)
				return nil, fmt.Errorf("AI card delivery failed: %s", dr.ErrorMsg)
			}
		}
	}

	slog.Info("dingtalk: AI card created",
		"cardInstanceId", cardInstanceId,
		"outTrackId", resolvedOutTrackId)

	card := &aiCard{
		cardInstanceId: cardInstanceId,
		outTrackId:     resolvedOutTrackId,
		templateKey:    p.cardTemplateKey,
		defaultTpl:     p.useDefaultTemplate,
		platform:       p,
		state:          "processing",
		throttleMs:     p.cardThrottleMs,
		done:           make(chan struct{}),
	}

	return card, nil
}

// Update replaces the card content with the given markdown.
// Implements throttling using single-flight + latest-wins semantics.
func (c *aiCard) Update(ctx context.Context, content string) error {
	c.mu.Lock()

	// If already finished or failed, skip
	if c.state == "finished" || c.state == "failed" {
		c.mu.Unlock()
		return nil
	}

	c.pendingContent = content

	// If there's an in-flight request, schedule a timer
	if c.inFlight {
		c.scheduleFlushLocked()
		c.mu.Unlock()
		return nil
	}

	// If enough time has passed since last send, flush immediately
	if c.timer == nil && time.Since(c.lastSentAt) >= time.Duration(c.throttleMs)*time.Millisecond {
		c.mu.Unlock()
		c.flush(ctx)
		return nil
	}

	// Otherwise, schedule a timer
	c.scheduleFlushLocked()
	c.mu.Unlock()
	return nil
}

// scheduleFlushLocked schedules a flush after throttleMs. Must be called with mu held.
func (c *aiCard) scheduleFlushLocked() {
	if c.timer != nil {
		return
	}
	delay := time.Duration(c.throttleMs)*time.Millisecond - time.Since(c.lastSentAt)
	if delay < 0 {
		delay = 0
	}
	c.timer = time.AfterFunc(delay, func() {
		c.mu.Lock()
		c.timer = nil
		c.mu.Unlock()
		c.flush(context.Background())
	})
}

// flush sends the pending content to the DingTalk streaming API.
func (c *aiCard) flush(ctx context.Context) {
	c.mu.Lock()
	if c.state == "finished" || c.state == "failed" {
		c.mu.Unlock()
		return
	}
	if c.pendingContent == "" {
		c.mu.Unlock()
		return
	}
	if c.inFlight {
		c.mu.Unlock()
		return
	}

	content := c.pendingContent
	c.pendingContent = ""
	c.inFlight = true
	c.mu.Unlock()

	err := c.doStream(ctx, content, false)

	c.mu.Lock()
	c.inFlight = false
	if err != nil {
		slog.Error("dingtalk: AI card stream update failed", "error", err)
		if isTransientCardError(err) && c.state != "failed" {
			if c.pendingContent == "" {
				c.pendingContent = content
			}
			c.scheduleFlushLocked()
		} else {
			c.markFailedLocked()
		}
	} else {
		c.lastSentContent = content
		c.lastSentAt = time.Now()
		// Check if new content arrived during in-flight
		if c.pendingContent != "" {
			c.scheduleFlushLocked()
		}
	}
	c.mu.Unlock()
}

// doStream sends content to the DingTalk streaming API.
func (c *aiCard) doStream(ctx context.Context, content string, isFinalize bool) error {
	return c.doStreamWithErrorFlag(ctx, content, isFinalize, false)
}

func (c *aiCard) doStreamWithErrorFlag(ctx context.Context, content string, isFinalize bool, isError bool) error {
	if c.defaultTpl {
		c.mu.Lock()
		needInputing := !c.inputingStarted
		c.mu.Unlock()
		if needInputing {
			if err := c.putFlowStatus(ctx, cardFlowInputing, content); err != nil {
				slog.Warn("dingtalk: AI card INPUTING transition failed", "error", err)
			} else {
				c.mu.Lock()
				c.inputingStarted = true
				c.mu.Unlock()
			}
		}
	}

	token, err := c.platform.getAccessToken()
	if err != nil {
		return fmt.Errorf("get access token: %w", err)
	}

	streamContent := normalizeForCard(content)
	if !isFinalize {
		streamContent = strings.TrimRight(streamContent, "\n ")
	}

	payload := map[string]any{
		"outTrackId": c.outTrackId,
		"key":        c.templateKey,
		"content":    streamContent,
		"isFull":     true,
		"isFinalize": isFinalize,
		"isError":    isError,
		"guid":       generateGUID(),
	}

	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}

	reqCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPut,
		"https://api.dingtalk.com/v1.0/card/streaming",
		bytes.NewReader(bodyBytes))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-acs-dingtalk-access-token", token)

	slog.Debug("dingtalk: streaming AI card",
		"outTrackId", c.outTrackId,
		"contentLen", len(streamContent),
		"isFinalize", isFinalize,
		"isError", isError)

	if err := cardLimiter.wait(reqCtx); err != nil {
		return fmt.Errorf("rate limit wait: %w", err)
	}
	resp, err := c.platform.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)

	slog.Debug("dingtalk: streaming response",
		"status", resp.StatusCode,
		"body", string(respBody),
		"isFinalize", isFinalize)

	if resp.StatusCode != http.StatusOK {
		if isQpsLimit(resp.StatusCode, respBody) {
			cardLimiter.triggerBackoff()
			slog.Warn("dingtalk: AI card stream hit QPS limit, backing off", "status", resp.StatusCode)
			return &cardAPIError{op: "stream AI card", status: resp.StatusCode, body: string(respBody), transient: true}
		}
		slog.Error("dingtalk: stream AI card failed",
			"status", resp.StatusCode,
			"body", string(respBody))
		// Check if we should trigger degrade
		if resp.StatusCode == 403 || resp.StatusCode == 429 || resp.StatusCode >= 500 {
			c.platform.activateCardDegrade(fmt.Sprintf("card.stream:%d", resp.StatusCode))
		}
		return &cardAPIError{op: "stream AI card", status: resp.StatusCode, body: string(respBody)}
	}

	slog.Debug("dingtalk: AI card streamed successfully", "isFinalize", isFinalize)
	return nil
}

// putFlowStatus updates the public template's flowStatus parameter via
// PUT /v1.0/card/instances. It is only used with DingTalk's public template.
func (c *aiCard) putFlowStatus(ctx context.Context, status, content string) error {
	token, err := c.platform.getAccessToken()
	if err != nil {
		return fmt.Errorf("get access token: %w", err)
	}

	body := map[string]any{
		"outTrackId": c.outTrackId,
		"cardData": map[string]any{
			"cardParamMap": map[string]string{
				"flowStatus":        status,
				c.templateKey:       normalizeForCard(content),
				"staticMsgContent":  "",
				"sys_full_json_obj": `{"order":["` + c.templateKey + `"]}`,
				"config":            `{"autoLayout":true}`,
			},
		},
		"cardUpdateOptions": map[string]any{
			"updateCardDataByKey": true,
		},
	}
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}

	reqCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPut,
		"https://api.dingtalk.com/v1.0/card/instances",
		bytes.NewReader(bodyBytes))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-acs-dingtalk-access-token", token)

	if err := cardLimiter.wait(reqCtx); err != nil {
		return fmt.Errorf("rate limit wait: %w", err)
	}
	resp, err := c.platform.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		if isQpsLimit(resp.StatusCode, respBody) {
			cardLimiter.triggerBackoff()
			return &cardAPIError{op: "update AI card flowStatus", status: resp.StatusCode, body: string(respBody), transient: true}
		}
		if resp.StatusCode == 403 || resp.StatusCode == 429 || resp.StatusCode >= 500 {
			c.platform.activateCardDegrade(fmt.Sprintf("card.flow:%d", resp.StatusCode))
		}
		return &cardAPIError{op: "update AI card flowStatus", status: resp.StatusCode, body: string(respBody)}
	}

	slog.Debug("dingtalk: AI card flowStatus updated", "status", status)
	return nil
}

// Finalize sends the final content and marks the card as complete.
func (c *aiCard) Finalize(ctx context.Context, content string) error {
	c.mu.Lock()

	// Stop any pending timer
	if c.timer != nil {
		c.timer.Stop()
		c.timer = nil
	}

	// If already finished or failed, skip
	if c.state == "finished" || c.state == "failed" {
		c.mu.Unlock()
		return nil
	}

	// Wait for in-flight to complete
	for c.inFlight {
		c.mu.Unlock()
		select {
		case <-c.done:
			return nil
		case <-time.After(100 * time.Millisecond):
		}
		c.mu.Lock()
	}

	c.inFlight = true
	c.mu.Unlock()

	err := c.doStream(ctx, content, true)
	if err == nil && c.defaultTpl {
		if ferr := c.putFlowStatus(ctx, cardFlowFinished, content); ferr != nil {
			slog.Warn("dingtalk: AI card FINISHED transition failed", "error", ferr)
			err = ferr
		}
	}

	c.mu.Lock()
	c.inFlight = false
	if err != nil {
		c.markFailedLocked()
	} else {
		c.state = "finished"
		c.lastSentContent = content
		c.lastSentAt = time.Now()
	}
	c.closeDoneLocked()
	c.mu.Unlock()

	return err
}

// Fail writes an error payload into the card and stops the public template's
// loading state. Callers fall back to a normal message if this fails.
func (c *aiCard) Fail(ctx context.Context, content string) error {
	c.mu.Lock()
	if c.timer != nil {
		c.timer.Stop()
		c.timer = nil
	}
	c.pendingContent = ""
	if c.state == "finished" || c.state == "failed" {
		c.mu.Unlock()
		return nil
	}
	for c.inFlight {
		c.mu.Unlock()
		select {
		case <-c.done:
			return nil
		case <-time.After(100 * time.Millisecond):
		}
		c.mu.Lock()
	}

	c.inFlight = true
	c.mu.Unlock()

	err := c.doStreamWithErrorFlag(ctx, content, true, true)
	if err == nil && c.defaultTpl {
		if ferr := c.putFlowStatus(ctx, cardFlowFinished, content); ferr != nil {
			slog.Warn("dingtalk: AI card failure FINISHED transition failed", "error", ferr)
			err = ferr
		}
	}

	c.mu.Lock()
	c.inFlight = false
	c.markFailedLocked()
	c.mu.Unlock()
	return err
}

// Failed returns true if the card has entered a failed state.
func (c *aiCard) Failed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state == "failed"
}

func (c *aiCard) closeDoneLocked() {
	select {
	case <-c.done:
	default:
		close(c.done)
	}
}

func (c *aiCard) markFailedLocked() {
	c.state = "failed"
	c.closeDoneLocked()
}

// isCardDegraded returns true if card API is temporarily degraded.
func (p *Platform) isCardDegraded() bool {
	p.degradeMu.Lock()
	defer p.degradeMu.Unlock()
	return time.Now().Before(p.degradeUntil)
}

// activateCardDegrade activates card API degradation for 30 minutes.
func (p *Platform) activateCardDegrade(reason string) {
	p.degradeMu.Lock()
	defer p.degradeMu.Unlock()
	p.degradeUntil = time.Now().Add(30 * time.Minute)
	slog.Warn("dingtalk: AI card API degraded",
		"reason", reason,
		"until", p.degradeUntil.Format(time.RFC3339))
}

type cardAPIError struct {
	op        string
	status    int
	body      string
	transient bool
}

func (e *cardAPIError) Error() string {
	return fmt.Sprintf("%s: status=%d, body=%s", e.op, e.status, e.body)
}

func isTransientCardError(err error) bool {
	var apiErr *cardAPIError
	return errors.As(err, &apiErr) && apiErr.transient
}

// ─────────────────────────────────────────────────────────────────────────────
// QPS limiter (process-wide, shared across all DingTalk Platform instances)
// ─────────────────────────────────────────────────────────────────────────────

type tokenBucket struct {
	mu            sync.Mutex
	tokens        float64
	maxTokens     float64
	refillPerSec  float64
	lastRefill    time.Time
	backoffUntil  time.Time
	backoffWindow time.Duration
	queue         chan struct{}
}

func newTokenBucket(maxTokens, refillPerSec float64, backoff time.Duration) *tokenBucket {
	return &tokenBucket{
		tokens:        maxTokens,
		maxTokens:     maxTokens,
		refillPerSec:  refillPerSec,
		lastRefill:    time.Now(),
		backoffWindow: backoff,
		queue:         make(chan struct{}, 1),
	}
}

func (b *tokenBucket) wait(ctx context.Context) error {
	select {
	case b.queue <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-b.queue }()

	for {
		b.mu.Lock()
		now := time.Now()
		if now.Before(b.backoffUntil) {
			delay := b.backoffUntil.Sub(now)
			b.mu.Unlock()
			select {
			case <-time.After(delay):
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		elapsed := now.Sub(b.lastRefill).Seconds()
		if elapsed > 0 {
			b.tokens += elapsed * b.refillPerSec
			if b.tokens > b.maxTokens {
				b.tokens = b.maxTokens
			}
			b.lastRefill = now
		}
		if b.tokens >= 1 {
			b.tokens--
			b.mu.Unlock()
			return nil
		}

		need := 1 - b.tokens
		wait := time.Duration((need/b.refillPerSec)*1000) * time.Millisecond
		b.mu.Unlock()
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (b *tokenBucket) triggerBackoff() {
	b.mu.Lock()
	defer b.mu.Unlock()
	end := time.Now().Add(b.backoffWindow)
	b.backoffUntil = end
	b.tokens = 0
	b.lastRefill = end
}

var cardLimiter = newTokenBucket(20, 20, 2*time.Second)

var qpsLimitRe = regexp.MustCompile(`"code"\s*:\s*"[^"]*QpsLimit[^"]*"`)

func isQpsLimit(status int, body []byte) bool {
	return status == http.StatusForbidden && qpsLimitRe.Match(body)
}

// ─────────────────────────────────────────────────────────────────────────────
// Markdown normalization for DingTalk AI Card
// ─────────────────────────────────────────────────────────────────────────────

var (
	tableDividerRe       = regexp.MustCompile(`^\s*\|?\s*:?-+:?\s*(\|?\s*:?-+:?\s*)+\|?\s*$`)
	tableRowRe           = regexp.MustCompile(`^\s*\|?.*\|.*\|?\s*$`)
	mdBlockStartRe       = regexp.MustCompile(`^(\s{0,3}(?:[-*+]|\d+[.)])[ ])|(\s{0,3}\|)|(\s{0,3}#{1,6}\s)|(\s{0,3}(?:[-*_])\s*(?:[-*_])\s*(?:[-*_]))`)
	fenceRe              = regexp.MustCompile(`^\s{0,3}` + "```")
	quoteRe              = regexp.MustCompile(`^\s{0,3}>\s?`)
	crlfNormalizerRegexp = regexp.MustCompile(`\r\n?`)
)

func normalizeForCard(content string) string {
	return fixNewlines(ensureTableBlankLines(content))
}

func ensureTableBlankLines(text string) string {
	text = crlfNormalizerRegexp.ReplaceAllString(text, "\n")
	lines := strings.Split(text, "\n")
	out := make([]string, 0, len(lines))

	isDivider := func(line string) bool {
		return strings.Contains(line, "|") && tableDividerRe.MatchString(line)
	}

	for i, line := range lines {
		next := ""
		if i+1 < len(lines) {
			next = lines[i+1]
		}
		if i > 0 &&
			tableRowRe.MatchString(line) &&
			isDivider(next) &&
			strings.TrimSpace(lines[i-1]) != "" &&
			!tableRowRe.MatchString(lines[i-1]) {
			out = append(out, "")
		}
		out = append(out, line)
	}
	return strings.Join(out, "\n")
}

func fixNewlines(text string) string {
	text = crlfNormalizerRegexp.ReplaceAllString(text, "\n")
	rawLines := strings.Split(text, "\n")

	merged := make([]string, 0, len(rawLines))
	pendingQuotes := []string{}
	inCode := false
	flushQuotes := func() {
		if len(pendingQuotes) > 0 {
			merged = append(merged, strings.Join(pendingQuotes, "<br>"))
			pendingQuotes = pendingQuotes[:0]
		}
	}

	for _, line := range rawLines {
		isFence := fenceRe.MatchString(line)
		if inCode {
			flushQuotes()
			merged = append(merged, line)
			if isFence {
				inCode = false
			}
			continue
		}
		if isFence {
			flushQuotes()
			merged = append(merged, line)
			inCode = true
			continue
		}
		if quoteRe.MatchString(line) {
			if len(pendingQuotes) == 0 {
				pendingQuotes = append(pendingQuotes, line)
			} else {
				pendingQuotes = append(pendingQuotes, quoteRe.ReplaceAllString(line, ""))
			}
		} else {
			flushQuotes()
			merged = append(merged, line)
		}
	}
	flushQuotes()

	inCode = false
	var sb strings.Builder
	for i, line := range merged {
		nextInCode := inCode
		if fenceRe.MatchString(line) {
			nextInCode = !inCode
		}
		sb.WriteString(line)
		if i < len(merged)-1 {
			next := merged[i+1]
			keepNewline := nextInCode ||
				line == "" ||
				next == "" ||
				fenceRe.MatchString(next) ||
				mdBlockStartRe.MatchString(next)
			if keepNewline {
				sb.WriteByte('\n')
			} else {
				sb.WriteString("<br>")
			}
		}
		inCode = nextInCode
	}
	return sb.String()
}

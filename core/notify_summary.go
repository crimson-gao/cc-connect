package core

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"sync"
	"text/template"
	"time"
)

// defaultNotifySessionSummaryTemplate uses English imperatives (more reliable
// for instruction following) while requiring a Chinese summary as output.
//
// The agent is allowed to call the Multica CLI for two specific reads
// (issue metadata + comments) — both keyed by the issue_id already in the
// prompt, so the agent does not need to discover anything. All other tool
// calls (shell, grep, find, filesystem search) are explicitly forbidden to
// avoid the 4+ minute "search for the UUID literally" wander that the
// smoke tests reproduced when the instruction was open-ended.
const defaultNotifySessionSummaryTemplate = `You have received {{.notificationCount}} Multica issue notification(s) for issue {{.issueId}}.

Cached issue context (already known, do NOT re-fetch):
- Title: "{{.issueTitle}}"
- Status (DB): {{.issueStatus}}
- Status tag (first "status:" label): {{.issueStatusTag}}
- Linked pull/merge requests:
{{.issuePullRequests}}

Generate a concise progress summary of approximately {{.summaryLength}} Chinese characters.

Allowed tool calls (each at most once, only if the cached context is insufficient):
- ` + "`multica issue get {{.issueId}}`" + ` — fetch full issue metadata (assignee, labels, milestone, parents).
- ` + "`multica issue comment list {{.issueId}}`" + ` — fetch the comment thread.

Hard constraints (must follow ALL):
- Write the summary in Chinese (中文). Do not output any English.
- DO NOT call shell, grep, rg, find, ls, cat, sed, awk, git, curl, or ANY command other than the two ` + "`multica`" + ` reads above. DO NOT search the filesystem or read any file.
- Merge related events by timeline, deduplicate, and highlight the most recent state transition. If a status tag is present, mention it.
- If pull/merge requests are listed, surface them in the summary (state + a short label).
- Output the summary text directly. No preamble, no greeting, no meta phrases (e.g. "以下是总结", "Summary:", "好的", "I will now…").
- End with exactly one line: "详情：multica issue {{.issueId}}"

Notification context:
{{.combinedContent}}`

type NotifySessionSummaryConfig struct {
	Enabled       bool
	Template      string
	SummaryLength int
	IdleWait      time.Duration
	MaxWait       time.Duration
}

func DefaultNotifySessionSummaryConfig() NotifySessionSummaryConfig {
	return NotifySessionSummaryConfig{
		Template:      defaultNotifySessionSummaryTemplate,
		SummaryLength: 300,
		IdleWait:      10 * time.Second,
		MaxWait:       30 * time.Second,
	}
}

func (c NotifySessionSummaryConfig) normalized() NotifySessionSummaryConfig {
	defaults := DefaultNotifySessionSummaryConfig()
	if strings.TrimSpace(c.Template) == "" {
		c.Template = defaults.Template
	}
	if c.SummaryLength <= 0 {
		c.SummaryLength = defaults.SummaryLength
	}
	if c.IdleWait <= 0 {
		c.IdleWait = defaults.IdleWait
	}
	if c.MaxWait <= 0 {
		c.MaxWait = defaults.MaxWait
	}
	return c
}

// NotifySessionResponse describes the disposition of a /notify-session call.
// Status values:
//   - "queued":   batched for summary injection; the prompt has NOT been
//     delivered yet and will fire after idle_wait_secs (or sooner if a flush
//     races); injection failures fall back to direct platform notifications.
//   - "notified": delivered synchronously via the direct platform path (used
//     when summary injection is disabled or no active session was found).
type NotifySessionResponse struct {
	Status     string `json:"status"`
	SessionKey string `json:"session_key,omitempty"`
	Message    string `json:"message,omitempty"`
}

type notifyHTTPError struct {
	status int
	msg    string
}

func (e notifyHTTPError) Error() string { return e.msg }

func writeNotifyError(w http.ResponseWriter, err error) {
	if he, ok := err.(notifyHTTPError); ok {
		http.Error(w, he.msg, he.status)
		return
	}
	http.Error(w, err.Error(), http.StatusInternalServerError)
}

func (s *APIServer) handleNotifySession(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}

	var req NotifyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	if req.UserID == "" || req.Content == "" {
		http.Error(w, "user_id and content are required", http.StatusBadRequest)
		return
	}
	if req.Platform == "" {
		req.Platform = "dingtalk"
	}

	// Always mirror the original notification to Aone — same behavior as
	// /notify. The summarized prompt rendered later is for LLM consumption only
	// and must never reach Aone.
	pushAoneComment(req.Title, req.Content, req.Metadata["inbox_type"])

	staffID, ok := staffIDFromAlibabaUserID(req.UserID)
	if ok {
		req.UserID = staffID
	}
	if !ok || s.notify == nil || !s.notify.enabled() {
		s.notifySessionDirect(w, r, req)
		return
	}

	target, ok := s.findNotifySessionTarget(req.Platform, staffID)
	if !ok {
		s.notifySessionDirect(w, r, req)
		return
	}
	if err := s.notify.enqueue(target, staffID, req); err != nil {
		slog.Warn("notify-session: enqueue failed, falling back to direct",
			"platform", req.Platform, "user_id", req.UserID, "error", err)
		s.notifySessionDirect(w, r, req)
		return
	}

	apiJSON(w, http.StatusOK, NotifySessionResponse{Status: "queued", SessionKey: target.sessionKey})
}

func (s *APIServer) notifySessionDirect(w http.ResponseWriter, r *http.Request, req NotifyRequest) {
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()
	if err := s.sendDirectNotification(ctx, req); err != nil {
		slog.Warn("notify-session: direct fallback failed",
			"platform", req.Platform, "user_id", req.UserID, "error", err)
		writeNotifyError(w, err)
		return
	}
	apiJSON(w, http.StatusOK, NotifySessionResponse{Status: "notified"})
}

func (s *APIServer) sendDirectNotification(ctx context.Context, req NotifyRequest) error {
	s.mu.RLock()
	if len(s.engines) == 0 {
		s.mu.RUnlock()
		return notifyHTTPError{status: http.StatusServiceUnavailable, msg: "no engine available"}
	}

	platformFound := false
	for _, engine := range s.engines {
		if engine == nil {
			continue
		}
		for _, p := range engine.platforms {
			if p.Name() != req.Platform {
				continue
			}
			platformFound = true
			notifier, ok := p.(DirectNotifier)
			if !ok {
				continue
			}
			s.mu.RUnlock()
			return notifier.SendNotification(ctx, req.UserID, req.Title, req.Content, req.Metadata)
		}
	}
	s.mu.RUnlock()
	if !platformFound {
		return notifyHTTPError{status: http.StatusNotFound, msg: fmt.Sprintf("platform %q not found", req.Platform)}
	}
	return notifyHTTPError{status: http.StatusBadRequest, msg: fmt.Sprintf("platform %q does not support direct notifications", req.Platform)}
}

type notifySessionTarget struct {
	engine     *Engine
	sessionKey string
}

func (s *APIServer) findNotifySessionTarget(platform, userID string) (notifySessionTarget, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, e := range s.engines {
		if e == nil || e.sessions == nil {
			continue
		}
		if key := e.sessions.FindActiveDirectSessionByUserID(platform, userID); key != "" {
			return notifySessionTarget{engine: e, sessionKey: key}, true
		}
	}
	return notifySessionTarget{}, false
}

// staffIDFromAlibabaUserID extracts the DingTalk staff ID from an
// "<工号>@alibaba-inc.com" address. Any other shape — bare numeric IDs,
// other domains, missing local-part — is rejected. The wire contract for
// /notify-session is that callers send the full Alibaba email; callers can
// not pass a raw staff ID.
func staffIDFromAlibabaUserID(userID string) (string, bool) {
	userID = strings.TrimSpace(userID)
	if userID == "" {
		return "", false
	}
	local, domain, hasDomain := strings.Cut(userID, "@")
	if !hasDomain {
		return "", false
	}
	local = strings.TrimSpace(local)
	domain = strings.TrimSpace(domain)
	if local == "" || !strings.EqualFold(domain, "alibaba-inc.com") {
		return "", false
	}
	return local, true
}

type notifySummaryDispatcher struct {
	server      *APIServer
	cfg         NotifySessionSummaryConfig
	tmpl        *template.Template
	templateErr error

	mu      sync.Mutex
	buckets map[string]*notifySummaryBucket
}

type notifySummaryBucket struct {
	key           string
	target        notifySessionTarget
	platform      string
	userID        string
	staffID       string
	sessionKey    string
	firstSeen     time.Time
	lastSeen      time.Time
	notifications []notifySummaryNotification
	idleTimer     *time.Timer
	maxTimer      *time.Timer
}

type notifySummaryNotification struct {
	Platform   string
	UserID     string
	Title      string
	Content    string
	Metadata   map[string]string
	ReceivedAt time.Time
}

func newNotifySummaryDispatcher(server *APIServer, cfg NotifySessionSummaryConfig) *notifySummaryDispatcher {
	cfg = cfg.normalized()
	d := &notifySummaryDispatcher{
		server:  server,
		cfg:     cfg,
		buckets: make(map[string]*notifySummaryBucket),
	}
	if cfg.Enabled {
		d.tmpl, d.templateErr = template.New("notify_session_summary").Parse(cfg.Template)
	}
	return d
}

func (d *notifySummaryDispatcher) enabled() bool {
	return d != nil && d.cfg.Enabled
}

func (d *notifySummaryDispatcher) enqueue(target notifySessionTarget, staffID string, req NotifyRequest) error {
	if !d.enabled() {
		return fmt.Errorf("notify session summary is disabled")
	}
	if d.templateErr != nil {
		return fmt.Errorf("parse notify session summary template: %w", d.templateErr)
	}
	if target.engine == nil || target.sessionKey == "" {
		return fmt.Errorf("notify session target required")
	}

	now := time.Now()
	key := notifySummaryBucketKey(req.Platform, staffID, target.sessionKey)
	n := notifySummaryNotification{
		Platform:   req.Platform,
		UserID:     req.UserID,
		Title:      req.Title,
		Content:    req.Content,
		Metadata:   cloneStringMap(req.Metadata),
		ReceivedAt: now,
	}

	d.mu.Lock()
	bucket := d.buckets[key]
	if bucket == nil {
		bucket = &notifySummaryBucket{
			key:        key,
			target:     target,
			platform:   req.Platform,
			userID:     req.UserID,
			staffID:    staffID,
			sessionKey: target.sessionKey,
			firstSeen:  now,
		}
		d.buckets[key] = bucket
		bucket.maxTimer = time.AfterFunc(d.cfg.MaxWait, func() {
			d.flush(key, "max_wait")
		})
	}
	bucket.lastSeen = now
	bucket.notifications = append(bucket.notifications, n)
	if bucket.idleTimer != nil {
		bucket.idleTimer.Stop()
	}
	bucket.idleTimer = time.AfterFunc(d.cfg.IdleWait, func() {
		d.flush(key, "idle_wait")
	})
	d.mu.Unlock()
	return nil
}

func (d *notifySummaryDispatcher) flush(key, reason string) {
	bucket := d.takeBucket(key)
	if bucket == nil {
		return
	}
	if len(bucket.notifications) == 0 {
		return
	}

	prompt, err := d.renderPrompt(bucket)
	if err == nil {
		err = bucket.target.engine.InjectExternalPrompt(bucket.sessionKey, bucket.staffID, prompt)
	}
	if err == nil {
		slog.Info("notify-session: summary prompt injected",
			"session_key", bucket.sessionKey,
			"staff_id", bucket.staffID,
			"count", len(bucket.notifications),
			"reason", reason)
		return
	}

	slog.Warn("notify-session: summary injection failed, falling back to direct notifications",
		"session_key", bucket.sessionKey,
		"staff_id", bucket.staffID,
		"count", len(bucket.notifications),
		"reason", reason,
		"error", err)
	for _, n := range bucket.notifications {
		req := NotifyRequest{
			Platform: n.Platform,
			UserID:   n.UserID,
			Title:    n.Title,
			Content:  n.Content,
			Metadata: cloneStringMap(n.Metadata),
		}
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		if fallbackErr := d.server.sendDirectNotification(ctx, req); fallbackErr != nil {
			slog.Warn("notify-session: direct fallback after injection failure failed",
				"platform", req.Platform, "user_id", req.UserID, "error", fallbackErr)
		}
		cancel()
	}
}

func (d *notifySummaryDispatcher) takeBucket(key string) *notifySummaryBucket {
	d.mu.Lock()
	defer d.mu.Unlock()

	bucket := d.buckets[key]
	if bucket == nil {
		return nil
	}
	delete(d.buckets, key)
	if bucket.idleTimer != nil {
		bucket.idleTimer.Stop()
	}
	if bucket.maxTimer != nil {
		bucket.maxTimer.Stop()
	}
	return bucket
}

func (d *notifySummaryDispatcher) stop() {
	d.mu.Lock()
	defer d.mu.Unlock()
	for key, bucket := range d.buckets {
		if bucket.idleTimer != nil {
			bucket.idleTimer.Stop()
		}
		if bucket.maxTimer != nil {
			bucket.maxTimer.Stop()
		}
		delete(d.buckets, key)
	}
}

func notifySummaryBucketKey(platform, staffID, sessionKey string) string {
	return platform + "\x00" + staffID + "\x00" + sessionKey
}

func (d *notifySummaryDispatcher) renderPrompt(bucket *notifySummaryBucket) (string, error) {
	if d.templateErr != nil {
		return "", d.templateErr
	}
	if d.tmpl == nil {
		return "", fmt.Errorf("notify session summary template is not initialized")
	}
	data := notifySummaryTemplateData(d.cfg, bucket)
	var out bytes.Buffer
	if err := d.tmpl.Execute(&out, data); err != nil {
		return "", err
	}
	prompt := strings.TrimSpace(out.String())
	if prompt == "" {
		return "", fmt.Errorf("notify session summary template rendered empty prompt")
	}
	return prompt, nil
}

func notifySummaryTemplateData(cfg NotifySessionSummaryConfig, bucket *notifySummaryBucket) map[string]any {
	notifications := make([]map[string]any, 0, len(bucket.notifications))
	for _, n := range bucket.notifications {
		notifications = append(notifications, map[string]any{
			"platform":   n.Platform,
			"userId":     n.UserID,
			"title":      n.Title,
			"content":    n.Content,
			"metadata":   cloneStringMap(n.Metadata),
			"inboxType":  metadataValue(n.Metadata, "inbox_type", "inboxType"),
			"receivedAt": n.ReceivedAt.Format(time.RFC3339),
		})
	}

	issueTitle := latestMetadataValue(bucket.notifications, "issue_title", "issueTitle")
	if issueTitle == "" && len(bucket.notifications) > 0 {
		issueTitle = bucket.notifications[len(bucket.notifications)-1].Title
	}
	data := map[string]any{
		"staffId":           bucket.staffID,
		"userId":            bucket.userID,
		"platform":          bucket.platform,
		"sessionKey":        bucket.sessionKey,
		"summaryLength":     cfg.SummaryLength,
		"notificationCount": len(bucket.notifications),
		"notifications":     notifications,
		"combinedContent":   combinedNotifyContent(bucket.notifications),
		"workspaceId":       latestMetadataValue(bucket.notifications, "workspace_id", "workspaceId"),
		"issueId":           latestMetadataValue(bucket.notifications, "issue_id", "issueId"),
		"issueTitle":        issueTitle,
		"issueStatus":       latestMetadataValue(bucket.notifications, "issue_status", "issueStatus"),
		"issueStatusTag":    latestMetadataValue(bucket.notifications, "issue_status_tag", "issueStatusTag"),
		"issueCreateTime":   latestMetadataValue(bucket.notifications, "issue_create_time", "issueCreateTime"),
		"issuePullRequests": latestMetadataValue(bucket.notifications, "issue_pull_requests", "issuePullRequests"),
		"inboxType":         latestMetadataValue(bucket.notifications, "inbox_type", "inboxType"),
	}
	return data
}

func combinedNotifyContent(notifications []notifySummaryNotification) string {
	var b strings.Builder
	for i, n := range notifications {
		if i > 0 {
			b.WriteString("\n\n---\n\n")
		}
		if n.Title != "" {
			b.WriteString("Title: ")
			b.WriteString(n.Title)
			b.WriteString("\n")
		}
		if n.Content != "" {
			b.WriteString("Content:\n")
			b.WriteString(n.Content)
			b.WriteString("\n")
		}
		if len(n.Metadata) > 0 {
			b.WriteString("Metadata:\n")
			keys := make([]string, 0, len(n.Metadata))
			for k := range n.Metadata {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				b.WriteString("- ")
				b.WriteString(k)
				b.WriteString(": ")
				b.WriteString(n.Metadata[k])
				b.WriteString("\n")
			}
		}
	}
	return strings.TrimSpace(b.String())
}

func latestMetadataValue(notifications []notifySummaryNotification, keys ...string) string {
	for i := len(notifications) - 1; i >= 0; i-- {
		for _, key := range keys {
			if v := metadataValue(notifications[i].Metadata, key); v != "" {
				return v
			}
		}
	}
	return ""
}

func metadataValue(metadata map[string]string, keys ...string) string {
	for _, key := range keys {
		if v := strings.TrimSpace(metadata[key]); v != "" {
			return v
		}
	}
	return ""
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

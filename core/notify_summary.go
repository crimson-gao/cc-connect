package core

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
)

// NotifySessionRequest is the wire shape for POST /notify-session. The caller
// (typically multica's per-(staff,issue) summary dispatcher) has already
// rendered the prompt; cc-connect just routes it to the user's active 1:1
// session as if the user typed it.
type NotifySessionRequest struct {
	Platform string `json:"platform,omitempty"`
	UserID   string `json:"user_id"`
	Prompt   string `json:"prompt"`
}

// NotifySessionResponse describes the disposition of a /notify-session call.
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

// handleNotifySession injects a pre-rendered prompt into the recipient's
// active 1:1 session. Strict input contract: user_id must be the Alibaba
// email form "<staff>@alibaba-inc.com"; prompt must be non-empty. No template
// rendering, no batching, no Aone mirror on this path — those are the
// caller's responsibility (multica owns them).
func (s *APIServer) handleNotifySession(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}

	var req NotifySessionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	if strings.TrimSpace(req.UserID) == "" || strings.TrimSpace(req.Prompt) == "" {
		http.Error(w, "user_id and prompt are required", http.StatusBadRequest)
		return
	}
	if req.Platform == "" {
		req.Platform = "dingtalk"
	}

	staffID, ok := staffIDFromAlibabaUserID(req.UserID)
	if !ok {
		http.Error(w, "user_id must be in the form <staff>@alibaba-inc.com", http.StatusBadRequest)
		return
	}

	target, ok := s.findNotifySessionTarget(req.Platform, staffID)
	if !ok {
		http.Error(w, fmt.Sprintf("no active 1:1 session for staff %q on platform %q", staffID, req.Platform), http.StatusNotFound)
		return
	}

	if err := target.engine.InjectExternalPrompt(target.sessionKey, staffID, req.Prompt); err != nil {
		slog.Warn("notify-session: inject failed",
			"platform", req.Platform, "staff_id", staffID, "session_key", target.sessionKey, "error", err)
		http.Error(w, fmt.Sprintf("inject failed: %v", err), http.StatusInternalServerError)
		return
	}

	slog.Info("notify-session: prompt injected",
		"platform", req.Platform, "staff_id", staffID, "session_key", target.sessionKey, "prompt_len", len(req.Prompt))
	apiJSON(w, http.StatusOK, NotifySessionResponse{Status: "injected", SessionKey: target.sessionKey})
}

// sendDirectNotification routes a NotifyRequest to the first engine/platform
// that implements DirectNotifier. Kept on the /notify hot path.
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
// "<工号>@alibaba-inc.com" address. Anything else (bare numeric IDs, other
// domains, missing local-part) is rejected. The wire contract for
// /notify-session and other Alibaba-only paths is the full email form.
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


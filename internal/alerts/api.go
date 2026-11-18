package alerts

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/shopspring/decimal"

	"marketdata/internal/exchange"
	"marketdata/internal/model"
)

// API is the HTTP interface for price rules:
//
//	POST   /v1/rules        {"exchange","symbol","direction":"above|below","price","note"}
//	GET    /v1/rules?exchange=&symbol=&limit=
//	GET    /v1/rules/{id}
//	DELETE /v1/rules/{id}
//
// A created rule fires once; its alert is published on the gateway channel
// alerts:rule:<id> (and alerts:<exchange>:<symbol>).
type API struct {
	svc *Service
	now func() time.Time
}

func NewAPI(svc *Service) *API { return &API{svc: svc, now: time.Now} }

func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/rules", a.create)
	mux.HandleFunc("GET /v1/rules", a.list)
	mux.HandleFunc("GET /v1/rules/{id}", a.get)
	mux.HandleFunc("DELETE /v1/rules/{id}", a.delete)
	return mux
}

type createRequest struct {
	Exchange  string `json:"exchange"`
	Symbol    string `json:"symbol"`
	Direction string `json:"direction"`
	Price     string `json:"price"`
	Note      string `json:"note"`
}

type ruleResponse struct {
	model.AlertRule
	Channel string `json:"channel"`
}

func respond(r model.AlertRule) ruleResponse {
	return ruleResponse{AlertRule: r, Channel: "alerts:rule:" + r.ID}
}

func (a *API) create(w http.ResponseWriter, r *http.Request) {
	var req createRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&req); err != nil {
		httpError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	rule, msg := validate(req)
	if msg != "" {
		httpError(w, http.StatusBadRequest, msg)
		return
	}
	rule.ID = newID()
	rule.CreatedAt = a.now().UTC()

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	switch err := a.svc.CreateRule(ctx, rule); {
	case errors.Is(err, ErrTooManyRules):
		httpError(w, http.StatusTooManyRequests, err.Error())
	case err != nil:
		httpError(w, http.StatusServiceUnavailable, "could not persist rule")
	default:
		writeJSON(w, http.StatusCreated, respond(rule))
	}
}

func validate(req createRequest) (model.AlertRule, string) {
	var r model.AlertRule
	if req.Exchange == "" {
		return r, "exchange is required"
	}
	if _, _, err := exchange.SplitSymbol(req.Symbol); err != nil {
		return r, err.Error()
	}
	if err := r.Direction.UnmarshalText([]byte(req.Direction)); err != nil {
		return r, err.Error()
	}
	price, err := decimal.NewFromString(req.Price)
	if err != nil || !price.IsPositive() {
		return r, "price must be a positive decimal string"
	}
	if len(req.Note) > 200 {
		return r, "note is limited to 200 characters"
	}
	r.Exchange, r.Symbol, r.Price, r.Note = req.Exchange, req.Symbol, price, req.Note
	return r, ""
}

func (a *API) list(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, err := strconv.Atoi(q.Get("limit"))
	if err != nil || limit <= 0 || limit > 1000 {
		limit = 100
	}
	rules := a.svc.ListRules(q.Get("exchange"), q.Get("symbol"), limit)
	out := make([]ruleResponse, len(rules))
	for i, rule := range rules {
		out[i] = respond(rule)
	}
	writeJSON(w, http.StatusOK, out)
}

func (a *API) get(w http.ResponseWriter, r *http.Request) {
	rule, ok := a.svc.GetRule(r.PathValue("id"))
	if !ok {
		httpError(w, http.StatusNotFound, "no such rule (it may have fired)")
		return
	}
	writeJSON(w, http.StatusOK, respond(rule))
}

func (a *API) delete(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	ok, err := a.svc.DeleteRule(ctx, r.PathValue("id"))
	switch {
	case err != nil:
		httpError(w, http.StatusServiceUnavailable, "could not delete rule")
	case !ok:
		httpError(w, http.StatusNotFound, "no such rule (it may have fired)")
	default:
		w.WriteHeader(http.StatusNoContent)
	}
}

func newID() string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func httpError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

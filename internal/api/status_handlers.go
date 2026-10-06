package api

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/wyw14/cry-103/internal/otdr"
)

func (s *Server) systemStatus(writer http.ResponseWriter, _ *http.Request) {
	status, err := s.services.Status()
	if err != nil {
		writeError(writer, http.StatusInternalServerError, err)
		return
	}
	writeJSON(writer, http.StatusOK, status)
}

func (s *Server) getTopology(writer http.ResponseWriter, request *http.Request) {
	snapshot, err := s.services.Topology.Snapshot(chi.URLParam(request, "id"))
	if err != nil {
		writeError(writer, http.StatusNotFound, err)
		return
	}
	writeJSON(writer, http.StatusOK, snapshot)
}

func (s *Server) listScans(writer http.ResponseWriter, request *http.Request) {
	query := request.URL.Query()
	filter := otdr.ScanFilter{
		SpanID:    query.Get("span_id"),
		StationID: query.Get("station_id"),
		State:     otdr.ScanState(query.Get("state")),
	}
	if raw := query.Get("generation"); raw != "" {
		generation, err := strconv.ParseUint(raw, 10, 64)
		if err != nil || generation == 0 {
			writeError(writer, http.StatusBadRequest, fmt.Errorf("invalid generation: %s", raw))
			return
		}
		filter.Generation = generation
	}
	if raw := query.Get("from"); raw != "" {
		from, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			writeError(writer, http.StatusBadRequest, fmt.Errorf("invalid from timestamp: %s", raw))
			return
		}
		filter.From = &from
	}
	if raw := query.Get("to"); raw != "" {
		to, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			writeError(writer, http.StatusBadRequest, fmt.Errorf("invalid to timestamp: %s", raw))
			return
		}
		filter.To = &to
	}
	limit := 0
	if raw := query.Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 {
			writeError(writer, http.StatusBadRequest, fmt.Errorf("invalid scan page limit: %s", raw))
			return
		}
		limit = parsed
	}
	page, err := s.services.OTDR.Query(filter, query.Get("cursor"), limit)
	if err != nil {
		writeError(writer, http.StatusBadRequest, err)
		return
	}
	writeJSON(writer, http.StatusOK, page)
}

func (s *Server) getScan(writer http.ResponseWriter, request *http.Request) {
	trace, exists := s.services.OTDR.Get(chi.URLParam(request, "id"))
	if !exists {
		writeJSON(writer, http.StatusNotFound, ErrorResponse{Error: "OTDR scan not found"})
		return
	}
	writeJSON(writer, http.StatusOK, trace)
}

func (s *Server) listEvents(writer http.ResponseWriter, request *http.Request) {
	query := request.URL.Query()
	offset, err := parsePageValue(query.Get("offset"), 0)
	if err != nil {
		writeError(writer, http.StatusBadRequest, err)
		return
	}
	limit, err := parsePageValue(query.Get("limit"), 50)
	if err != nil {
		writeError(writer, http.StatusBadRequest, err)
		return
	}
	page, err := s.services.Events(query.Get("kind"), query.Get("entity_id"), offset, limit)
	if err != nil {
		writeError(writer, http.StatusInternalServerError, err)
		return
	}
	writeJSON(writer, http.StatusOK, page)
}

func (s *Server) repeaterTelemetry(writer http.ResponseWriter, request *http.Request) {
	repeaterID := chi.URLParam(request, "id")
	if _, err := s.services.Repeater(repeaterID); err != nil {
		writeError(writer, http.StatusNotFound, err)
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{
		"repeater_id": repeaterID,
		"samples":     s.services.TelemetryHistory(repeaterID),
	})
}

func parsePageValue(raw string, fallback int) (int, error) {
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 0 {
		return 0, &pageValueError{value: raw}
	}
	return value, nil
}

type pageValueError struct {
	value string
}

func (e *pageValueError) Error() string {
	return "invalid non-negative page value: " + e.value
}

package api

import (
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
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

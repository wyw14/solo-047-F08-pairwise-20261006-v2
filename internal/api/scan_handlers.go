package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

func (s *Server) startScan(writer http.ResponseWriter, request *http.Request) {
	spanID := chi.URLParam(request, "id")
	target, err := s.services.Span(spanID)
	if err != nil {
		writeError(writer, http.StatusNotFound, err)
		return
	}
	var input ScanRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, err)
		return
	}
	if input.StationID == "" {
		input.StationID = "east"
	}
	if input.LengthKM <= 0 {
		input.LengthKM = target.Snapshot().LengthKM
	}
	if input.Chunks <= 0 {
		input.Chunks = 4
	}
	trace, err := s.services.OTDR.Run(request.Context(), spanID, input.StationID, input.LengthKM, input.Chunks)
	if err != nil {
		writeError(writer, http.StatusConflict, err)
		return
	}
	if err := s.services.Persist("otdr-scan", trace.ID, trace); err != nil {
		writeError(writer, http.StatusInternalServerError, err)
		return
	}
	writeJSON(writer, http.StatusCreated, trace)
}

func (s *Server) listScans(writer http.ResponseWriter, request *http.Request) {
	filter, err := parseScanFilter(request.URL.Query())
	if err != nil {
		writeError(writer, http.StatusBadRequest, err)
		return
	}
	writeJSON(writer, http.StatusOK, scanPageResponse(filter, s.services.OTDR.Query(filter)))
}

func (s *Server) getScan(writer http.ResponseWriter, request *http.Request) {
	trace, exists := s.services.OTDR.Get(chi.URLParam(request, "id"))
	if !exists {
		writeJSON(writer, http.StatusNotFound, ErrorResponse{Error: "OTDR scan not found"})
		return
	}
	writeJSON(writer, http.StatusOK, trace)
}

func (s *Server) getSpan(writer http.ResponseWriter, request *http.Request) {
	target, err := s.services.Span(chi.URLParam(request, "id"))
	if err != nil {
		writeError(writer, http.StatusNotFound, err)
		return
	}
	writeJSON(writer, http.StatusOK, target.Snapshot())
}

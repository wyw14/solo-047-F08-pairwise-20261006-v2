package api

import (
	"encoding/base64"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/wyw14/cry-103/internal/otdr"
)

const scanCursorVersion = "v1"

type ScanPageResponse struct {
	State      otdr.TraceStatus `json:"state"`
	Items      []otdr.Trace     `json:"items"`
	NextCursor string           `json:"next_cursor,omitempty"`
}

func parseScanFilter(query url.Values) (otdr.ScanFilter, error) {
	filter := otdr.ScanFilter{
		SpanID:    query.Get("span_id"),
		StationID: query.Get("station_id"),
	}
	if raw := query.Get("generation"); raw != "" {
		generation, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			return filter, fmt.Errorf("invalid generation: %s", raw)
		}
		filter.Generation = generation
	}
	var err error
	if filter.From, err = parseScanTime(query.Get("from")); err != nil {
		return filter, err
	}
	if filter.To, err = parseScanTime(query.Get("to")); err != nil {
		return filter, err
	}
	if !filter.From.IsZero() && !filter.To.IsZero() && filter.From.After(filter.To) {
		return filter, errors.New("invalid time range: from is after to")
	}
	switch state := query.Get("state"); state {
	case "", string(otdr.StatusCompleted):
		filter.State = otdr.StatusCompleted
	case string(otdr.StatusRunning):
		filter.State = otdr.StatusRunning
	default:
		return filter, fmt.Errorf("invalid scan state: %s", state)
	}
	limit, err := parsePageValue(query.Get("limit"), 50)
	if err != nil {
		return filter, err
	}
	filter.Limit = limit
	if raw := query.Get("cursor"); raw != "" {
		cursor, err := decodeScanCursor(raw)
		if err != nil {
			return filter, err
		}
		filter.After = &cursor
	}
	return filter, nil
}

func parseScanTime(raw string) (time.Time, error) {
	if raw == "" {
		return time.Time{}, nil
	}
	parsed, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid time range bound: %s", raw)
	}
	return parsed.UTC(), nil
}

func scanPageResponse(filter otdr.ScanFilter, page otdr.ScanPage) ScanPageResponse {
	response := ScanPageResponse{State: filter.State, Items: page.Items}
	if page.HasMore {
		response.NextCursor = encodeScanCursor(page.NextCursor)
	}
	return response
}

func encodeScanCursor(cursor otdr.ScanCursor) string {
	raw := fmt.Sprintf("%s|%d|%s", scanCursorVersion, cursor.StartedAt.UTC().UnixNano(), cursor.ID)
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

func decodeScanCursor(raw string) (otdr.ScanCursor, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return otdr.ScanCursor{}, errors.New("invalid scan cursor")
	}
	parts := strings.Split(string(decoded), "|")
	if len(parts) != 3 || parts[0] != scanCursorVersion || parts[2] == "" {
		return otdr.ScanCursor{}, errors.New("invalid scan cursor")
	}
	nanos, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return otdr.ScanCursor{}, errors.New("invalid scan cursor")
	}
	return otdr.ScanCursor{StartedAt: time.Unix(0, nanos).UTC(), ID: parts[2]}, nil
}

package nats_service

import (
	"bytes"
	"compress/gzip"
	"io"
	"log"
	"strings"
	"testing"
	"time"
)

func TestGetTime(t *testing.T) {
	msg := &NatsMessage{
		Logger:     log.New(io.Discard, "", 0),
		Parameters: make(map[string]string),
		Body:       []byte("test payload for GetTime"),
	}

	err := GetTime(msg)

	if err != nil {
		t.Errorf("TestGetTime: expected no error, got %v", err)
	}

	if msg.ResponseBody == nil {
		t.Fatalf("TestGetTime: ResponseBody is nil")
	}

	timeStrFull := string(msg.ResponseBody)
	if !strings.HasPrefix(timeStrFull, "current time: ") {
		t.Errorf("TestGetTime: ResponseBody prefix is incorrect. Got: %s", timeStrFull)
	}

	timeStr := strings.TrimPrefix(timeStrFull, "current time: ")
	_, parseErr := time.Parse(time.RFC3339, timeStr)
	if parseErr != nil {
		t.Errorf("TestGetTime: Time format is incorrect. Got: %s, Error: %v", timeStr, parseErr)
	}
}

func TestGetTimeError(t *testing.T) {
	msg := &NatsMessage{
		Logger: log.New(io.Discard, "", 0),
		Body:   []byte("test payload for GetTimeError"),
	}

	svcErr := GetTimeError(msg)

	if svcErr == nil {
		t.Fatal("TestGetTimeError: Expected an error, but got nil")
	}

	// Values from handlers.go for the simulated error
	expectedStatus := 500
	expectedApiStatusCode := 1001
	expectedErrorMessagePart := "simulated error getting time"

	if svcErr.Status != expectedStatus {
		t.Errorf("TestGetTimeError: Status mismatch. Expected %d, Got %d", expectedStatus, svcErr.Status)
	}

	if svcErr.ApiStatusCode != expectedApiStatusCode {
		t.Errorf("TestGetTimeError: ApiStatusCode mismatch. Expected %d, Got %d", expectedApiStatusCode, svcErr.ApiStatusCode)
	}

	if !strings.Contains(svcErr.ErrorMessage, expectedErrorMessagePart) {
		t.Errorf("TestGetTimeError: ErrorMessage mismatch. Expected to contain '%s', Got '%s'", expectedErrorMessagePart, svcErr.ErrorMessage)
	}

	if !strings.Contains(svcErr.InternalErr, expectedErrorMessagePart) {
		t.Errorf("TestGetTimeError: InternalErr mismatch. Expected to contain '%s', Got '%s'", expectedErrorMessagePart, svcErr.InternalErr)
	}
}

func TestGetCompressedResponse(t *testing.T) {
	originalDataStr := "This is a test string for compression. Repeating this string multiple times to make it longer for a better test."
	originalData := []byte(originalDataStr)

	msg := &NatsMessage{
		Logger:     log.New(io.Discard, "", 0),
		Parameters: make(map[string]string),
		Body:       originalData,
	}

	svcErr := GetCompressedResponse(msg)

	if svcErr != nil {
		t.Fatalf("TestGetCompressedResponse: Expected no error, got %v", svcErr)
	}

	if msg.ResponseBody == nil {
		t.Fatal("TestGetCompressedResponse: ResponseBody is nil")
	}

	if len(msg.ResponseBody) == 0 {
		t.Fatal("TestGetCompressedResponse: ResponseBody is empty")
	}

	if bytes.Equal(msg.ResponseBody, originalData) {
		t.Error("TestGetCompressedResponse: ResponseBody is the same as original data, expected compressed data.")
	}

	// Verify decompression
	gz, err := gzip.NewReader(bytes.NewReader(msg.ResponseBody))
	if err != nil {
		t.Fatalf("TestGetCompressedResponse: Could not create gzip reader: %v", err)
	}
	defer gz.Close()

	decompressedData, err := io.ReadAll(gz)
	if err != nil {
		t.Fatalf("TestGetCompressedResponse: Could not decompress data: %v", err)
	}

	if !bytes.Equal(originalData, decompressedData) {
		t.Errorf("TestGetCompressedResponse: Decompressed data does not match original data.\nOriginal: %s\nDecompressed: %s", originalDataStr, string(decompressedData))
	}
}

func TestGetCompressedResponse_EmptyInput(t *testing.T) {
	originalData := []byte("") // Empty input

	msg := &NatsMessage{
		Logger:     log.New(io.Discard, "", 0),
		Parameters: make(map[string]string),
		Body:       originalData,
	}

	svcErr := GetCompressedResponse(msg)

	if svcErr != nil {
		t.Fatalf("TestGetCompressedResponse_EmptyInput: Expected no error, got %v", svcErr)
	}

	if msg.ResponseBody == nil {
		t.Fatal("TestGetCompressedResponse_EmptyInput: ResponseBody is nil")
	}

	// Decompress to verify it's a valid (empty) gzip stream
	gz, err := gzip.NewReader(bytes.NewReader(msg.ResponseBody))
	if err != nil {
		t.Fatalf("TestGetCompressedResponse_EmptyInput: Could not create gzip reader for empty input's response: %v", err)
	}
	defer gz.Close()

	decompressedData, err := io.ReadAll(gz)
	if err != nil {
		t.Fatalf("TestGetCompressedResponse_EmptyInput: Could not decompress data for empty input's response: %v", err)
	}

	if len(decompressedData) != 0 {
		t.Errorf("TestGetCompressedResponse_EmptyInput: Decompressed data for empty input should be empty, got %d bytes", len(decompressedData))
	}
}

func TestGetCompressedResponse_FilePathMissing(t *testing.T) {
	msg := &NatsMessage{
		Logger:     log.New(io.Discard, "", 0),
		Parameters: make(map[string]string), // No "filePath"
		Body:       nil,                     // No direct body input
	}

	svcErr := GetCompressedResponse(msg)

	if svcErr == nil {
		t.Fatal("TestGetCompressedResponse_FilePathMissing: Expected an error, but got nil")
	}

	expectedStatus := 400
	expectedApiStatusCode := 1002 // As defined in handlers.go for missing param
	expectedErrorMessagePart := "filePath parameter is missing"

	if svcErr.Status != expectedStatus {
		t.Errorf("TestGetCompressedResponse_FilePathMissing: Status mismatch. Expected %d, Got %d", expectedStatus, svcErr.Status)
	}
	if svcErr.ApiStatusCode != expectedApiStatusCode {
		t.Errorf("TestGetCompressedResponse_FilePathMissing: ApiStatusCode mismatch. Expected %d, Got %d", expectedApiStatusCode, svcErr.ApiStatusCode)
	}
	if !strings.Contains(svcErr.ErrorMessage, expectedErrorMessagePart) {
		t.Errorf("TestGetCompressedResponse_FilePathMissing: ErrorMessage mismatch. Expected to contain '%s', Got '%s'", expectedErrorMessagePart, svcErr.ErrorMessage)
	}
}

func TestGetCompressedResponse_FilePathInvalid(t *testing.T) {
	// Create a non-existent file path for testing
	invalidFilePath := "/tmp/this/path/should/not/exist/testfile.txt"

	msg := &NatsMessage{
		Logger:     log.New(io.Discard, "", 0),
		Parameters: map[string]string{"filePath": invalidFilePath},
		Body:       nil, // No direct body input
	}

	svcErr := GetCompressedResponse(msg)

	if svcErr == nil {
		t.Fatal("TestGetCompressedResponse_FilePathInvalid: Expected an error for invalid file path, but got nil")
	}

	expectedStatus := 500                                              // Error reading file results in 500
	expectedApiStatusCode := 1003                                      // As defined in handlers.go for file read error
	expectedErrorMessagePart := "file read error"                      // Public error message
	expectedInternalErrPart := "error reading file " + invalidFilePath // Internal error detail

	if svcErr.Status != expectedStatus {
		t.Errorf("TestGetCompressedResponse_FilePathInvalid: Status mismatch. Expected %d, Got %d", expectedStatus, svcErr.Status)
	}
	if svcErr.ApiStatusCode != expectedApiStatusCode {
		t.Errorf("TestGetCompressedResponse_FilePathInvalid: ApiStatusCode mismatch. Expected %d, Got %d", expectedApiStatusCode, svcErr.ApiStatusCode)
	}
	if !strings.Contains(svcErr.ErrorMessage, expectedErrorMessagePart) {
		t.Errorf("TestGetCompressedResponse_FilePathInvalid: ErrorMessage mismatch. Expected to contain '%s', Got '%s'", expectedErrorMessagePart, svcErr.ErrorMessage)
	}
	if !strings.Contains(svcErr.InternalErr, expectedInternalErrPart) {
		t.Errorf("TestGetCompressedResponse_FilePathInvalid: InternalErr mismatch. Expected to contain '%s', Got '%s'", expectedInternalErrPart, svcErr.InternalErr)
	}
}

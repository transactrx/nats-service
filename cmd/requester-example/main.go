package main

import (
	"bytes"
	"compress/gzip"
	"fmt"
	nats_service "github.com/transactrx/nats-service/pkg/nats-service"
	"io"
	"log"
	"strings"
	"time"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	exampleGetTime()
	exampleGetTimeError()
	exampleGetCompressedResponse()
	// exampleWithExceptions is removed as it tested for a non-existent endpoint,
	// which is not relevant when calling handlers directly.
}

func exampleGetTime() {
	fmt.Println("--- Running Test: exampleGetTime ---")
	msg := &nats_service.NatsMessage{
		Logger:     log.New(io.Discard, "", 0), // Corrected to Logger
		Parameters: make(map[string]string),    // Initialize if needed, GetTime doesn't use it
		Body:       []byte("test payload for GetTime"), // Corrected to Body
	}

	svcErr := nats_service.GetTime(msg)

	if svcErr != nil {
		log.Printf("FAIL: exampleGetTime - Expected no error, got: %v", svcErr)
		return
	}

	if msg.ResponseBody == nil || !strings.Contains(string(msg.ResponseBody), "current time:") {
		log.Printf("FAIL: exampleGetTime - Response data is not as expected. Got: %s", string(msg.ResponseBody))
		return
	}

	// Verify time format (basic check)
	timeStr := strings.Replace(string(msg.ResponseBody), "current time: ", "", 1)
	_, err := time.Parse(time.RFC3339, timeStr)
	if err != nil {
		log.Printf("FAIL: exampleGetTime - Time format is incorrect. Got: %s, Error: %v", timeStr, err)
		return
	}

	log.Println("PASS: exampleGetTime")
}

func exampleGetTimeError() {
	fmt.Println("--- Running Test: exampleGetTimeError ---")
	msg := &nats_service.NatsMessage{
		Logger: log.New(io.Discard, "", 0), // Corrected to Logger
		Body:   []byte("test payload for GetTimeError"), // Corrected to Body
	}

	svcErr := nats_service.GetTimeError(msg)

	if svcErr == nil {
		log.Printf("FAIL: exampleGetTimeError - Expected an error, but got nil")
		return
	}

	// Check against the new NatsServiceError structure
	expectedStatus := 500
	expectedApiStatusCode := 1001 // This is the custom code we set in the handler
	expectedErrorMessagePart := "simulated error getting time" // Check for a part of the message

	if svcErr.Status != expectedStatus {
		log.Printf("FAIL: exampleGetTimeError - Status mismatch. Expected %d, Got %d", expectedStatus, svcErr.Status)
		return
	}

	if svcErr.ApiStatusCode != expectedApiStatusCode {
		log.Printf("FAIL: exampleGetTimeError - ApiStatusCode mismatch. Expected %d, Got %d", expectedApiStatusCode, svcErr.ApiStatusCode)
		return
	}

	if !strings.Contains(svcErr.ErrorMessage, expectedErrorMessagePart) {
		log.Printf("FAIL: exampleGetTimeError - ErrorMessage mismatch. Expected to contain '%s', Got '%s'", expectedErrorMessagePart, svcErr.ErrorMessage)
		return
	}

	log.Println("PASS: exampleGetTimeError")
}

func exampleGetCompressedResponse() {
	fmt.Println("--- Running Test: exampleGetCompressedResponse ---")
	originalDataStr := "This is a test string for compression. It needs to be reasonably long to show some compression effect. " +
		"Repeating this string multiple times to make it longer. " +
		"Repeating this string multiple times to make it longer. " +
		"Repeating this string multiple times to make it longer. " +
		"Repeating this string multiple times to make it longer."
	originalData := []byte(originalDataStr)

	msg := &nats_service.NatsMessage{
		Logger:     log.New(io.Discard, "", 0), // Corrected to Logger
		Parameters: make(map[string]string), // Initialize to avoid nil map issues
		Body:       originalData,            // Corrected to Body
	}

	svcErr := nats_service.GetCompressedResponse(msg)

	if svcErr != nil {
		log.Printf("FAIL: exampleGetCompressedResponse - Expected no error, got: %v", svcErr)
		return
	}

	// The Compressed flag is not set on NatsMessage by the handler directly.
	// We will verify by attempting to decompress the ResponseBody.
	// msg.Data is the input request data, it should not be nilled by the handler.
	// The compressed output is in msg.ResponseBody.
	if len(msg.ResponseBody) == 0 {
		log.Printf("FAIL: exampleGetCompressedResponse - ResponseBody is empty")
		return
	}

	// Verify decompression
	gz, err := gzip.NewReader(bytes.NewReader(msg.ResponseBody))
	if err != nil {
		log.Printf("FAIL: exampleGetCompressedResponse - Could not create gzip reader: %v", err)
		return
	}
	defer gz.Close()

	decompressedData, err := io.ReadAll(gz)
	if err != nil {
		log.Printf("FAIL: exampleGetCompressedResponse - Could not decompress data: %v", err)
		return
	}

	if !bytes.Equal(originalData, decompressedData) {
		log.Printf("FAIL: exampleGetCompressedResponse - Decompressed data does not match original data.")
		log.Printf("Original: %s", originalDataStr)
		log.Printf("Decompressed: %s", string(decompressedData))
		return
	}

	if len(msg.ResponseBody) >= len(originalData) {
		log.Printf("WARN: exampleGetCompressedResponse - Compressed data is not smaller than original. Original: %d, Compressed: %d. This might happen for small or already compressed data.", len(originalData), len(msg.ResponseBody))
		// This is not a strict fail, as compression effectiveness varies.
	}

	log.Println("PASS: exampleGetCompressedResponse")
}

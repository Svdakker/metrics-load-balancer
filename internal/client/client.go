package client

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
)

type Client struct {
	httpClient *http.Client
}

func New() *Client {
	return &Client{
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				IdleConnTimeout:     90 * time.Second,
				MaxIdleConnsPerHost: 20,
			},
		},
	}
}

func (c *Client) Decode(r io.Reader) (*prompb.WriteRequest, error) {
	compressed, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}

	uncompressed, err := snappy.Decode(nil, compressed)
	if err != nil {
		return nil, fmt.Errorf("snappy decode: %w", err)
	}

	var req prompb.WriteRequest
	if err := req.Unmarshal(uncompressed); err != nil {
		return nil, fmt.Errorf("protobuf unmarshal: %w", err)
	}

	return &req, nil
}

func (c *Client) Pack(req *prompb.WriteRequest) ([]byte, error) {
	if req == nil {
		return nil, fmt.Errorf("cannot pack nil request")
	}

	data, err := req.Marshal()
	if err != nil {
		return nil, fmt.Errorf("marshal: %w", err)
	}

	return snappy.Encode(nil, data), nil
}

func (c *Client) Push(ctx context.Context, url string, payload []byte) error {
	if len(payload) == 0 {
		return nil
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	httpReq.Header.Set("Content-Encoding", "snappy")
	httpReq.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("remote write returned %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

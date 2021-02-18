// Copyright 2018 The Go Cloud Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package paramstore

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/service/ssm"
	"github.com/Lioric/go-cloud/internal/testing/setup"
	"github.com/Lioric/go-cloud/runtimevar"
	"github.com/Lioric/go-cloud/runtimevar/drivertest"
)

// This constant records the region used for the last --record.
// If you want to use --record mode,
// 1. Update this constant to your AWS region.
// TODO(issue #300): Use Terraform to get this.
const region = "us-east-2"

type harness struct {
	client  *Client
	session client.ConfigProvider
	closer  func()
}

func newHarness(t *testing.T) (drivertest.Harness, error) {
	sess, done := setup.NewAWSSession(t, region)
	client := NewClient(context.Background(), sess)
	return &harness{client: client, session: sess, closer: done}, nil
}

func (h *harness) MakeVar(ctx context.Context, name string, decoder *runtimevar.Decoder) (*runtimevar.Variable, error) {
	return h.client.NewVariable(ctx, name, decoder, nil)
}

func (h *harness) CreateVariable(ctx context.Context, name string, val []byte) error {
	svc := ssm.New(h.session)
	_, err := svc.PutParameter(&ssm.PutParameterInput{
		Name:      aws.String(name),
		Type:      aws.String("String"),
		Value:     aws.String(string(val)),
		Overwrite: aws.Bool(true),
	})
	return err
}

func (h *harness) UpdateVariable(ctx context.Context, name string, val []byte) error {
	return h.CreateVariable(ctx, name, val)
}

func (h *harness) DeleteVariable(ctx context.Context, name string) error {
	svc := ssm.New(h.session)
	_, err := svc.DeleteParameter(&ssm.DeleteParameterInput{Name: aws.String(name)})
	return err
}

func (h *harness) Close() {
	h.closer()
}

func TestConformance(t *testing.T) {
	drivertest.RunConformanceTests(t, newHarness)
}

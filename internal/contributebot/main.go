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

// contributebot is a service for keeping the Go Cloud project tidy.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"

	"cloud.google.com/go/pubsub"
	"github.com/google/go-github/github"
)

const userAgent = "Lioric/go-cloud Contribute Bot"

type flagConfig struct {
	project      string
	subscription string
	gitHubAppID  int64
	keyPath      string
}

func main() {
	addr := flag.String("listen", ":8080", "address to listen for health checks")
	var cfg flagConfig
	flag.StringVar(&cfg.project, "project", "", "GCP project for topic")
	flag.StringVar(&cfg.subscription, "subscription", "contributebot-github-events", "subscription name inside project")
	flag.Int64Var(&cfg.gitHubAppID, "github_app", 0, "GitHub application ID")
	flag.StringVar(&cfg.keyPath, "github_key", "", "path to GitHub application private key")
	flag.Parse()
	if cfg.project == "" || cfg.gitHubAppID == 0 || cfg.keyPath == "" {
		fmt.Fprintln(os.Stderr, "contributebot: must specify -project, -github_app, and -github_key")
		os.Exit(2)
	}

	ctx := context.Background()
	w, server, cleanup, err := setup(ctx, cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer cleanup()
	log.Printf("Serving health checks at %s", *addr)
	go server.ListenAndServe(*addr, w)
	log.Fatal(w.receive(ctx))
}

// worker contains the connections used by this server.
type worker struct {
	sub  *pubsub.Subscription
	auth *gitHubAppAuth
}

// receive listens for events on its subscription and handles them.
func (w *worker) receive(ctx context.Context) error {
	return w.sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		id := msg.Attributes["X-GitHub-Delivery"]
		eventType := msg.Attributes["X-GitHub-Event"]
		if eventType == "integration_installation" || eventType == "integration_installation_repositories" {
			// Deprecated event types. Ignore them in favor of supported ones.
			log.Printf("Skipped event %s of deprecated type %s", id, eventType)
			msg.Ack()
			return
		}
		event, err := github.ParseWebHook(eventType, msg.Data)
		if err != nil {
			log.Printf("Parsing %s event %s: %v", eventType, id, err)
			msg.Nack()
			return
		}
		var handleErr error
		switch event := event.(type) {
		case *github.IssuesEvent:
			handleErr = w.receiveIssueEvent(ctx, event)
		case *github.PingEvent, *github.InstallationEvent, *github.PullRequestEvent, *github.CheckSuiteEvent:
			// No-op.
		default:
			log.Printf("Unhandled webhook event type %s (%T) for %s", eventType, event, id)
			msg.Nack()
			return
		}
		if handleErr != nil {
			log.Printf("Failed processing %s event %s: %v", eventType, id, handleErr)
			msg.Nack()
			return
		}
		msg.Ack()
		log.Printf("Processed %s event %s", eventType, id)
	})
}

func (w *worker) receiveIssueEvent(ctx context.Context, e *github.IssuesEvent) error {
	// An issueRule is a rule about what to do for a given event.
	// Rules are executed in random order to ensure there are no dependencies.
	type issueRule struct {
		// name is the rule's name for error messages.
		name string

		// condition reports true if the event is "interesting" to this rule,
		// meaning that the worker should fetch the latest information about
		// the issue and call the run function.
		condition func(*github.IssuesEvent) bool

		// run executes the rule. The issue data is retrieved before any
		// rules are executed, so may be ever-so-slightly stale.
		run func(context.Context, *github.Client, issueRuleData) error
	}

	rules := []issueRule{
		// Remove "in progress" label from closed issues.
		{
			name: "remove in progress label from closed",
			condition: func(e *github.IssuesEvent) bool {
				return e.GetAction() == "closed" && hasLabel(e.GetIssue(), inProgressLabel)
			},
			run: removeInProgressLabel,
		},
	}

	// Check conditions to see if issue data is needed.
	// No need to consume API quota if events aren't relevant.
	runs := make([]bool, len(rules))
	runCount := 0
	for i := range rules {
		if rules[i].condition(e) {
			runs[i] = true
			runCount++
		}
	}
	if runCount == 0 {
		return nil
	}

	// Retrieve the current issue state.
	client := w.ghClient(e.GetInstallation().GetID())
	owner := e.GetRepo().GetOwner().GetLogin()
	repoName := e.GetRepo().GetName()
	num := e.GetIssue().GetNumber()
	iss, _, err := client.Issues.Get(ctx, owner, repoName, num)
	if err != nil {
		return err
	}

	// Execute relevant rules.
	ok := true
	data := issueRuleData{
		issue: iss,
		owner: owner,
		repo:  repoName,
	}
	for i := range runs {
		if !runs[i] {
			continue
		}
		if err := rules[i].run(ctx, client, data); err != nil {
			ok = false
			log.Printf("Issue rule %q failed on %s/%s#%d: %v", rules[i].name, owner, repoName, num, err)
		}
	}
	if !ok {
		return fmt.Errorf("one or more rules failed for %s/%s#%d", owner, repoName, num)
	}
	log.Printf("Applied %d relevant rules on %s/%s#%d successfully", runCount, owner, repoName, num)
	return nil
}

const inProgressLabel = "in progress"

// issueRuleData is the information passed to an issue rule.
type issueRuleData struct {
	repo  string
	owner string
	issue *github.Issue
}

// removeInProgressLabel removes the "in progress" label from closed issues.
func removeInProgressLabel(ctx context.Context, client *github.Client, data issueRuleData) error {
	if data.issue.GetState() != "closed" || !hasLabel(data.issue, inProgressLabel) {
		return nil
	}
	num := data.issue.GetNumber()
	_, err := client.Issues.RemoveLabelForIssue(ctx, data.owner, data.repo, num, inProgressLabel)
	if err != nil {
		return err
	}
	return nil
}

// ghClient creates a GitHub client authenticated for the given installation.
func (w *worker) ghClient(installID int64) *github.Client {
	c := github.NewClient(&http.Client{Transport: w.auth.forInstall(installID)})
	c.UserAgent = userAgent
	return c
}

func hasLabel(iss *github.Issue, label string) bool {
	for i := range iss.Labels {
		if iss.Labels[i].GetName() == label {
			return true
		}
	}
	return false
}

// ServeHTTP serves a page explaining that this port is only open for health checks.
func (w *worker) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	if req.URL.Path != "/" {
		http.NotFound(resp, req)
		return
	}
	const responseData = `<!DOCTYPE html>
<title>Go Cloud Contribute Bot Worker</title>
<h1>Go Cloud Contribute Bot Worker</h1>
<p>This HTTP port is only open to serve health checks.</p>`
	resp.Header().Set("Content-Length", fmt.Sprint(len(responseData)))
	resp.Header().Set("Content-Type", "text/html; charset=utf-8")
	io.WriteString(resp, responseData)
}

func (w *worker) CheckHealth() error {
	return w.auth.CheckHealth()
}

# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

`virgo4-solr-push` is a Go service (single `main` package in `cmd/virgo4-solr-push/`) that reads Solr update documents from an AWS SQS queue, batches them, and POSTs them to a Solr core's `/update` endpoint. The same binary is deployed as several ECS tasks (Virgo4 default/image cores, Mandala kmassets/kmterms), each configured as either an "add" or "delete" pusher via environment variables.

## Commands

- `make build` / `make darwin`: build `bin/virgo4-solr-push.darwin` (amd64, with `-race`)
- `make linux`: build the static linux binary (this is what the Dockerfile uses)
- `make fmt`, `make vet`: run gofmt / go vet on the cmd package
- `make check`: run staticcheck (`-checks all,-S1002,-ST1003`) and the `shadow` vet analyzer
- `make dep`: update dependencies, then `go mod tidy` and `go mod verify`
- `docker build -f package/Dockerfile -t virgo4-solr-push --build-arg BUILD_TAG=<tag> .`: build the container image

The repo has no tests.

## Configuration

All configuration comes from environment variables, loaded in `config.go`. Every variable is required and the process exits if one is missing, except `SOLR_PUSH_SUBDOC_ID_DELIMITER`. Some semantics that aren't obvious from the names:
- `VIRGO4_SOLR_PUSH_SOLR_MODE` is used directly as the XML wrapper tag, e.g. `add` produces `<add>...</add>` and `delete` produces `<delete>...</delete>`. SQS message payloads must already be the inner XML fragments (`<doc>...</doc>`, `<id>...</id>`, etc.).
- `VIRGO4_SOLR_PUSH_SOLR_BUFFER_SIZE` is in MB.
- Setting `..._SOLR_COMMIT_TIME=0` turns off explicit client commits. Setting `..._SOLR_COMMIT_WITHIN_TIME=0` drops the `commitWithin` attribute.
- `VIRGO4_SQS_MESSAGE_BUCKET` is the S3 bucket the `virgo4-sqs-sdk` uses for oversized messages.

The version string comes from a `buildtag.*` file in the working directory, which the Dockerfile creates.

## Architecture

**Flow:** `main.go` polls SQS in batches and feeds messages into a buffered channel. `VIRGO4_SOLR_PUSH_WORKERS` goroutines (`worker.go`) consume from that channel. Each worker has its own `SOLR` instance (`solr-interface.go` → `solr-factory.go`), and its own buffer and commit state.

**Worker loop (`worker.go`):** each received message is appended to the Solr buffer and to a `queued` list of SQS messages. SQS messages are deleted only after Solr accepts them, so anything not deleted is redelivered by SQS. A flush happens when the block count, buffer size, or flush time is reached (`IsTimeToAdd`). Commits are driven separately by `IsTimeToCommit`.

**Failure recovery:** this is the most involved logic. It is split between `solr-protocol.go` (classifying Solr's response) and `worker.go` (acting on the result).
- `processResponsePayload` reads the Solr XML error message with regexes:
  - `[N,M]` means one document failed at 1-based document number N. This returns `ErrDocumentAdd`.
  - `[doc=ID]` or `document id ID` means Solr rejected the whole batch because of document ID. This returns `ErrAllDocumentAdd`.
  - An HTTP 400 is also treated as `ErrAllDocumentAdd`.
- On `ErrDocumentAdd`, the worker deletes the SQS messages before the failed one, drops the failed one, and re-buffers and retries the rest.
- On `ErrAllDocumentAdd`, the worker finds the failing message by its `AttributeKeyRecordId` and removes it, then retries the remainder.
  - If `SubDocIdDelimiter` is set (Mandala nested documents), the parent ID is taken from the sub-document ID first.
  - If the failed doc can't be identified, the whole batch is abandoned without being deleted from SQS, so SQS will redeliver it.
- Any other error is fatal (`fatalIfError` → `log.Fatalf`), and the container restarts. New Solr error formats that the regexes don't recognize end up here; the code logs a request to add handling for them.

HTTP GET/POST retry up to 3 times, but only for the network error strings listed in `canRetry`.

`xmlquery.DisableSelectorCache = true` is set in `main` because the selector cache isn't thread-safe across workers. Keep this setting.

## Deployment

`pipeline/buildspec.yml` (AWS CodeBuild) builds the image, pushes it to ECR, and records the build tag in SSM. `pipeline/deployspec.yml` clones `uvalib/terraform-infrastructure` and runs `terraform apply` for each staging ECS task that uses this image. If you add or remove a deployment target, update that list. Changes to the Go/Alpine version are made in `package/Dockerfile` (and `go.mod`).

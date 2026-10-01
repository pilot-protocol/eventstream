# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Removed
- Governed publication, which existed for Pilot's hosted control plane (now
  retired) and has been unreachable since pilotprotocol v1.14.0 stopped
  configuring it:
  - `Service.SetGovernedPublication`, `SetGovernedReceiptRecorder`,
    `SetGovernedContentInspector`, `RequireGovernedContentInspection` and
    `SetGovernedTransferQuota`;
  - `Client.PublishGoverned` and `PublishGovernedWithDisclosure`;
  - `GovernedEvent`, `GovernedTopic`, `DecisionEventVerifier`, the receipt
    recorder interfaces and the envelope encode/decode helpers.
- The module no longer imports `github.com/pilot-protocol/common/decision`.

### Unchanged
- The topic `"\x00pilot.governed.v1"` stays reserved. A broker refuses a
  publication to it with `pubsub.publish_denied` and never fans it out, as a
  broker with no verifier configured already did.

## [v0.1.0]

Initial release.

# Samsa Changelog

## [Unreleased]
### Changed
- [#39](https://github.com/CallistoLabsNYC/samsa/issues/39) Consumer cannot stream when caller only has a reference to a wrapper struct

## [0.1.2] - 2024-03-09
### Added
- [#25](https://github.com/CallistoLabsNYC/samsa/issues/25) Offer a way to read from a Consumer without streaming
- [#14](https://github.com/CallistoLabsNYC/samsa/issues/14) Implement headers in fetch and produce

### Changed
- [#26](https://github.com/CallistoLabsNYC/samsa/issues/26) StreamMessage struct should be named ConsumeMessage
- [#22](https://github.com/CallistoLabsNYC/samsa/issues/22) Include docker-compose stack in the CICD

### Fixed
- [#19](https://github.com/CallistoLabsNYC/samsa/issues/19) Fix busted integration tests

## [0.1.1] - 2024-03-02
### Fixed
- [#20](https://github.com/CallistoLabsNYC/samsa/issues/20) Cannot perform ConsumerBuilder::build() via a reference
- [#10](https://github.com/CallistoLabsNYC/samsa/issues/10) Export the TopicPartitions type
- [#23](https://github.com/CallistoLabsNYC/samsa/issues/23) Update producer message version

## [0.1.0] - 2024-02-26
### Added
- Initial release
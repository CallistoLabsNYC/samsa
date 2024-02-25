#!/bin/bash
#
# Usage: ./run-all-tests <Kafka versions separated by ':'>...
#
# This script will automatically set up the Docker environment for Kafka, and then
# run all cargo tests, including integration tests, for each Kafka version specified.
# For example, running:
#
# ./run-all-tests 0.8.2.2:0.9.0.0
#
# will run the tests against Kafka versions 0.8.2.2 and 0.9.0.0. The arguments
# should correspond to a Docker tag of the `wurstmeister/kafka` image, so any valid
# tag will do—i.e.,
#
# ./run-all-tests latest
#
# will run against the most recent Kafka version published in the
# `wurstmeister/kafka` repo. If there are no versions specified, it runs the tests
# on a set of default versions.

set -e

stop_docker() {
  docker-compose down
}

start_docker() {
  # pull zookeeper and build the kafka image
  docker-compose pull
  docker-compose build redpanda-1
  docker-compose build redpanda-2
  docker-compose build redpanda-console
  docker-compose up -d

  # wait for Kafka to be ready and the test topic to be created
  ./do_until_success.sh "docker-compose logs redpanda-1 | grep 'Successfully started Redpanda!'"
  ./do_until_success.sh "docker-compose logs redpanda-2 | grep 'Successfully started Redpanda!'"
}

setup() {
  # use a subshell so the working directory changes back after it exits
  (
    cd "$(dirname $0)"
    stop_docker # just in case something went wrong with a previous shutdown

    start_docker
  )
}

teardown() {
  # use a subshell so the working directory changes back after it exits
  (
    cd "$(dirname $0)"
    stop_docker
  )
}

### START TEST ###

# need to run tests serially to avoid the tests stepping on each others' toes
export RUST_TEST_THREADS=1

echo -n "Running tests with Redpanda"
setup || {
       teardown
        exit 0
}

cargo test --features integration_tests || {
        teardown
        exit 0
}

teardown


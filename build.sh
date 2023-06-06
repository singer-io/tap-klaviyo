#!/bin/bash

set -e

function unit_tests() {
  echo "Unit tests - TO BE IMPLEMENTED"
}

function integration_tests() {
  echo "Integration tests - TO BE IMPLEMENTED"
}

function all_test() {
  unit_tests
  integration_tests
}

function publish() {
  echo "Publish package to artifactory - TO BE IMPLEMENTED"
}

if [[ "$1" == "--unit-tests" ]]; then
  unit_tests
elif [[ "$1" == "--integration-tests" ]]; then
  integration_tests
elif [[ "$1" == "--test" ]]; then
  all_test
elif [[ "$1" == "--deploy" ]]; then
  publish
fi

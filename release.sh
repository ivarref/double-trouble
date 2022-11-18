#!/usr/bin/env bash

set -euo pipefail

clojure -X:test
clojure -T:build jar
clojure -T:build deploy

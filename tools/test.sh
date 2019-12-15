#!/usr/bin/env bash

pytest tests --cov=pyspark_util --verbose --showlocals --doctest-modules pyspark_util

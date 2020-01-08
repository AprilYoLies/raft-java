#!/usr/bin/env bash

jps | grep ServerMain | cut -c 1-5 | xargs kill -9
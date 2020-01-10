#!/usr/bin/env bash

jps | grep ServerMain | cut -f 1 -d " " | xargs kill -9
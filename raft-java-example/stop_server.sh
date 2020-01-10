#!/usr/bin/env bash

jps | grep ServerMain | cut -f 1 -d " " | xargs kill -9

ROOT_DIR=./env
cd $ROOT_DIR || exit
rm -fr ./*

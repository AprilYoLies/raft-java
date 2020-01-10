#!/usr/bin/env bash

jps | grep ServerMain | cut -c 1-5 | xargs kill -9

ROOT_DIR=./env

cd $ROOT_DIR || exit
rm -fr ./*
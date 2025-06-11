#!/bin/bash

SCRIPT_PATH="$(cd "$(dirname "$0")"; pwd)"

if [ -z "$(echo ${PYTHONPATH} | grep sdk)" ]; then
    echo "PYTHONPATH to the tfos package is not set. Source mysetup"
    exit 1
fi

pylint --rcfile=${SCRIPT_PATH}/.pylintrc \
    --disable=missing-module-docstring --disable=missing-function-docstring \
    --disable=duplicate-code --disable=too-many-arguments \
    --disable=too-few-public-methods \
    bios \
    || pylint-exit --error-fail --warn-fail $?

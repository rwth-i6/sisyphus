#!/bin/bash

nohup $(dirname $0)/background.sh $*&
echo $! > pid

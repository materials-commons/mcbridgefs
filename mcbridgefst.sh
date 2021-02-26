#!/usr/bin/env bash

cmd/mcbridgefs/mcbridgefs -t $1 $2 
/usr/bin/fusermount -u $2
rm -rf --preserve-root $2


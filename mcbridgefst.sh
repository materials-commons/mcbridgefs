#!/usr/bin/env bash

cmd/mcbridgefs/mcbridgefs -t $1 $2 > $3 2>&1
/usr/bin/fusermount -u $2 >> $3 2>&1
rm -rf --preserve-root $2 >> $3 2>&1


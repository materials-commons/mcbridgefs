#!/usr/bin/env bash

/usr/local/bin/mcbridgefs -g $1 $2 
/usr/bin/fusermount -u $2


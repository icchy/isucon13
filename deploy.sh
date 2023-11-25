#!/bin/bash

HOST=i1
APPDIR=/home/isucon/webapp

# isupipe
scp webapp/go/isupipe $HOST:/tmp/_isupipe
ssh $HOST mv /tmp/_isupipe $APPDIR/go/isupipe

# db
rsync -av --delete -e ssh webapp/sql/ $HOST:$APPDIR/sql

ssh $HOST make prebench

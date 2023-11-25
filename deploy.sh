#!/bin/bash

HOST=i1
APPDIR=/home/isucon/webapp

# isupipe
cd webapp/go
make build
cd ../..
scp webapp/go/isupipe $HOST:/tmp/_isupipe
ssh $HOST mv /tmp/_isupipe $APPDIR/go/isupipe

# isudns
scp isudns/isudns $HOST:/tmp/_isudns
ssh $HOST mv /tmp/_isudns $HOME/isudns

# db
rsync -av --delete -e ssh webapp/sql/ $HOST:$APPDIR/sql

ssh $HOST make prebench

#!/bin/bash

HOST=i3
APPDIR=/home/isucon/webapp

# isupipe
scp webapp/go/isupipe $HOST:/tmp/_isupipe
ssh $HOST mv /tmp/_isupipe $APPDIR/go/isupipe
ssh $HOST make prebench


# db
rsync -av --delete -e ssh webapp/sql/ $HOST:$APPDIR/sql

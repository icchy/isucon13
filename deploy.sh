#!/bin/bash

#HOST=i1
APPDIR=/home/isucon/webapp

# isupipe
ISUPIPE_HOST=i1
cd webapp/go
make build
cd ../..
scp webapp/go/isupipe i2:/tmp/_isupipe
ssh i2 mv /tmp/_isupipe $APPDIR/go/isupipe

# isudns
cd isudns
GOOS=linux GOARCH=amd64 go build .
cd ../
scp isudns/isudns i1:/tmp/_isudns
ssh i1 mv /tmp/_isudns /home/isucon/isudns

# db
rsync -av --delete -e ssh webapp/sql/ i2:$APPDIR/sql

ssh i1 make prebench
ssh i2 make prebench
ssh i3 make prebench

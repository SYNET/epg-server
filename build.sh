#!/bin/sh

cd modules
./setup.py build
cp build/lib.linux-i686-2.6/*.so ..

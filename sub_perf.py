#!/usr/bin/env python

import os
import sys
import time
sys.path.append(os.path.join('..','puka'))

import pats

sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)


loop=100000
loop_hash=10000

received = 1

start = None

def main():
    p = pats.Pats('amqp:///')
    p.x_connect()

    def on_test(body, _r, _t):
        global received, start
        if received == 1:
            start = time.time()
            print "Started receiving"
        received += 1
        if received == loop:
            return done()
        if received % loop_hash == 0:
            print '+',

    p.subscribe('test', callback=on_test)

    def done():
        print
        td = time.time() - start
        ms = (td / loop)*1000.0
        hz = loop / td
        print "Test completed: %.2fms, %i msgs/sec" % (ms, hz)
        p.loop_break()


    print "Waiting for %i messages." % \
        (loop,)
    p.loop()


if __name__ == '__main__':
    main()

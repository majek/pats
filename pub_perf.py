#!/usr/bin/env python

import os
import sys
import time
sys.path.append(os.path.join('..','puka'))

import pats

sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)


loop=100000
loop_hash=10000
data = '-ok-'
parallelism = 1000

to_drain = loop

def main():
    p = pats.Pats('amqp:///')
    p.x_connect()

    def drain():
        global to_drain
        while to_drain > 0:
            to_drain -= 1
            if to_drain % loop_hash == 0:
                print '+',
            if to_drain % parallelism != 0:
                p.publish('test', data)
            else:
                p.publish('test', data, callback=drain)
                return
        p.publish('test', data, done)

    def done():
        print
        td = time.time() - start
        ms = (td / loop)*1000.0
        hz = loop / td
        print "Publishing completed: %.2fms, %i msgs/sec" % (ms, hz)
        p.loop_break()


    print "Sending %i messages of size %i bytes, %i in parallel" % \
        (loop, len(data), parallelism)
    start = time.time()
    drain()
    p.loop()


if __name__ == '__main__':
    main()

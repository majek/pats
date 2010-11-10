#!/usr/bin/env python

import os
import sys
import time
sys.path.append(os.path.join('..','puka'))

import pats

sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)

loop=10000
loop_hash = 1000
drain = loop-1

def main():

    p = pats.Pats('amqp:///')
    p.x_connect()


    def on_test(body, reply_to, topic):
        p.publish(reply_to)
    p.subscribe('test', callback=on_test)

    def send_request():
        def on_reply(body):
            global drain
            drain -= 1
            if drain == 0:
                done()
            else:
                send_request()
                if drain % loop_hash == 0:
                    print '+',
        p.request('test', callback=on_reply)

    def done():
        print
        ms = ((time.time() - start) / loop)*1000.0
        print "Test completed: %.2fms" % (ms,)
        p.loop_break()

    print "Sending %i request/responses" % (loop,)
    start = time.time()
    send_request()
    p.loop()


if __name__ == '__main__':
    main()

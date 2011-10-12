#!/usr/bin/env python
# -*- coding: UTF-8 -*-
#
# Copyright 2011 Synesis LLC.
#
# Technical support and updates: http://synet.synesis.ru
# You are free to use this software for evaluation and commercial purposes
# under condition that it is used only in conjunction with digital TV
# receivers running SYNET middleware by Synesis.
# 
# To contribute modifcations, additional modules and derived works please
# contact pnx@synesis.ru

import tornado.httpserver
import tornado.ioloop
import tornado.web
import tornado.database
import sys
import binwriter
import server_helper
import server_plain_req
import server_ctg_req

class MainHandler(tornado.web.RequestHandler):
    def db_connect(self):
        return tornado.database.Connection('localhost', 'epg',
            user = 'epg',
            password = 'epg')
    def initialize(self):
        self.db = None

    #TODO: Move to server_helper
    def validate_args(self):
        for param in self.request.arguments.keys():
            if not param in server_helper.where_params and \
                not param in server_helper.paging_params and \
                not param in server_helper.other_params:
                raise tornado.web.HTTPError(404)
        for param in server_helper.int_params:
            try:
                if self.request.arguments.has_key(param):
                    for arg in self.request.arguments[param]:
                        i = int(arg)
            except:
                raise tornado.web.HTTPError(404)
        if self.request.arguments.has_key('limit') and int(self.request.arguments['limit'][0]) > server_helper.limit_max:
            self.request.arguments['limit'] = [ str(self.limit_max) ]

    def get(self, location):
        writers = {
            'epg' : binwriter.row2bin,
            'epg_py' : lambda row: '%s\n' % row,
        }
        content_types = {
            'epg' : 'application/octet-stream',
            'epg_py' : 'text/plain',
        }

        query_db = server_plain_req.query_db
        if self.request.arguments.has_key('ctg_id'):
            query_db = server_ctg_req.query_db

        if location not in ('epg', 'epg_py'):
            raise tornado.web.HTTPError(404)
        self.validate_args()
        self.set_header("Content-Type", content_types[location])
        if not self.db:
            self.db = self.db_connect()

        rows = query_db(self.db, self.request.arguments)
        writer = writers[location]
        #ts1 = time.time()
        #rows = self.db.query(req, *args)
        #ts2 = time.time()
        #print req, args
        #if len(rows) > 0 and rows[0].has_key('COUNT'):
        #    print 'Count is %d' % rows[0]['COUNT']
        #print 'Request took %f ms, %d rows' % ((ts2 - ts1) * 1000, len(rows))
        #print req % tuple(args)
        for row in rows:
            self.write(writer(row))

application = tornado.web.Application([
    (r"/(.*)", MainHandler),
])

if __name__ == "__main__":
    port = 8080
    address = '0.0.0.0'
    if len(sys.argv) >= 2:
        port = int(sys.argv[1])
    if len(sys.argv) >= 3:
        address = sys.argv[2]
    http_server = tornado.httpserver.HTTPServer(application)
    http_server.listen(port, address=address)
    tornado.ioloop.IOLoop.instance().start()

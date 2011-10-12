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

import MySQLdb
import xmltv
from dateutil import parser
import sys
import time, calendar
import codecs
from xml.etree.cElementTree import ElementTree, Element, SubElement, tostring

def catch_db(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except MySQLdb.Error, e:
            sys.stderr.write('DB error %d: %s\n' % (e.args[0], e.args[1]))
            sys.exit(1)
    wrapper.__name__ = func.__name__
    wrapper.__module__ = func.__module__
    wrapper.__dict__ = func.__dict__
    wrapper.__doc__ = func.__doc__
    return wrapper

@catch_db
def db_connect():
    return MySQLdb.connect(host = 'localhost',
        user = 'epg',
        passwd = 'epg',
        db = 'epg',
        use_unicode = True,
        charset = 'utf8')

@catch_db
def create_tables(conn):
    cursor = conn.cursor()
    cursor.execute('DROP TABLE IF EXISTS programs;')
    cursor.execute('DROP TABLE IF EXISTS categories;')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS programs
        (
            pr_id INTEGER UNSIGNED PRIMARY KEY AUTO_INCREMENT,
            start INTEGER UNSIGNED NOT NULL,
            end INTEGER UNSIGNED NOT NULL,
            aux_id VARCHAR(128) NOT NULL,
            cat_id INTEGER UNSIGNED NOT NULL,
            rating INTEGER UNSIGNED NOT NULL,
            title TEXT NOT NULL,
            title_l VARCHAR(16) NOT NULL,
            descr TEXT NOT NULL,
            icon TEXT NOT NULL,
            descr_l VARCHAR(16) NOT NULL,
            INDEX aux_id_start_idx(aux_id, start),
            INDEX start_idx(start),
            INDEX end_idx(end)
        ) DEFAULT CHARSET=utf8;
        ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS categories
        (
            id INTEGER UNSIGNED PRIMARY KEY AUTO_INCREMENT,
            ctg_id INTEGER UNSIGNED NOT NULL,
            pr_id INTEGER UNSIGNED NOT NULL,
            start INTEGER UNSIGNED NOT NULL,
            end INTEGER UNSIGNED NOT NULL,
            INDEX ctgid_prid (ctg_id, pr_id),
            INDEX start_idx(start),
            INDEX start_ctdid_idx(ctg_id, start),
            INDEX end_idx(end),
            UNIQUE KEY ctdid_prid_uniq(ctg_id, pr_id)
        ) DEFAULT CHARSET=utf8;
        ''')

@catch_db
def insert_programs(conn, programs, categories):
    cursor = conn.cursor()
    prog_id = 1
    for p in programs:
        title = title_lang = descr = descr_lang = icon = ''
        cat_id = 0
        rating = 0

        if p.has_key('catalog_id'):
            cat_id = int(p['catalog_id'][0])

        if p.has_key('icon') and p['icon'][0].has_key('src'):
            icon = p['icon'][0]['src']

        aux_id = codecs.utf_8_encode(p['channel'])[0]

        start = int(calendar.timegm(parser.parse(p['start']).utctimetuple()))
        end = int(calendar.timegm(parser.parse(p['stop']).utctimetuple()))

        if p.has_key('title'):
            title = codecs.utf_8_encode(p['title'][0][0])[0]
            title_lang = codecs.utf_8_encode(p['title'][0][1])[0]

        if p.has_key('desc'):
            if p['desc'][0][0]:
                descr = codecs.utf_8_encode(p['desc'][0][0])[0]
            if p['desc'][0][1]:
                descr_lang = codecs.utf_8_encode(p['desc'][0][1])[0]

        if p.has_key('genre'):
            genres = p['genre']
            for g in genres:
                if not g:
                    sys.stderr.write('Empty categories are not allowed\n')
                    continue
                if g not in categories.keys():
                    sys.stderr.write('Category %s is not known\n' % g.encode('UTF-8'))
                    continue
                if categories[g].has_key('parent'):
                    parents = categories[g]['parent']
                    # if category has only one parent, 
                    # we insert in db pairs: (ctg_id, prog_id), (parent_ctg_id, prog_id)
                    if (len(parents) == 1):
                        cursor.execute('''REPLACE INTO categories(ctg_id, pr_id, start, end)
                            VALUES(%s, %s, %s, %s);''', (categories[parents[0]]['id'], prog_id, start, end))
                        #TODO: remove after moving to new ctg_id scheme
                        cursor.execute('''REPLACE INTO categories(ctg_id, pr_id, start, end)
                            VALUES(%s, %s, %s, %s);''', (categories[g]['id'], prog_id, start, end))
                        if parents[0] not in genres:
                            id = (categories[parents[0]]['id'] << 16) | categories[g]['id']
                            cursor.execute('''REPLACE INTO categories(ctg_id, pr_id, start, end)
                                VALUES(%s, %s, %s, %s);''', (id, prog_id, start, end))

                    # otherwise we insert in db pair (parent_ctg_id << 16 | ctg_id, prog_id)
                    for parent in parents:
                        if parent in genres:
                            id = (categories[parent]['id'] << 16) | categories[g]['id']
                            cursor.execute('''REPLACE INTO categories(ctg_id, pr_id, start, end)
                                VALUES(%s, %s, %s, %s);''', (id, prog_id, start, end))

                else:
                    cursor.execute('''REPLACE INTO categories(ctg_id, pr_id, start, end)
                        VALUES(%s, %s, %s, %s);''', (categories[g]['id'], prog_id, start, end))
        else:
            for c in categories.keys():
                if aux_id in categories[c]['channels']:
                    cursor.execute('''REPLACE INTO categories(ctg_id, pr_id, start, end)
                        VALUES(%s, %s, %s, %s);''', (categories[c]['id'], prog_id, start, end))

        cursor.execute('''INSERT INTO programs (pr_id, start, end,
            aux_id, cat_id, rating,
            title, title_l,
            descr, descr_l, icon)
            VALUES (%s, %s, %s,
            %s, %s, %s,
            %s, %s,
            %s, %s, %s)''',
            (prog_id, start, end, aux_id, cat_id, rating,
            title, title_lang, descr, descr_lang, icon))
        prog_id += 1
    cursor.execute('''CHECK TABLE categories''')
    cursor.execute('''REPAIR TABLE categories''')
    cursor.execute('''CHECK TABLE programs''')
    cursor.execute('''REPAIR TABLE programs''')

    cursor.close()
    conn.commit()

def parse_category(elem, parent, categories):
    retval = {}
    channels = []
    if 'channel' in elem.keys() and elem.get('channel') == 'true':
        for channel in elem.findall('Channel'):
            channels.append(channel.get('id'))
    try:
        retval['id'] = int(elem.get('id'))
    except ValueError:
        sys.stderr.write('Category ID is not integer!\n')
        sys.exit(-1)

    if retval['id'] < 1 or retval['id'] > 65535:
        sys.stderr.write('ID of category %s is out of bounds!\n' % elem.get('name').encode('UTF-8'))
        sys.exit(-1)

    retval['channels'] = channels
    if parent:
        if not 'channel' in parent.keys():
            sys.stderr.write('ERROR! only channel category can be top category!\n')
            sys.exit(-1)
        retval['parent'] = [ parent.get('name') ]
    if categories.has_key(elem.get('name')):
        old_cat = categories[elem.get('name')]
        if old_cat['id'] != retval['id']:
            sys.stderr.write('ERROR! found categories %s with same name but different id!\n' % elem.get('name').encode('UTF-8'))
            sys.exit(-1)
        if len(old_cat['channels']) != 0 or len(retval['channels']):
            sys.stderr.write('ERROR! found duplicate channel categories!\n')
            sys.exit(-1)
        if not retval.has_key('parent') or not old_cat.has_key('parent'):
            sys.stderr.write('ERROR! found duplicate categories of different level!\n')
        old_cat['parent'].extend(retval['parent'])
        categories[elem.get('name')] = old_cat
    else:
        categories[elem.get('name')] = retval

    for category in elem.findall('Category'):
        parse_category(category, elem, categories)
    return categories

def parse_categories(fn):
    categories = {}
    et = ElementTree()
    tree = et.parse(fn)
    for elem in tree.findall('Category'):
        parse_category(elem, None, categories)
    return categories

def usage():
    sys.stderr.write('Usage:\n')
    sys.stderr.write('\t%s xmltv.xml categories.xml\n' % sys.argv[0])

if __name__ == '__main__':
    if len(sys.argv) != 3:
        usage()
        sys.exit(-1)

    print 'Connecting to DB...'
    conn = db_connect()

    print '(Re-)Creating tables...'
    create_tables(conn)

    print 'Parsing categories...'
    try:
        categories = parse_categories(sys.argv[2])
    except Exception as e:
        print 'Error while parsing %s: %s' % (sys.argv[2], e)
        conn.close()
        sys.exit(-1)

    print 'Parsing EPG from %s...' % sys.argv[1]

    try:
        programmes = xmltv.read_programmes(open(sys.argv[1], 'r'))
    except Exception as e:
        print 'Error while parsing %s: %s' % (sys.argv[2], e)
        conn.close()
        sys.exit(-1)

    programmes.sort(key = lambda program : int(calendar.timegm(parser.parse(program['start']).utctimetuple())))

    print 'Inserting data to DB...'
    insert_programs(conn, programmes, categories)
    conn.close()

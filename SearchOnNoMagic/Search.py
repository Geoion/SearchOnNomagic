#encoding = utf-8
#import MySQLdb
import torndb as database
import pprint
import re
import uuid
import json
import nomagic
from setting import conn
from setting import ring
import simplejson
import string

_NUMBER = len(ring)

db = database.Connection("localhost","qishu1","root","root")
select_line = 'SELECT body FROM qishu1.entities WHERE auto_increment=%s'


def _number(key): 
    return int(key, 16) % _NUMBER

def _unpack(data): 
    return json.loads(data)

def _key(data): 
    return data

def _keyword(data):
    return "%"+data+"%"

def _get_entity_by_id(entity_id):
    entity = ring[_number(entity_id)].get("SELECT body FROM entities WHERE id = %s", _key(entity_id))
    return _unpack(entity["body"]) if entity else None

def _get_entities_by_ids(entity_ids):
    entities = []

    for h in range(_NUMBER):
        ids = [str(i) for i in entity_ids if h == _number(i)]

        if len(ids) > 1:
            entities.extend([(i["id"], _unpack(i["body"])) \
                for i in ring[h].query("SELECT * FROM entities WHERE id IN %s" % str(tuple(ids)))])
        elif len(ids) == 1:
            entity = _get_entity_by_id(ids[0])
            entities.extend([(ids[0], entity)] if entity else [])

    entities = dict(entities)
    return [(i, entities[i]) for i in entity_ids]

def _get_entitiy_by_keyword(keyword_str):
    for i in range(0,_NUMBER):
        entity = ring[i].get("SELECT body FROM entities WHERE body LIKE %s", _keyword(keyword_str))
        return _unpack(entity["body"]) if entity else None

#d = _get_entity_by_id('444fc84fff0c415ba5bbb55636a4bf82')
#d = _get_entitiy_by_keyword('\u6587\u6863')
#d = _get_entitiy_by_keyword(u"文档")
#print d

'''
Implements temporary data stores for generating the Cesium point cloud.
'''

import logging
import functools
import geohash
import datetime

SOLR_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
SOLR_TIME_FORMAT2 = "%Y-%m-%dT%H:%M:%SZ"

#https://hyde.cyverse.org/isamples_central/thing/select?q=*:*&q.op=OR&facet=true&facet.field=hasContextCategory&rows=0
CONTEXT_VALUES = {
    "not provided": 0,
    "site of past human activities": 1,
    "earth interior": 2,
    "subaerial surface environment":3,
    "marine water body bottom": 4,
    "marine water body":5,
    "lake river or stream bottom":6,
    "terrestrial water body":8,
    "active human occupation site":9,
    "lake, river or stream bottom":10,
    "subsurface fluid reservoir":11,
    "marine biome":12,
    "subaerial terrestrial biome":13,
}

MATERIAL_VALUES = {
    "not Provided":0,
    "organic material":1,
    "biogenic non organic material":2,
    "rock":3,
    "anthropogenic material":4,
    "mineral":5,
    "sediment":6,
    "water":7,
    "soil":8,
    "anthropogenic metal":9,
    "gaseous material":10,
    "biogenic non-organic material":11,
    "particulate":13,
    "non-aqueous liquid material":14,
    "ice":15,
}

SPECIMEN_VALUES = {
    "not provided":0,
    "organism part":1,
    "whole organism":3,
    "other solid object":4,
    "artifact":5,
    "aggregation":6,
    "biome aggregation":7,
    "anthropogenic aggregation":8,
    "analytical preparation":9,
    "liquid or gas sample":10,
    "organism product":11,
    "experiment product":12,
}

SOURCE_VALUES = {
    "geome":1,
    "sesar":2,
    "opencontext":3,
    "smithsonian":4,
}

@functools.cache
def parseSolrDateTime(sdt):
    if sdt is None:
        return None
    try:
        dt = datetime.datetime.strptime(sdt, SOLR_TIME_FORMAT)
    except ValueError:
        dt = datetime.datetime.strptime(sdt, SOLR_TIME_FORMAT2)
    return dt


@functools.cache
def parseCoords(xy):
    if xy is None:
        return None,None
    try:
        y,x = xy.split(",")
        y = float(y)
        x = float(x)
        assert (x>-180.0 and x<180.0)
        assert (y>-90.0 and y<90.0)
    except:
        return None,None
    return x,y


@functools.cache
def getHash(x,y, precision=11):
    if x is None or y is None:
        return None
    return geohash.encode(y, x, precision=precision)

def parseContext(cv):
    try:
        tv = cv[0].strip().lower()
    except IndexError:
        return 0
    v = CONTEXT_VALUES.get(tv, 0)
    return v

def parseMaterial(cv):
    try:
        tv = cv[0].strip().lower()
    except IndexError:
        return 0
    v = MATERIAL_VALUES.get(tv, 0)
    return v

def parseSpecimen(cv):
    try:
        tv = cv[0].strip().lower()
    except IndexError:
        return 0
    v = SPECIMEN_VALUES.get(tv, 0)
    return v

def parseSource(cv):
    try:
        tv = cv.strip().lower()
    except Exception:
        return 0
    v = SOURCE_VALUES.get(tv, 0)
    return v

def encode5(a, b, c):
    v = a + (b << 5) + (c << 10)
    return v

def decode5(v):
    a = v & 31
    b = v & (31 << 5)
    c = v & (31 << 10)
    return a,b,c

def yearDay(sdt):
    dt = parseSolrDateTime(sdt)
    if dt is None:
        return None, None
    return dt.year, dt.timetuple().tm_yday

def transformSourceRecord(s, geohash_precision=11):
    r = {
        "id": s.get("id"),
        "source": parseSource(s.get("source")),
        "tstamp": parseSolrDateTime(s["indexUpdatedTime"]),
        "ststamp": s["indexUpdatedTime"]
    }
    x,y = parseCoords(s.get("XY"))
    if x is None:
        return None
    if y is None:
        return None
    r["x"] = x
    r["y"] = y
    r["g"] = getHash(x,y, precision=geohash_precision)
    a = parseContext(s.get("context", []))
    b = parseSpecimen(s.get("specimen", []))
    c = parseMaterial(s.get("material", []))
    r["csm"] = encode5(a,b,c)
    r["year"],r["day"] = yearDay(s.get("producedBy_resultTime"))
    return r


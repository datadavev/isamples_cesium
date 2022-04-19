'''Model definitions used by icesium the the intermediate data stores.

Samples: Model for sample records the be converted to cesium point cloud
Heights: Model for Cesium heights for locations

'''

import logging
import datetime
import sqlmodel
import sqlalchemy.exc #.NoResultFound
import sqlalchemy

L = logging.getLogger("models")

_MISSING_HEIGHT = -9999.0

class Heights(sqlmodel.SQLModel, table=True):
    geohash:str = sqlmodel.Field(primary_key=True)
    latitude: float = sqlmodel.Field(default=None)
    longitude: float = sqlmodel.Field(default=None)
    height: float = sqlmodel.Field(default=_MISSING_HEIGHT)


class Samples(sqlmodel.SQLModel, table=True):
    identifier:str = sqlmodel.Field(primary_key=True, description="Record unique identifier")
    tstamp: datetime.datetime = sqlmodel.Field(None, description="Time record was updated in source index")    
    geohash: str = sqlmodel.Field(default=None, foreign_key=Heights.geohash, index=True)
    source: int = sqlmodel.Field(default=None, description="Source collection, 8bits, 256 values allowed -> classification")
    vocabs: int = sqlmodel.Field(default=None, description="5 bits each for the three vocabs, 16bit word -> intensity")
    syear: int = sqlmodel.Field(default=None, description="Integer year of sample, 16 bits -> red")
    sday: int = sqlmodel.Field(default=None, description="Integer day of year, 16 bits -> green")
    w3: int = sqlmodel.Field(default=0, description="16 bits -> blue")


def create_db_and_tables(engine):
    sqlmodel.SQLModel.metadata.create_all(engine)

def mostRecentlyIndexedSample(session):
    result = session.query(sqlalchemy.func.max(Samples.tstamp))
    return result.one()[0]


def addSample(session, source):
    hash_val = source.get("g", None)
    if hash_val is None:
        L.warning("No geohash for %s", source['id'])
        return False
    query = sqlmodel.select(Heights).where(Heights.geohash == hash_val)
    res = session.exec(query)
    try:
        height = res.one()
    except sqlalchemy.exc.NoResultFound:
        height = None
    if height is None:
        # create new height entry
        #sql = "INSERT INTO heights(geohash,longitude,latitude,height) VALUES (:geohash,:x,:y,:z);"
        #session.execute(sql, {"geohash":hash_val, "x":source.get("x"), "y":source.get("y"), "z":_MISSING_HEIGHT})
        height = Heights(geohash=hash_val, latitude=source.get("y"), longitude=source.get("x"))
        session.add(height)
        # Always commit heights so they are available for new samples
        # Initial load will be slow, but subsequent operations should be much faster
        session.commit()
    query = sqlmodel.select(Samples).where(Samples.identifier == source['id'])
    res = session.exec(query)
    try:
        sample = res.one()
    except sqlalchemy.exc.NoResultFound:
        # no sample entry, add one
        sample = Samples(
            identifier=source['id'],
            geohash=hash_val,
            tstamp=source['tstamp'],
            source=source['source'],
            vocabs=source['csm'],
            syear=source['year'],
            sday=source['day']
        )
        #sql = (
        #    "INSERT INTO samples(identifier,geohash,tstamp,source,vocabs,syear,sday) "
        #    "VALUES (:id,:g,:ststamp,:source,:csm,:year,:day);"
        #)
        #session.execute(sql, source)
        session.add(sample)
        # Commit outside of here
    else:
        L.info("Sample %s already present", source['id'])
    return True

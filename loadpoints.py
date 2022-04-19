'''
Script to generate a .las file from an iSamples query
'''

import sys
import typing
import urllib
import logging
import csv
import requests
import click
import sqlmodel
import sqlmodel.sql.expression
import icesium
import icesium.models

sqlmodel.sql.expression.SelectOfScalar.inherit_cache = True  # type: ignore
sqlmodel.sql.expression.Select.inherit_cache = True  # type: ignore

BASE_URL = 'https://hyde.cyverse.org/isamples_central/'
PAGE_SIZE = 10000
MAX_RECORDS = 8000000
GEO_PRECISION = 11
DEFAULT_FIELDS = [
    "id",
    "XY:producedBy_samplingSite_location_ll",
    "source",
    "context:hasContextCategory",
    "material:hasMaterialCategory",
    "specimen:hasSpecimenCategory",
    "producedBy_resultTime",
    "indexUpdatedTime"
]

def getLogger():
    return logging.getLogger("points")

LOG_LEVELS = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "WARN": logging.WARNING,
    "ERROR": logging.ERROR,
    "FATAL": logging.CRITICAL,
    "CRITICAL": logging.CRITICAL,
}
LOG_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"
LOG_FORMAT = "%(asctime)s %(name)s:%(levelname)s: %(message)s"

L = getLogger()


def initialize_logging(verbosity: typing.AnyStr):
    logging.basicConfig(
        level=LOG_LEVELS.get(verbosity, logging.INFO),
        format=LOG_FORMAT,
        datefmt=LOG_DATE_FORMAT,
    )
    verbosity = verbosity.upper()
    if verbosity not in LOG_LEVELS.keys():
        L.warning("%s is not a log level, set to INFO", verbosity)


def get_solr_url(base_url, path_component: str):
    return urllib.parse.urljoin(base_url, path_component)


def fetch_solr_records(
    rsession=None,
    base_url=BASE_URL,
    q: typing.Optional[str] = None,
    fq: typing.Optional[str] = None,
    start_index: int = 0,
    batch_size: int = PAGE_SIZE,
    fields: typing.Optional[list] = None,
    sort: typing.Optional[str] = None
):
    if rsession is None:
        rsession = requests.session()
    headers = {"Accept": "application/json"}
    query = q
    if q is None:
        q = "*:*"
    params = {
        "q": query,
        "rows": batch_size,
        "start": start_index,
    }
    if fields is not None:
        params["fl"] = ",".join(fields)
    if sort is not None:
        params["sort"] = sort
    if fq is not None:
        params["fq"] = fq
    _url = get_solr_url(base_url, "thing/select")
    res = rsession.get(_url, headers=headers, params=params)
    L.debug("URL = %s", res.url)
    json = res.json()
    docs = json["response"]["docs"]
    num_found = json["response"]["numFound"]
    has_next = start_index + len(docs) < num_found
    return docs, has_next


class ISBCoreSolrRecordIterator:
    """
    Iterator class for looping over all the Solr records in the ISB core Solr schema
    """

    def __init__(
        self,
        rsession=None,
        base_url=BASE_URL,
        q: str = None,
        fq: str = None,
        batch_size: int = PAGE_SIZE,
        offset: int = 0,
        fields = None,
        sort: str = None,
        max_records = MAX_RECORDS,
    ):
        """
        Args:
            rsession: The requests.session object to use for sending the solr request
            authority_id: The authority_id to use when querying SOLR, defaults to all
            batch_size: Number of documents to fetch at a time
            offset: The offset into the records to begin iterating
            sort: The solr sort parameter to use
        """
        self.rsession = rsession
        if self.rsession is None:
            self.rsession = requests.session()
        self.base_url = base_url
        self.q = q
        self.fq = fq
        self.fields = fields
        self.batch_size = batch_size
        self.offset = offset
        self.sort = sort
        self._current_batch = []
        self._current_batch_index = -1
        self.max_records = max_records
        self._total_retrieved = 0

    def __iter__(self):
        return self

    def __next__(self) -> typing.Dict:
        if len(self._current_batch) == 0 or self._current_batch_index == len(
            self._current_batch
        ):
            self._current_batch = fetch_solr_records(
                rsession = self.rsession,
                base_url= self.base_url,
                q = self.q,
                fq = self.fq,
                start_index = self.offset,
                batch_size = self.batch_size,
                fields = self.fields,
                sort = self.sort
            )[0]
            if len(self._current_batch) == 0:
                # reached the end of the records
                raise StopIteration
            L.info(
                f"Just fetched {len(self._current_batch)} ISB Core solr records at offset {self.offset}"
            )
            self.offset += self.batch_size
            self._current_batch_index = 0
        # return the next one in the list and increment our index
        next_record = self._current_batch[self._current_batch_index]
        self._current_batch_index = self._current_batch_index + 1
        self._total_retrieved = self._total_retrieved + 1
        if self._total_retrieved > self.max_records:
            L.info("Exceeded max records of %s", self.max_records)
            raise StopIteration
        return next_record


def getPoints(base_url=BASE_URL, q="*:*", fq=None, fields=None, max_records=MAX_RECORDS):
    ctx = click.get_current_context()
    engine = ctx.obj.get("engine", None)
    if engine is None:
        L.error("No database engine available. Results are not preserved.")
        return
    fq = None
    with sqlmodel.Session(engine) as dbsession:
        # Find the newest record. We query for records that are more recent.
        try:
            tmax = icesium.models.mostRecentlyIndexedSample(dbsession)
            stmax = tmax.strftime(icesium.SOLR_TIME_FORMAT)
            fq = f"indexUpdatedTime:[{stmax} TO *]"
            L.info("Starting with records dating from: %s", fq)
        except Exception as e:
            L.error(e)
            pass
    _session = requests.session()
    if fields is None:
        fields = DEFAULT_FIELDS
    records = ISBCoreSolrRecordIterator(
        rsession=_session,
        base_url=base_url,
        q=q,
        fq=fq,
        fields=fields,
        max_records=max_records,
        sort="indexUpdatedTime asc"
    )
    counter = 0
    with sqlmodel.Session(engine) as dbsession:
        for source_record in records:
            record = icesium.transformSourceRecord(source_record)
            counter = counter + 1
            if record is not None:
                icesium.models.addSample(dbsession, record)
                dbsession.commit()
                #print(record)
            else:
                L.warning("Failed to parse record for %s", source_record.get("id"))
            if counter % 100 == 0:
                print(f"Processed {counter} records")

def uniqueLocations(records):
    pass

@click.group()
@click.pass_context
@click.option(
    "-v", "--verbosity", default="INFO", help="Specify logging level", show_default=True
)
@click.option("-r", "--records", default="records.sqlite", help="Path to records sqlite db", show_default=True)
def main(ctx, verbosity, records):
    ctx.ensure_object(dict)
    initialize_logging(verbosity)
    ctx.obj["records_db"] = f"sqlite:///{records}"
    ctx.obj["engine"] = sqlmodel.create_engine(ctx.obj["records_db"])
    icesium.models.create_db_and_tables(ctx.obj["engine"])

@main.command("get")
@click.option(
    "-q","--query",default=None, help="Query to execute", show_default=True
)
@click.option(
    "-x", "--maxrecs", default=MAX_RECORDS, help="Max records to retrieve", type=int, show_default=True
)
@click.option(
    "-c","--count", help="Count records to retrieve but don't do anything", is_flag=True
)
@click.pass_context
def getRecords(ctx, query, maxrecs, count):
    if query is None:
        query = "producedBy_samplingSite_location_ll:[* TO *]"
    getPoints(q=query, max_records=maxrecs)


@main.command("csv")
@click.pass_context
def generateLidarCSV(ctx):

    def _nz(v):
        if v is None:
            return 0
        return v

    def _zh(z, n):
        z = _nz(z)
        return 10 + z + n*0.3

    def _sd(v):
        return int(v*179.0573770491803)

    def _sy(v):
        if v >= 2022:
            return 65535        
        if v < 1022:
            return 0
        return int((v-1022)*65.535)

    offsets = {}
    ctx = click.get_current_context()
    engine = ctx.obj.get("engine", None)
    if engine is None:
        L.error("No database engine available. Results are not preserved.")
        return
    _writer = csv.writer(sys.stdout, delimiter=' ', quoting=csv.QUOTE_MINIMAL)
    header = ["X","Y","Z","Classification","Intensity","Red","Green","Blue", ]
    _writer.writerow(header)
    counter = 0
    with sqlmodel.Session(engine) as dbsession:
        statement = sqlmodel.select(icesium.models.Samples, icesium.models.Heights).where(icesium.models.Samples.syear == 2022).join(icesium.models.Heights)
        results = dbsession.exec(statement)
        for s,h in results:
            _year = _nz(s.syear)
            if _year > 0:
                n = offsets.get(s.geohash,0)
                n = n +1
                offsets[s.geohash] = n
                row = [
                    h.longitude,       #x
                    h.latitude,        #y
                    _zh(h.height, n),  #z
                    _nz(s.source),     #classification
                    _nz(s.vocabs),     #intensity
                    _sy(_year),        #red
                    _sd(_nz(s.sday)),  #green
                    _nz(s.w3),         #blue
                ]
                _writer.writerow(row)
                counter = counter + 1
                if counter % 100000 == 0:
                    L.info("Written %s records", counter)
    L.info("Written %s records", counter)



if __name__ == "__main__":
    sys.exit(main())


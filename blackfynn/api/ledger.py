# -*- coding: utf-8 -*-

from blackfynn.api.base import APIBase
from blackfynn.models import LedgerEntry

class LedgerAPI(APIBase):
    base_uri = "/ledger"
    name = 'ledger'

    def create_ledger_entry(self, entry):
        return self._post( self._uri('/entries'), json=entry.as_dict())

    def get_ledger_entries(self, orgId, start, end):
        s = start.replace(microsecond=0).isoformat() + 'Z' #the 'Z' is because python's datetime doesn't conform to the ISO 8601 standard
        e = end.replace(microsecond=0).isoformat()  + 'Z'
        uri = self._uri('/entries?orgId={orgId}&start={start}&end={end}', orgId=orgId, start=s,end=e)
        return [ LedgerEntry.from_dict( r ) for r in self._get( uri ) ]


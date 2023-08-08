import logging
import argparse

import asyncio
import aiohttp
import pandas as pd

from constants import (
    COMPANY_LIST,
    API_KEY,
    FULL_ENDPOINT,
    LOOKUP_ENDPOINT,
    SEARCH_ENDPOINT,
    RUNNING_MODE,
    EXPECTED_FIELDS,
    PARQUET_PARTITION_COLS,
    LOGGING_FORMAT,
    LOG_LEVEL,
)


async def extract_fields(raw_data) -> pd.DataFrame:
    """
    ● Organization permalink
    ● Organization website url
    ● Date the website url was updated
    ● Organization LinkedIn page url
    ● Organization city, region and country
    """
    # TODO: can be refactored
    return pd.DataFrame.from_dict(
        {
            'name': raw_data['identifier']['value'],
            'created_at': raw_data['created_at'],
            'permalink': raw_data['identifier']['permalink'],
            'website': raw_data['website_url'],
            'updated_at': raw_data['updated_at'],
            'linkedin': raw_data['linkedin'],
            'location_identifiers': ', '.join(
                f"{e['location_type']}: {e['value']}"
                for e in raw_data['location_identifiers']
            ),
            'short_description': raw_data['short_description'],
            'elt_timestamp': (pd.Timestamp.now(tz='UTC')).floor('H'),
        }
    )


class CrunchbaseConnector:
    def __init__(self, api_key: str = API_KEY):
        self.api_key = api_key
        self.destination = "destination/"
        self.header = {
            'accept': 'application/json',
            'Content-Type': 'application/json',
            'X-cb-user-key': self.api_key,
        }

        self.args = self.parse_arguments()

        if self.args.mode not in RUNNING_MODE:
            raise ValueError(f"Mode ({self.args.mode}) must be one of '{RUNNING_MODE}'")

        self.mode = self.args.mode

        self.logger = self.init_logging()

    def init_logging(self):
        logging.basicConfig(level=LOG_LEVEL, format=LOGGING_FORMAT)
        logging.getLogger(self.mode).setLevel(LOG_LEVEL)
        return logging.getLogger(self.mode)

    async def unload(self, data_list: list):
        res_df = pd.DataFrame(columns=EXPECTED_FIELDS)
        for d in data_list:
            df = await extract_fields(d)
            res_df = pd.concat([df, res_df], ignore_index=True)

        res_df.to_parquet(
            path=self.destination,
            index=False,
            compression='gzip',
            engine='pyarrow',
            partition_cols=PARQUET_PARTITION_COLS,
        )

    async def fetch_all(self, session):
        payload = {
            "field_ids": [
                "name",
                "identifier",
                "permalink",
                "linkedin",
                "entity_def_id",
                "updated_at",
                "location_identifiers",
                "created_at",
                "short_description",
            ],
            "order": [{"field_id": "rank_org", "sort": "asc"}],
            "limit": 1000,
        }

        res_list = []

        idx = 0
        while True:
            data = await self.post_request(
                session=session, url=FULL_ENDPOINT, payload=payload
            )
            count = data['count']
            await asyncio.sleep(1)
            for entity in data['entities']:
                for company in COMPANY_LIST:
                    if entity['properties']['name'].lower() == company.lower():
                        res_list.append(await self.fetch_data(session, entity['uuid']))
                payload['after_id'] = entity['uuid']
            idx += payload['limit']
            if len(res_list) == len(COMPANY_LIST) or idx >= count:
                break
        return res_list

    async def fetch_data(self, session, entity_id):
        params = {
            "field_ids": [
                "name",
                "categories",
                "identifier",
                "permalink",
                "linkedin",
                "entity_def_id",
                "updated_at",
                "location_identifiers",
                "created_at",
                "website",
            ],
            "card_ids": "fields",
        }

        res = await self.get_request(
            session, url=LOOKUP_ENDPOINT.format(entity_id=entity_id), params=params
        )
        if res.get('cards'):
            data = res['cards']['fields']
        else:
            data = res['fields']
        return data

    async def get_request(
        self,
        session: aiohttp.ClientSession,
        url: str,
        params: dict = None,
        payload: dict = None,
    ):
        async with session.get(
            url,
            params=params if params else {},
            json=payload if payload else {},
            headers=self.header,
        ) as response:
            return await response.json()

    async def post_request(
        self,
        session: aiohttp.ClientSession,
        url: str,
        params: dict = None,
        payload: dict = None,
    ):
        async with session.post(
            url,
            params=params if params else {},
            json=payload if payload else {},
            headers=self.header,
        ) as response:
            return await response.json()

    async def fetch_permalink(self, session, company):
        # This is autocompletes endpoint, which doesn't support pagination

        params = {
            'query': company,
            'collection_ids': 'organization.companies',
            'limit': 10,
        }

        permalink = None
        uuid = None

        res = await self.get_request(
            session, url=SEARCH_ENDPOINT, params=params, payload={}
        )
        organizations = res['entities'] if res.get('entities') else []
        for org in organizations:
            if org.get('identifier').get('value').lower() == company.lower():
                permalink = org.get('identifier').get('permalink')
                uuid = org.get('identifier').get('uuid')
                break
        return permalink, uuid

    async def get_data_by_companies(self, session):
        res_list = []
        for company in COMPANY_LIST:
            permalink, uuid = await self.fetch_permalink(session, company)
            res_list.append(await self.fetch_data(session, permalink or uuid))
        return res_list

    async def _get_data(self):
        self.logger.info(f"Starting fetching data in the '{self.mode}' mode...")
        try:
            async with aiohttp.ClientSession() as session:

                if self.mode.lower() == 'specific':
                    data = await self.get_data_by_companies(session)
                else:
                    data = await self.fetch_all(session)

            await self.unload(data)

            self.logger.info(f"Successfully unloaded to '{self.destination}'")

        except Exception as e:
            self.logger.error(f"Error: {e}")
            raise e

    def get_data(self):
        return asyncio.run(self._get_data())

    def parse_arguments(self):
        """Returns the parsed arguments from the command line"""

        arg_parser = argparse.ArgumentParser(prog="CrunchbaseConnector",
                                             conflict_handler='resolve')

        arg_parser.add_argument("--mode", help="Date of process",
                                default='specific')
        args = arg_parser.parse_args()
        return args


def execute(**kwargs):
    CrunchbaseConnector(**kwargs).get_data()


if __name__ == '__main__':
    execute()

LOGGING_FORMAT = '%(asctime)s %(levelname)s %(name)s: %(message)s'
LOG_LEVEL = 'INFO'

COMPANY_LIST = ['Superside']

SEARCH_ENDPOINT = 'https://api.crunchbase.com/api/v4/autocompletes'
LOOKUP_ENDPOINT = 'https://api.crunchbase.com/api/v4/entities/organizations/{entity_id}'
FULL_ENDPOINT = 'https://api.crunchbase.com/api/v4/searches/organizations'

API_KEY = '8df662e61ffe5ab9be29ae9c0335b4f4'  # better to use AWS Secrets Manager and extract keys and tokens from there

RUNNING_MODE = ['full', 'specific']

EXPECTED_FIELDS = [
    "name",
    "created_at",
    "permalink",
    "website",
    "updated_at",
    "linkedin",
    "location_identifiers",
    "short_description",
    "elt_timestamp",
]

PARQUET_PARTITION_COLS = ['elt_timestamp']

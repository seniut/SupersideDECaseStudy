import pytest
from unittest.mock import AsyncMock
from unittest.mock import Mock
from crunchbase import CrunchbaseConnector

# TODO: can be refactored


@pytest.fixture
def mock_session():
    return Mock()


@pytest.fixture
def connector():
    return CrunchbaseConnector()


@pytest.mark.asyncio
async def test_fetch_data(mock_session, connector):
    entity_id = 'entity_id'
    response_json = {'cards': {'fields': ['data']}}

    request_mock = AsyncMock()
    request_mock.__aenter__.return_value = request_mock
    request_mock.json.return_value = response_json

    session = mock_session
    session.get.return_value = request_mock

    data = await connector.get_request(session, entity_id)

    assert data == response_json


@pytest.mark.asyncio
async def test_fetch_permalink(mock_session, connector):
    company = 'company'
    response_json = {'entities': [{'identifier': {'value': 'company', 'permalink': 'permalink', 'uuid': 'uuid'}}]}

    request_mock = AsyncMock()
    request_mock.__aenter__.return_value = request_mock
    request_mock.json.return_value = response_json

    session = mock_session
    session.get.return_value = request_mock

    permalink, uuid = await connector.fetch_permalink(session, company)
    assert permalink == response_json['entities'][0]['identifier']['permalink']
    assert uuid == response_json['entities'][0]['identifier']['uuid']


@pytest.mark.asyncio
async def test_get_data_by_companies(mock_session, connector):
    response_json = {'entities': [{'identifier': {'value': 'Superside', 'permalink': 'permalink', 'uuid': 'uuid'}}],
                     'fields': {'data': 'data'}}

    request_mock = AsyncMock()
    request_mock.__aenter__.return_value = request_mock
    request_mock.json.return_value = response_json

    session = mock_session
    session.get.return_value = request_mock

    data = await connector.get_data_by_companies(session)
    assert data == [response_json['fields']]


@pytest.mark.asyncio
async def test_get_request(mock_session, connector):
    url = 'http://example.com'
    response_json = {'key': 'value'}

    request_mock = AsyncMock()
    request_mock.__aenter__.return_value = request_mock
    request_mock.json.return_value = response_json

    session = mock_session
    session.get.return_value = request_mock

    resp = await connector.get_request(session, url)
    assert resp == response_json


if __name__ == '__main__':
    pytest.main()

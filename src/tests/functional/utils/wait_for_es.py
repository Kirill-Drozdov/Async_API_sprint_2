from elasticsearch import Elasticsearch

from tests.functional.settings import test_settings
from tests.functional.utils.helpers import ping_remote_service_host

if __name__ == '__main__':
    ping_remote_service_host(
        service=Elasticsearch(hosts=test_settings.es_host),
        service_name='Elasticsearch',
    )

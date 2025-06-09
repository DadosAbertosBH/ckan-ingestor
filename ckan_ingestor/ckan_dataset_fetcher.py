import http.client
import json

from ckan_ingestor.dataset_fetcher import DatasetFetcher


class CkanDatasetFetcher(DatasetFetcher):
    url: str

    def __init__(self, url: str):
        super().__init__()
        self.url = url

    def do_fetch(self) -> list[dict[str, any]]:
        packages = self.__fetch_request("package_search?rows=10000")["result"]["results"]
        return packages

    @staticmethod
    def __fetch_request(action: str) -> dict[str, any]:
        conn = http.client.HTTPSConnection("dados.pbh.gov.br")
        payload = ''
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:130.0) Gecko/20100101 Firefox/130.0',
            'Content-Type': 'application/json'
        }
        conn.request("GET", f"/api/action/{action}", payload, headers)
        res = conn.getresponse()
        data = json.loads(res.read())
        return data

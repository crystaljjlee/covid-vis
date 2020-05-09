import csv
import html
import json
import time
import urllib.request
from datetime import date, timedelta
from datetime import datetime, timezone
from pathlib import Path


class RedditScraper:

    def __init__(self,
               search_term=None, subreddit=None, number_of_results_per_day=1000,
               start_date='2020-01-01', end_date='2030-01-01',
               min_score=0, sort_by='score', filename=None
               ):
        """

        :param search_term: all comments need to include this search term
        :param subreddit: limit results to subreddit
        :param number_of_results: max number of comments to return
        :param start_date: all comments need to be posted on or after this date (format: YYYY-MM-DD)
        :param end_date: all comments need to be posted before or on this date (format: YYYY-MM-DD)
        :param min_score: minimum score (upvotes) for a comment to be included.
        """

        # the code in the init file mostly just validates the input, e.g. are the submitted dates
        # formed correctly
        for param in [search_term, subreddit]:
            if not (isinstance(param, str) or param is None):
                raise ValueError("Search term and sub reddit have to be strings.")
        self.search_term = search_term
        self.subreddit = subreddit

        start_date_date = datetime.strptime(start_date, '%Y-%m-%d').date()
        if start_date_date < date(2020, 1, 1):
            print("setting start date to 2020-01-01")
            start_date = date(2020, 1, 1)
        self.start_date_str = start_date
        self.start_date_date = datetime.strptime(start_date, '%Y-%m-%d').date()

        end_date_date = datetime.strptime(start_date, '%Y-%m-%d').date()
        if end_date_date > date.today():
            end_date = date.today() - timedelta(days=1)
            print(f"setting end date to {end_date}")
        self.end_date_str = end_date
        self.end_date_date = datetime.strptime(end_date, '%Y-%m-%d').date()

        for param in [number_of_results_per_day, min_score]:
            if not isinstance(param, int) or param < 0:
                raise ValueError("number_of_results and min_score have to be positive integers.")
        self.number_of_results = number_of_results_per_day
        self.min_score = min_score
        self.sort_by = sort_by

        if not filename:
            filename = self._generate_filename()
        self.filename = filename

    def _generate_filename(self):
        """
        generate filename for this search
        :return:
        """

        name_parts = []
        if self.search_term:
            name_parts.append(self.search_term)
        if self.subreddit:
            name_parts.append(f'r{self.subreddit}')
        if self.min_score > 0:
            name_parts.append(f'minscore_{self.min_score}')

        name_parts.append(f'{self.start_date_str}to{self.end_date_str}')

        name_parts = "_".join(name_parts)

        return f'{name_parts}.csv'

    def execute_query_and_store_as_csv(self, output_filename=None):
        """
        Execute search and stores result as a csv file

        :return:
        """

        documents = []

        current_date = self.start_date_date

        # iterate over all days from the start date (= current_date) as long as current_date
        # is less than the end date
        while current_date <= self.end_date_date:

            print(f"downloading comments from {current_date}")
            url = self._generate_query_url(start_date=current_date,
                                           end_date=current_date + timedelta(days=1))
            documents += self._get_documents(url)
            current_date += timedelta(days=1)
            time.sleep(1)

        self._store_documents_to_csv(documents, output_filename)

        print(f'Found {len(documents)} matching your search query.')

    def _generate_query_url(self, start_date=None, end_date=None):
        """
        Generates a url to query pushshift.io with the passed parameters
        :return: str

        >>> r = RedditScraper(search_term='visualization', subreddit='coronavirus')
        >>> r._generate_query_url()
        'https://api.pushshift.io/reddit/search/?q=visualization&subreddit=coronavirus&size=100&sort_type=score&sort=desc'

        >>> r2 = RedditScraper(start_date='2014-01-01', end_date='2014-12-31')
        >>> r2._generate_query_url()
        'https://api.pushshift.io/reddit/search/?size=100&after=1388552400&before=1420002000&sort_type=score&sort=desc'
        """

        search_params = {}
        if self.search_term:
            search_params['q'] = self.search_term
        if self.subreddit:
            search_params['subreddit'] = self.subreddit
        search_params['size'] = self.number_of_results

        if not start_date:
            start_date = self.start_date_date
        if not end_date:
            end_date = self.end_date_date

        start_timestamp = int(datetime(start_date.year, start_date.month,
                                       start_date.day).timestamp())
        end_timestamp = int(datetime(end_date.year, end_date.month, end_date.day).timestamp())

        search_params['after'] = start_timestamp
        search_params['before'] = end_timestamp

        if self.sort_by:
            search_params['sort_type'] = self.sort_by
            search_params['sort'] = 'desc'

        url = f'https://api.pushshift.io/reddit/search/?{urllib.parse.urlencode(search_params)}'

        if self.min_score and self.min_score > 0:
            url += f'&score=>{self.min_score}'

        return url

    def _get_documents(self, url):
        """
        Downloads up to number_of_documents matching the search query from pushshift.io

        :param url: str
        :return: list[dict]
        """

        documents = []
        with urllib.request.urlopen(url) as response:
            response = response.read().decode('utf-8')

            for doc_raw in json.loads(response)['data']:

                timestamp = doc_raw['created_utc']
                datetime_utc = datetime.utcfromtimestamp(timestamp)
                datetime_est = datetime_utc.replace(tzinfo=timezone.utc).astimezone(tz=None)
                date_str = datetime_est.strftime('%Y-%m-%d')

                if 'permalink' in doc_raw:
                    url = f'https://www.reddit.com{doc_raw["permalink"]}'
                else:
                    url = 'n/a'

                documents.append({
                    'date': date_str,
                    'author': doc_raw['author'],
                    'subreddit': doc_raw['subreddit'],
                    'score': doc_raw['score'],
                    'url': url,
                    'text': html.unescape(doc_raw['body']),
                })

        print(len(documents))

        return documents

    def _store_documents_to_csv(self, documents, filename):
        """
        Stores the downloaded documents in a csv in the data folder.
        If no filename is provided, it will automatically generate one.

        :param documents: list[dict]
        :param filename: str
        :return:
        """

        if not filename:
            filename = self.filename

        with open(Path('reddit_data', filename), 'w') as csvfile:
            fieldnames = ['date', 'author', 'subreddit', 'score', 'url', 'text']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for doc in documents:
                writer.writerow(doc)


if __name__ == '__main__':
    r = RedditScraper(subreddit='coronavirus',
                      number_of_results_per_day=2000, min_score=2,
                      start_date='2020-01-28',
                      end_date='2020-01-30',
                      filename='coronavirus.csv')
    r.execute_query_and_store_as_csv()


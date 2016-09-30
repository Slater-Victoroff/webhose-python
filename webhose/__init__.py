from datetime import datetime, timedelta

try:
    from urlparse import urljoin, parse_qs, urlparse
except ImportError:
    from urllib.parse import urljoin, parse_qs, urlparse

import requests

from settings import ALL_FIELDS, LIST_FIELDS, DIRECT_FIELDS

class TokenMissingException(IOError):
    """No token has been defined for making API requests"""


class Struct(object):
    def __init__(self, **params):
        self.__dict__.update(params)

class Query(object):
    def __init__(self, **kwargs):
        map(lambda name: setattr(self, name, kwargs.get(name, None)), ALL_FIELDS)

    def __setattr__(self, name, value):
        if name in LIST_FIELDS and isinstance(value, basestring):
            # They're called list fields for a reason, we'll assume they're lists to reduce redundancy
            value = [value]
        super(Query, self).__setattr__(name, value)

    def list_string(self, field):
        """A method for generating partial query strings of fields that may be lists
        """
        return "(" + " OR ".join("%s:%s" % (field, value) for value in getattr(self, field)) + ")"

    def direct_string(self, field):
        """A method for generating partial query strings of fields that only have a single value
        """
        return "%s:%s" % (field, getattr(self, field))

    def query_string(self):
        qs = []
        if self.all_terms:
            qs.append(self.all_terms)
        if self.exact_phrase:
            qs.append('"%s"' % self.exact_phrase)
        if self.some_terms:
            qs.append("(" + self.some_terms.replace(" ", " OR ") + ")")
        if self.exclude:
            qs.append("-%s" % self.exclude)
        if self.is_first:
            qs.append("is_first:true")
        if self.thread_title:
            qs.append("thread.title:(%s)" % self.thread_title)
        if self.thread_section_title:
            qs.append("thread.section_title:(%s)" % self.thread_section_title)
        if self.language:
            qs.append("language:(%s)" % self.language)
        if self.thread_country:
            qs.append("thread.country:%s" % self.thread_country)
        # For anything fitting repetitive query structures, it will be automatically handled here
        qs.extend(map(self.direct_string, filter(lambda x: getattr(self, x), DIRECT_FIELDS)))
        qs.extend(map(self.list_string, filter(lambda x: getattr(self, x), LIST_FIELDS)))
        return " ".join(term for term in qs)

    def __str__(self):
        return self.query_string()

class Response(object):
    """Webhose response. Usually contains a list of posts
    """

    def __init__(self, response, session):
        self.response = response
        self.session = session
        self.total = self.response.json()['totalResults']
        self.next = urljoin(self.response.url, self.response.json()['next'])
        self.next_ts = self.extract_next_ts()
        self.left = self.response.json()['requestsLeft']
        self.more = self.response.json()['moreResultsAvailable']
        self.posts = []
        for post in self.response.json()['posts']:
            self.posts.append(Post(post))

    def extract_next_ts(self):
        resource = self.response.json()['next']
        parsed = urlparse(resource)
        params = parse_qs(parsed.query)
        return params['ts'][0]

    def get_next(self):
        return self.session.get(self.next)

    def __iter__(self):
        response = self
        while True:
            for post in response.posts:
                yield post
            if response.more == 0:
                break
            response = response.get_next()
            self.total = response.total
            self.next = response.next
            self.left = response.left
            self.more = response.more
            self.posts = response.posts
            self.next_ts = response.next_ts


class Thread(Struct):
    """Information about the thread to which the post belongs
    """

    def __init__(self, **thread):
        super(Thread, self).__init__(**thread)
        self.site_section = thread.get("site_section")
        self.section_title = thread.get("section_title")
        self.title_full = thread.get("title_full", self.title)
        self.published_parsed = parse_iso8601(thread["published"])
        self.country = thread.get("country")


class Post(Struct):
    """Convenience class for post properties
    """

    def __init__(self, **post):
        super(Post, self).__init__(**post)
        self.published_parsed = parse_iso8601(post["published"])
        self.crawled_parsed = parse_iso8601(post["crawled"])
        self.external_links = post.get("external_links")
        self.persons = post["entities"]["persons"]
        self.locations = post["entities"]["locations"]
        self.organizations = post["entities"]["organizations"]
        self.thread = Thread(post["thread"])


class Session(object):
    """Requests Session, plus additional config
    """

    def __init__(self, token=None):
        self.session = requests.Session()
        self.token = token

    def get(self, url):
        response = self.session.get(url)
        return Response(response, self)

    def search(self, query, token=None, since=None):
        if token is None and self.token is None:
            raise TokenMissingException("No token defined for webhose API request")

        if type(query) is Query:
            query = query.query_string()

        params = {
            "q": query,
            "token": token or self.token
        }

        if since:
            params['ts'] = since

        response = self.session.get("http://webhose.io/search", params=params)
        if response.status_code != 200:
            raise Exception(response.text)
        return Response(response, self)


def parse_iso8601(str_date):
    dt = datetime.strptime(str_date[:-10], "%Y-%m-%dT%H:%M:%S")
    offset = timedelta(hours=int(str_date[24:26]), minutes=int(str_date[27:29])) * (1 if str_date[23] == '+' else -1)
    return dt - offset

__session = Session()


def config(token):
    __session.token = token


def search(query, token=None, since=None):
    return __session.search(query, token, since=since)


def get(url):
    return __session.get(url)

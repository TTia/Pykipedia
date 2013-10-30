'''
OpenAnything: a kind and thoughtful library for HTTP web services
'''

__author__ = 'Mark Pilgrim (mark@diveintopython.org) - Simone Aonzo'
__version__ = '$Revision: Simone Aonzo adapted for python 3.3$'
__date__ = '$Date: 2013/10/21 12:00 $'
__copyright__ = 'Copyright (c) 2004 Mark Pilgrim'
__license__ = 'Python'


from gzip import *
from io import BytesIO
from urllib.parse import *
from urllib.request import *
from io import StringIO
from urllib.request import *
from urllib.error import *
from urllib.parse import *
from urllib.robotparser import *
from gzip import *



USER_AGENT = 'pykiPedia/0.1 (http://example.com/MyCoolTool/; MyCoolTool@example.com) BasedOnSuperLib/1.4'

class SmartRedirectHandler(HTTPRedirectHandler):
    # 301 Moved Permanently
    def http_error_301(self, req, fp, code, msg, headers):
        result = HTTPRedirectHandler.http_error_301(
            self, req, fp, code, msg, headers)
        result.status = code
        return result

    # 302 Found
    def http_error_302(self, req, fp, code, msg, headers):
        result = HTTPRedirectHandler.http_error_302(
            self, req, fp, code, msg, headers)
        result.status = code
        return result

class DefaultErrorHandler(HTTPDefaultErrorHandler):
    def http_error_default(self, req, fp, code, msg, headers):
        result = HTTPError(req.get_full_url(), code, msg, headers, fp)
        result.status = code
        return result

def openAnything(source, etag=None, lastmodified=None, agent=USER_AGENT):
    """URL, filename, or string --> stream

    This function lets you define parsers that take any input source
    (URL, pathname to local or network file, or actual data as a string)
    and deal with it in a uniform manner.  Returned object is guaranteed
    to have all the basic stdio read methods (read, readline, readlines).
    Just .close() the object when you're done with it.

    If the etag argument is supplied, it will be used as the value of an
    If-None-Match request header.

    If the lastmodified argument is supplied, it must be a formatted
    date/time string in GMT (as returned in the Last-Modified header of
    a previous request).  The formatted date/time will be used
    as the value of an If-Modified-Since request header.

    If the agent argument is supplied, it will be used as the value of a
    User-Agent request header.
    """

    if hasattr(source, 'read'):
        return source

    if source == '-':
        return sys.stdin

    if urlparse(source)[0] == 'http':
        # open URL with urllib2
        request = Request(source)
        request.add_header('User-Agent', agent)
        if lastmodified:
            request.add_header('If-Modified-Since', lastmodified)
        if etag:
            request.add_header('If-None-Match', etag)
        request.add_header('Accept-encoding', 'gzip')
        opener = build_opener(SmartRedirectHandler(), DefaultErrorHandler())
        return opener.open(request)
    
    # try to open with native open function (if source is a filename)
    try:
        return open(source)
    except (IOError, OSError):
        pass

    # treat source as string
    return StringIO(str(source))

def fetch(source, etag=None, lastmodified=None, agent=USER_AGENT):
    '''Fetch data and metadata from a URL, file, stream, or string'''
    result = {}
    f = openAnything(source, etag, lastmodified, agent)
    result['data'] = f.read()
    if hasattr(f, 'headers'):
        # save ETag, if the server sent one
        result['etag'] = f.headers.get('ETag')
        # save Last-Modified header, if the server sent one
        result['lastmodified'] = f.headers.get('Last-Modified')
        if f.headers.get('content-encoding') == 'gzip':
            # data came back gzip-compressed, decompress it
            result['data'] = GzipFile(fileobj=BytesIO(result['data'])).read() # more than 1 hour used for search
    if hasattr(f, 'url'):
        result['url'] = f.url
        result['status'] = 200
    if hasattr(f, 'status'):
        result['status'] = f.status
    f.close()
    return result
    

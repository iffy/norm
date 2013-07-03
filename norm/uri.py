"""
URI parsing
"""


from urlparse import urlparse, parse_qs



def parseURI(uri):
    """
    Parse a database uri into component parts.
    """
    ret = {}
    r = urlparse(uri)
    ret['scheme'] = r.scheme
    if r.scheme == 'sqlite':
        # sqlite
        ret['file'] = uri.split(':')[1]
    else:
        # postgres
        parts = r.path.lstrip('/').split('?')
        ret['db'] = parts[0]
        if r.username:
            ret['user'] = r.username
        if r.hostname:
            ret['host'] = r.hostname
        if r.port:
            ret['port'] = r.port
        if r.password:
            ret['password'] = r.password
        if len(parts) == 2:
            query = parts[1]
            pq = parse_qs(query)
            for k,v in parse_qs(query).items():
                ret[k] = v[-1]
    return ret



def mkConnStr(parsed_uri):
    """
    Turn a parsed uri into a connection string suitable for the db module
    """
    if parsed_uri['scheme'] == 'sqlite':
        return parsed_uri['file'] or ':memory:'

    # postgres
    parts = ['dbname=%s' % parsed_uri['db']]
    for attr in ['host', 'port', 'user', 'password', 'sslmode']:
        if attr in parsed_uri:
            parts.append('%s=%s' % (attr, parsed_uri[attr]))
    return ' '.join(parts)
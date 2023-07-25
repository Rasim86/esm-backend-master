from ldap3 import Server, Connection, ALL, SUBTREE
from server.cfg import ldap_server

username = 'gpsm'
password = '%t?cgvG#'
server = Server(ldap_server, get_info=ALL)


def ldap_user_check(user_login, user_password):
    conn = Connection(server, user=username, password=password)
    conn.bind()
    search_base = 'DC=tattelecom,DC=ttc'
    search_filter = '(&(objectClass=Person)(sAMAccountName=%s))' % user_login

    conn.search(search_base, search_filter, SUBTREE,
                       attributes=['profilePath', 'manager', 'title', 'description', 'department', 'memberOf',
                                   'distinguishedName', 'userPrincipalName', 'CN', 'mail'])
    try:
        result = conn.entries[0]
        conn.unbind()
        user_auth = Connection(server, user=str(result['userPrincipalName']), password=str(user_password))
        if user_auth.bind():
            user_auth.unbind()
            login_info = [str(result['cn']), str(result['mail'])]
            return login_info
    except Exception:
        return False
    return False


def ldap_user_info(user_login):
    conn = Connection(server, user=username, password=password)
    conn.bind()
    search_base = 'DC=tattelecom,DC=ttc'
    search_filter = '(&(objectClass=Person)(sAMAccountName=%s))' % user_login

    conn.search(search_base, search_filter, SUBTREE,
                       attributes=['profilePath', 'manager', 'title', 'description', 'department', 'memberOf',
                                   'distinguishedName', 'userPrincipalName', 'CN', 'mail'])
    try:
        result = conn.entries[0]
        return result
    except Exception:
        return False

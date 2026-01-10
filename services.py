import backend as db
from cassandra.query import SimpleStatement, BatchStatement
from cassandra import ConsistencyLevel
import threading
import uuid
from collections import namedtuple

CELEBRITY_TRESHOLD = 100000
CONCURRENT_REQUESTS_LIMIT = 500

PUSH_SEMAPHORE = threading.Semaphore(CONCURRENT_REQUESTS_LIMIT)

session = None
prepared_statements = {}

QUERIES = {
    'INSERT_MOJE_POSTY': "INSERT INTO moje_posty (username, post_id, content) VALUES (?,?,?)",
    'GET_FOLLOWERS': "SELECT follower_username FROM kto_mnie_obserwuje WHERE username =?",
    'INSERT_MOJA_OS_CZASU': "INSERT INTO moja_os_czasu (username, post_id, author_username, content) VALUES (?,?,?,?)",
    'INSERT_KOGO_OBSERWUJE': "INSERT INTO kogo_obserwuje (username, following_username) VALUES (?,?)",
    'INSERT_KTO_MNIE_OBSERWUJE': "INSERT INTO kto_mnie_obserwuje (username, follower_username) VALUES (?,?)",
    'DELETE_KOGO_OBSERWUJE': "DELETE FROM kogo_obserwuje WHERE username = ? AND following_username = ?",
    'DELETE_KTO_MNIE_OBSERWUJE': "DELETE FROM kto_mnie_obserwuje WHERE username = ? AND follower_username = ?",
    'INC_FOLLOWERS': "UPDATE user_stats SET followers_count = followers_count + 1 WHERE username =?",
    'DEC_FOLLOWERS': "UPDATE user_stats SET followers_count = followers_count - 1 WHERE username =?",
    'GET_MOJA_OS_CZASU': "SELECT author_username, content, post_id FROM moja_os_czasu WHERE username =?",
    'GET_KOGO_OBSERWUJE': "SELECT following_username FROM kogo_obserwuje WHERE username =?",
    'GET_STATS': "SELECT followers_count FROM user_stats WHERE username =?",
    'GET_MOJE_POSTY_LIMIT': "SELECT username, content, post_id FROM moje_posty WHERE username =? LIMIT 10",
    'GET_PROFIL': "SELECT content, post_id FROM moje_posty WHERE username =? LIMIT 20",
    'INC_FOLLOWERS_PULL_TEST': "UPDATE user_stats SET followers_count = followers_count + ? WHERE username =?"
}

def initialize_prepared_statements():
    global session, prepared_statements
    session = db.get_session()
    for name, query in QUERIES.items():
        prepared_statements[name] = session.prepare(query)
    print(f"[INFO] Zainicjalizowano {len(prepared_statements)} przygotowanych zapytań.")

def get_prepared(name):
    return prepared_statements[name]

# --- LOGIKA BIZNESOWA ---

def follow_user(user_who_follows, user_to_be_followed):
    session.execute(get_prepared('INSERT_KOGO_OBSERWUJE'), [user_who_follows, user_to_be_followed])
    session.execute(get_prepared('INSERT_KTO_MNIE_OBSERWUJE'), [user_to_be_followed, user_who_follows])
    session.execute(get_prepared('INC_FOLLOWERS'), [user_to_be_followed])

def unfollow_user(user_who_follows, user_to_be_followed):
    session.execute(get_prepared('DELETE_KOGO_OBSERWUJE'), [user_who_follows, user_to_be_followed])
    session.execute(get_prepared('DELETE_KTO_MNIE_OBSERWUJE'), [user_to_be_followed, user_who_follows])
    session.execute(get_prepared('DEC_FOLLOWERS'), [user_to_be_followed])
    print(f"INFO: {user_who_follows} przestał obserwować {user_to_be_followed}")

def post(author, content):
    post_id = uuid.uuid1()
    
    # 1. Zapisz u siebie (zawsze)
    session.execute(get_prepared('INSERT_MOJE_POSTY'), [author, post_id, content])
    
    # 2. Sprawdź czy jestem celebrytą
    stats_row = session.execute(get_prepared('GET_STATS'), [author]).one()
    followers_count = stats_row.followers_count if stats_row else 0

    if followers_count < CELEBRITY_TRESHOLD:
        # PUSH: Rozsyłamy do wszystkich fanów
        rows = session.execute(get_prepared('GET_FOLLOWERS'), [author])
        followers = [row.follower_username for row in rows]

        def release_semaphore(result):
            PUSH_SEMAPHORE.release()

        for follower_username in followers:
            PUSH_SEMAPHORE.acquire()
            output = session.execute_async(get_prepared('INSERT_MOJA_OS_CZASU'), [follower_username, post_id, author, content])
            output.add_callback(release_semaphore)
            output.add_errback(release_semaphore)
        
        # Czekamy na zakończenie wszystkich operacji
        for _ in range(CONCURRENT_REQUESTS_LIMIT):
            PUSH_SEMAPHORE.acquire()
        for _ in range(CONCURRENT_REQUESTS_LIMIT):
            PUSH_SEMAPHORE.release()

    else:
        # PULL: Nic nie robimy. Fani sami muszą pobrać moje posty.
        print(f"[INFO] Użytkownik {author} jest celebrytą ({followers_count} fanów). PUSH pominięty.")

def get_timeline(user):
    all_posts = []

    rows_push = session.execute(get_prepared('GET_MOJA_OS_CZASU'), [user])
    for row in rows_push:
        all_posts.append({
            'author': row.author_username,
            'content': row.content,
            'id': row.post_id,
            'is_celeb': False
        })

    following = session.execute(get_prepared('GET_KOGO_OBSERWUJE'), [user])
    
    for row in following:
        friend_name = row.following_username
        
        stats = session.execute(get_prepared('GET_STATS'), [friend_name]).one()
        f_count = stats.followers_count if stats else 0
        
        if f_count >= CELEBRITY_TRESHOLD:
            celeb_posts = session.execute(get_prepared('GET_MOJE_POSTY_LIMIT'), [friend_name])
            for cp in celeb_posts:
                all_posts.append({
                    'author': cp.username,
                    'content': cp.content,
                    'id': cp.post_id,
                    'is_celeb': True
                })

    all_posts.sort(key=lambda x: x['id'], reverse=True)

    timeline_strings = []
    for p in all_posts:
        prefix = " [CELEBRYTA]" if p['is_celeb'] else ""
        timeline_strings.append(f"@{p['author']}{prefix}: {p['content']}")
    
    return timeline_strings

def get_profile(user):
    return session.execute(get_prepared('GET_PROFIL'), [user])

def get_following_list(user):
    rows = session.execute(get_prepared('GET_KOGO_OBSERWUJE'), [user])
    return [row.following_username for row in rows]

def get_followers_list(user):
    rows = session.execute(get_prepared('GET_FOLLOWERS'), [user])
    return [row.follower_username for row in rows]
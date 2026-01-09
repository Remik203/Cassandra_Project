import backend as db
from cassandra.query import SimpleStatement, BatchStatement
from cassandra import ConsistencyLevel
import threading
import uuid
from collections import namedtuple

CELEBRITY_TRESHOLD = 100000

CONCURRENT_REQUESTS_LIMIT = 500 # Limit równoległych poleceń PUSH w locie

PUSH_SEMAPHORE = threading.Semaphore(CONCURRENT_REQUESTS_LIMIT) # Semafor do PUSH postów

# Przygotowanie zapytań
session = db.get_session()
    
PREPARED_INSERT_MOJE_POSTY = session.prepare("INSERT INTO moje_posty (username, post_id, content) VALUES (?,?,?)")
PREPARED_GET_FOLLOWERS = session.prepare("SELECT follower_username FROM kto_mnie_obserwuje WHERE username =?")
PREPARED_INSERT_MOJA_OS_CZASU = session.prepare("INSERT INTO moja_os_czasu (username, post_id, author_username, content) VALUES (?,?,?,?)")

PREPARED_INSERT_KOGO_OBSERWUJE = session.prepare("INSERT INTO kogo_obserwuje (username, following_username) VALUES (?,?)")
PREPARED_INSERT_KTO_MNIE_OBSERWUJE = session.prepare("INSERT INTO kto_mnie_obserwuje (username, follower_username) VALUES (?,?)")

PREPARED_DELETE_KOGO_OBSERWUJE = session.prepare("DELETE FROM kogo_obserwuje WHERE username = ? AND following_username = ?")
PREPARED_DELETE_KTO_MNIE_OBSERWUJE = session.prepare("DELETE FROM kto_mnie_obserwuje WHERE username = ? AND follower_username = ?")

PREPARED_INC_FOLLOWERS = session.prepare("UPDATE user_stats SET followers_count = followers_count + 1 WHERE username =?")
PREPARED_DEC_FOLLOWERS = session.prepare("UPDATE user_stats SET followers_count = followers_count - 1 WHERE username =?")


PREPARED_GET_MOJA_OS_CZASU = session.prepare("SELECT author_username, content, post_id FROM moja_os_czasu WHERE username =?")
PREPARED_GET_KOGO_OBSERWUJE = session.prepare("SELECT following_username FROM kogo_obserwuje WHERE username =?")
PREPARED_GET_STATS = session.prepare("SELECT followers_count FROM user_stats WHERE username =?")
PREPARED_GET_MOJE_POSTY_LIMIT = session.prepare("SELECT username, content, post_id FROM moje_posty WHERE username =? LIMIT 10")


PREPARED_GET_PROFIL = session.prepare("SELECT content, post_id FROM moje_posty WHERE username =? LIMIT 20")

# ZAPYTANIE SPECJALNIE DO TESTU PULL
PREPARED_INC_FOLLOWERS_pull_test = session.prepare("UPDATE user_stats SET followers_count = followers_count + ? WHERE username =?")

# Definicje funkcji
def follow_user(user_who_follows, user_to_be_followed):
    # 1. Dodaj do tabeli 'kogo_obserwuje'
    session.execute(PREPARED_INSERT_KOGO_OBSERWUJE, [user_who_follows, user_to_be_followed])

    # 2. Dodaj do tablicy 'kto_mnie_obserwuje' drugiego usera
    session.execute(PREPARED_INSERT_KTO_MNIE_OBSERWUJE, [user_to_be_followed, user_who_follows])

    # 3. Zwiększ licznik obserwujących drugiego usera
    session.execute(PREPARED_INC_FOLLOWERS, [user_to_be_followed])


def unfollow_user(user_who_follows, user_to_be_followed):
    # 1. Usuń z tabeli 'kogo_obserwuje'
    session.execute(PREPARED_DELETE_KOGO_OBSERWUJE, [user_who_follows, user_to_be_followed])

    # 2. Usuń z tablicy 'kto_mnie_obserwuje' drugiego usera
    session.execute(PREPARED_DELETE_KTO_MNIE_OBSERWUJE, [user_to_be_followed, user_who_follows])

    # 3. Zmniejsz licznik obserwujących drugiego usera
    session.execute(PREPARED_DEC_FOLLOWERS, [user_to_be_followed])

    print(f"INFO: {user_who_follows} przestał obserwować {user_to_be_followed}")

def post(author, content):
    post_id = uuid.uuid1()
    
    # Zapisz w moich postach
    session.execute(PREPARED_INSERT_MOJE_POSTY, [author, post_id, content])
    
    # Sprawdzenie czy użytkownik jest celebrytą
    stats_row = session.execute(PREPARED_GET_STATS, [author]).one()
    followers_count = stats_row.followers_count if stats_row else 0


    if followers_count < CELEBRITY_TRESHOLD:
        rows = session.execute(PREPARED_GET_FOLLOWERS, [author])
        followers = [row.follower_username for row in rows]

        def release_semaphore(result):
            PUSH_SEMAPHORE.release()

        for follower_username in followers:
            PUSH_SEMAPHORE.acquire()

            output = session.execute_async(PREPARED_INSERT_MOJA_OS_CZASU, [follower_username, post_id, author, content])

            output.add_callback(release_semaphore)
            output.add_errback(release_semaphore)
        
        for _ in range(CONCURRENT_REQUESTS_LIMIT):
            PUSH_SEMAPHORE.acquire()
        
        for _ in range(CONCURRENT_REQUESTS_LIMIT):
            PUSH_SEMAPHORE.release()

    else:
        print(f"[INFO] Użytkownik {author} jest celebrytą. Obserwujący muszą zrobić PULL.")
            
def get_timeline(user):    
    all_posts = []
    seen_posts_ids = set()

    # 1. PUSH: Pobranie postów już wypchniętych do mojej osi czasu (od zwykłych userów)
    pushed_posts = list(session.execute(PREPARED_GET_MOJA_OS_CZASU, [user]))

    for post in pushed_posts:
        all_posts.append(post)
        seen_posts_ids.add(post.post_id)

    # 2. PULL: Pobranie postów od obserwowanych CELEBRYTÓW
    following_rows = session.execute(PREPARED_GET_KOGO_OBSERWUJE, [user])

    pulled_posts = []

    for row in following_rows:
        if row.is_celebrity:
            followed_celeb = row.following_username
            celeb_posts = list(session.execute(PREPARED_GET_MOJE_POSTY_LIMIT, [followed_celeb]))

            for post in celeb_posts:
                if post.post_id not in seen_posts_ids:                  
                    pulled_posts.append(post)
                    seen_posts_ids.add(post.post_id)

    all_posts.extend(pulled_posts)
    all_posts.sort(key=lambda x: x.post_id, reverse=True)

    return all_posts[:20]

def get_profile(user):
    return session.execute(PREPARED_GET_PROFIL, [user])
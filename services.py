import backend as db
from cassandra.query import SimpleStatement
import uuid

# Lazy cache for prepared statements
_PREPARED = {}

def _ensure_prepared():
    if _PREPARED:
        return
    sess = db.get_session()
    _PREPARED['FOLLOW'] = sess.prepare("INSERT INTO followers_by_user (username, follower_username) VALUES (?,?)")
    _PREPARED['GET_TIMELINE'] = sess.prepare("SELECT author_username, content, post_id FROM timeline_by_user WHERE username =? LIMIT 20")
    _PREPARED['GET_PROFILE'] = sess.prepare("SELECT content, post_id FROM posts_by_user WHERE username =? LIMIT 20")
    _PREPARED['INSERT_POST'] = sess.prepare("INSERT INTO posts_by_user (username, post_id, content) VALUES (?,?,?)")
    _PREPARED['GET_FOLLOWERS'] = sess.prepare("SELECT follower_username FROM followers_by_user WHERE username =?")
    _PREPARED['INSERT_TIMELINE'] = sess.prepare("INSERT INTO timeline_by_user (username, post_id, author_username, content) VALUES (?,?,?,?)")

def follow_user(user, user_to_follow):
    sess = db.get_session()
    _ensure_prepared()
    sess.execute(_PREPARED['FOLLOW'], [user_to_follow, user])

def get_timeline(user):
    sess = db.get_session()
    _ensure_prepared()
    rows = sess.execute(_PREPARED['GET_TIMELINE'], [user])
    return rows 

def get_profile(user):
    sess = db.get_session()
    _ensure_prepared()
    rows = sess.execute(_PREPARED['GET_PROFILE'], [user])
    return rows

def post(author, content):
    sess = db.get_session()
    _ensure_prepared()

    # Krok 1: Wygeneruj TimeUUID
    post_uuid = uuid.uuid1()

    # Krok 2: Zapis na własnym profilu
    sess.execute(_PREPARED['INSERT_POST'], [author, post_uuid, content])

    # Krok 3: Pobranie listy śledzących
    rows = sess.execute(_PREPARED['GET_FOLLOWERS'], [author])
    followers = [row.follower_username for row in rows]

    if not followers:
        print(f"INFO: Użytkownik {author} opublikował post, ale nie ma obserwujących.")
        return

    # Krok 4: Pętla "Fan-out on Write" (Implementacja synchroniczna)
    print(f"INFO: Rozpoczynam fan-out dla {author} do {len(followers)} obserwujących...")
    for follower_username in followers:
        sess.execute(
            _PREPARED['INSERT_TIMELINE'],
            [follower_username, post_uuid, author, content]
        )
    print(f"INFO: Fan-out zakończony.")
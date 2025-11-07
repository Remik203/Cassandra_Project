from cassandra.query import SimpleStatement, BatchStatement
from cassandra import ConsistencyLevel
from collections import namedtuple
import uuid
import backend as db

# Lazy cache for prepared statements
_PREPARED = {}

def _ensure_prepared():
    if _PREPARED:
        return
    sess = db.get_session()
    # Polskie nazwy tabel
    _PREPARED['FOLLOW'] = sess.prepare("INSERT INTO kto_mnie_obserwuje (username, follower_username) VALUES (?,?)")
    _PREPARED['GET_TIMELINE'] = sess.prepare("SELECT author_username, content, post_id FROM moja_os_czasu WHERE username =? LIMIT 50")
    _PREPARED['GET_PROFILE'] = sess.prepare("SELECT content, post_id FROM moje_posty WHERE username =? LIMIT 20")
    _PREPARED['INSERT_POST'] = sess.prepare("INSERT INTO moje_posty (username, post_id, content) VALUES (?,?,?)")
    _PREPARED['GET_FOLLOWERS'] = sess.prepare("SELECT follower_username FROM kto_mnie_obserwuje WHERE username =?")
    _PREPARED['INSERT_TIMELINE'] = sess.prepare("INSERT INTO moja_os_czasu (username, post_id, author_username, content) VALUES (?,?,?,?)")
    _PREPARED['GET_FOLLOWERS_COUNT'] = sess.prepare("SELECT COUNT(*) FROM kto_mnie_obserwuje WHERE username =?")
    _PREPARED['ADD_FOLLOWING'] = sess.prepare("INSERT INTO kogo_obserwuje (username, following_username, is_celebrity) VALUES (?,?,?)")
    _PREPARED['GET_FOLLOWING'] = sess.prepare("SELECT following_username, is_celebrity FROM kogo_obserwuje WHERE username =?")
    _PREPARED['GET_CELEBRITY_POSTS'] = sess.prepare("SELECT content, post_id FROM moje_posty WHERE username =? LIMIT 10")

def get_followers_count(user):
    sess = db.get_session()
    _ensure_prepared()
    result = sess.execute(_PREPARED['GET_FOLLOWERS_COUNT'], [user])
    row = result.one()
    return row[0]  # Zwracaj bezpośrednio liczbę

def follow_user(user, user_to_follow, is_celebrity=None):
    sess = db.get_session()
    _ensure_prepared()
    
    # Automatycznie wykryj czy to celebryta
    if is_celebrity is None:
        follower_count = get_followers_count(user_to_follow)
        is_celebrity = follower_count > 5000
    
    # Tradycyjne obserwowanie (dla fan-outu)
    sess.execute(_PREPARED['FOLLOW'], [user_to_follow, user])
    
    # Nowe: zapisz kogo obserwujesz + czy to celebryta (dla fan-in)
    sess.execute(_PREPARED['ADD_FOLLOWING'], [user, user_to_follow, is_celebrity])
    
    print(f"INFO: {user} obserwuje {user_to_follow} {'(CELEBRYTA)' if is_celebrity else '(zwykły)'}")

def post(author, content):
    sess = db.get_session()
    _ensure_prepared()
    post_uuid = uuid.uuid1() # Używamy TimeUUID
    
    # Zawsze zapisz na własnym profilu (dla fan-in / pull)
    sess.execute(_PREPARED['INSERT_POST'], [author, post_uuid, content])
    
    # Sprawdź czy author ma dużo obserwujących (celebryta)
    followers_count = get_followers_count(author)
    
    if followers_count > 5000:  # Próg celebryty
        print(f"INFO: {author} jest celebrytą ({followers_count} obserwujących) - pomijam fan-out")
        return
    
    # Fan-out tylko dla zwykłych użytkowników
    rows = sess.execute(_PREPARED['GET_FOLLOWERS'], [author])
    followers = [row.follower_username for row in rows]
    
    if not followers:
        print(f"INFO: Użytkownik {author} opublikował post, ale nie ma obserwujących.")
        return
        
    print(f"INFO: Rozpoczynam fan-out dla {author} do {len(followers)} obserwujących...")
    for follower_username in followers:
        sess.execute(_PREPARED['INSERT_TIMELINE'], 
                    [follower_username, post_uuid, author, content])
    print(f"INFO: Fan-out zakończony.")

# Definiujemy prosty obiekt, aby ujednolicić format wyników
PostRow = namedtuple('PostRow', ['author_username', 'content', 'post_id'])

def get_timeline(user):
    sess = db.get_session()
    _ensure_prepared()
    
    # 1. Pobierz gotowe posty z moja_os_czasu (zwykli użytkownicy)
    regular_posts = list(sess.execute(_PREPARED['GET_TIMELINE'], [user]))
    
    # 2. Pobierz listę obserwowanych celebrytów
    following_rows = sess.execute(_PREPARED['GET_FOLLOWING'], [user])
    all_posts = list(regular_posts)
    
    for row in following_rows:
        if row.is_celebrity:
            # Pull postów od celebryty
            celeb_posts = sess.execute(_PREPARED['GET_CELEBRITY_POSTS'], [row.following_username])
            for post in celeb_posts:
                # Dodaj jako obiekt podobny do regular_posts
                all_posts.append(PostRow(row.following_username, post.content, post.post_id))
    
    # 3. Zwróć pierwsze 20 postów
    return all_posts[:20]

def get_profile(user):
    sess = db.get_session()
    _ensure_prepared()
    
    rows = sess.execute(_PREPARED['GET_PROFILE'], [user])
    # Konwertujemy na PostRow (author_username będzie tym samym co user)
    return [PostRow(user, row.content, row.post_id) for row in rows]

# --- NOWA FUNKCJA DLA STRESS TESTU ---

def stress_add_followers_batch(celebrity_username, followers_list):
    """
    Dodawanie obserwujących wsadowo (batch) dla lepszej wydajności w stress testach
    """
    sess = db.get_session()
    
    # Batch dla kto_mnie_obserwuje 
    batch_followers = BatchStatement(consistency_level=ConsistencyLevel.LOCAL_QUORUM)
    batch_following = BatchStatement(consistency_level=ConsistencyLevel.LOCAL_QUORUM)
    
    for follower in followers_list:
        # kto_mnie_obserwuje: celebrity <- follower
        batch_followers.add(SimpleStatement("INSERT INTO kto_mnie_obserwuje (username, follower_username) VALUES (?,?)"), 
                          [celebrity_username, follower])
        # kogo_obserwuje: follower -> celebrity (is_celebrity=True)
        batch_following.add(SimpleStatement("INSERT INTO kogo_obserwuje (username, following_username, is_celebrity) VALUES (?,?,?)"), 
                          [follower, celebrity_username, True])
    
    # Wykonaj batch
    sess.execute(batch_followers)
    sess.execute(batch_following)
    
    print(f"INFO: Dodano {len(followers_list)} obserwujących dla {celebrity_username} (BATCH)")
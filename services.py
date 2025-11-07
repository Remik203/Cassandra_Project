import backend as db
from cassandra.query import SimpleStatement, BatchStatement
from cassandra import ConsistencyLevel
import uuid
from collections import namedtuple

# Lazy cache for prepared statements
_PREPARED = {}

def _ensure_prepared():
    if _PREPARED:
        return
    sess = db.get_session()
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
    
    #print(f"INFO: {user} obserwuje {user_to_follow} {'(CELEBRYTA)' if is_celebrity else '(zwykły)'}")

def post(author, content):
    sess = db.get_session()
    _ensure_prepared()
    post_uuid = uuid.uuid1() # Używamy TimeUUID
    
    # Zawsze zapisz na własnym profilu (dla fan-in / pull)
    sess.execute(_PREPARED['INSERT_POST'], [author, post_uuid, content])
    
    # Sprawdź czy author ma dużo obserwujących (celebryta)
    followers_count = get_followers_count(author)
    
    if followers_count > 5000:  # Próg celebryty
        print(f"INFO: {author} jest celebrytą ({followers_count} obserwujących) - pomijam fan-out (push)")
        return # CELEBRYTA: Zakończ. Posty będą dociągane (pull) przez get_timeline.
    
    # Fan-out (push) tylko dla zwykłych użytkowników
    rows = sess.execute(_PREPARED['GET_FOLLOWERS'], [author])
    followers = [row.follower_username for row in rows]
    
    if not followers:
        print(f"INFO: Użytkownik {author} opublikował post, ale nie ma obserwujących.")
        return
        
    print(f"INFO: Rozpoczynam fan-out (push) dla {author} do {len(followers)} obserwujących...")
    for follower_username in followers:
        sess.execute(_PREPARED['INSERT_TIMELINE'], 
                    [follower_username, post_uuid, author, content])
    print(f"INFO: Fan-out zakończony.")

# Definiujemy prosty obiekt, aby ujednolicić format wyników
PostRow = namedtuple('PostRow', ['author_username', 'content', 'post_id'])

def get_timeline(user):
    """
    Pobiera oś czasu w modelu hybrydowym (PUSH + PULL).
    """
    sess = db.get_session()
    _ensure_prepared()
    
    all_posts = []

    # 1. PULL (Fan-out-on-read): Dociągnij posty od CELEBRYTÓW, których obserwujesz
    
    # Najpierw znajdźmy celebrytów
    following_rows = sess.execute(_PREPARED['GET_FOLLOWING'], [user])
    celebrities = [row.following_username for row in following_rows if row.is_celebrity]
    
    print(f"DEBUG: {user} obserwuje celebrytów: {celebrities}")

    # Dla każdego celebryty, dociągnij jego posty z jego profilu
    for celeb_name in celebrities:
        celeb_post_rows = sess.execute(_PREPARED['GET_CELEBRITY_POSTS'], [celeb_name])
        for row in celeb_post_rows:
            # Tworzymy ujednolicony obiekt PostRow
            all_posts.append(PostRow(author_username=celeb_name, 
                                     content=row.content, 
                                     post_id=row.post_id))

    # 2. PUSH (Fan-out-on-write): Pobierz posty od ZWYKŁYCH użytkowników
    # Te posty zostały już wepchnięte do naszej osi czasu przy publikacji
    pushed_rows = sess.execute(_PREPARED['GET_TIMELINE'], [user])
    for row in pushed_rows:
        # row już ma format (author_username, content, post_id), więc pasuje
        all_posts.append(row)

    # 3. Połącz, posortuj i zwróć
    
    # Sortujemy po post_id (TimeUUID) malejąco (od najnowszych)
    all_posts.sort(key=lambda p: p.post_id, reverse=True)
    
    # Zwróć tylko 20 najnowszych (lub inną rozsądną liczbę)
    return all_posts[:20]

def get_profile(user):
    sess = db.get_session()
    _ensure_prepared()
    # Zwracamy to samo co GET_TIMELINE dla spójności
    rows = sess.execute(_PREPARED['GET_PROFILE'], [user])
    
    # Zmieniamy format na ten sam co get_timeline
    profile_posts = []
    for row in rows:
        profile_posts.append(PostRow(author_username=user, # Na profilu autor jest zawsze ten sam
                                     content=row.content,
                                     post_id=row.post_id))
    return profile_posts

# --- NOWA FUNKCJA DLA STRESS TESTU ---

def stress_add_followers_batch(celebrity_username, followers_list):
    """
    Szybko dodaje wielu obserwujących za pomocą wstawiania wsadowego (batch).
    """
    sess = db.get_session()
    _ensure_prepared()
    
    # Ustawiamy rozmiar paczki (batch_size)
    # Będziemy wysyłać 100 obserwujących na raz
    # (co daje 200 zapytań INSERT w jednej paczce)
    batch_size = 100
    batch = None
    
    print(f"INFO: Rozpoczynam wstawianie wsadowe {len(followers_list)} obserwujących...")
    
    for i, follower_username in enumerate(followers_list):
        if i % batch_size == 0:
            # Wykonaj poprzednią paczkę, jeśli istnieje
            if batch:
                sess.execute(batch)
                #print(f"INFO: ...wysłano paczkę {i // batch_size}...")
            
            # Rozpocznij nową paczkę
            # Używamy UNLOGGED, ponieważ operujemy na różnych kluczach partycji
            # To tylko grupuje zapytania po stronie klienta
            batch = BatchStatement(consistency_level=ConsistencyLevel.LOCAL_QUORUM)
        
        # W teście zakładamy, że obserwowany jest celebrytą
        is_celebrity = True
        
        # 1. Dodaj do listy obserwujących celebryty
        batch.add(_PREPARED['FOLLOW'], [celebrity_username, follower_username])
        
        # 2. Dodaj celebrytę do listy obserwowanych przez followera
        batch.add(_PREPARED['ADD_FOLLOWING'], [follower_username, celebrity_username, is_celebrity])

    # Wykonaj ostatnią, niepełną paczkę
    if batch:
        sess.execute(batch)
        print("INFO: ...wysłano ostatnią paczkę.")
    
    print("INFO: Wstawianie wsadowe zakończone.")
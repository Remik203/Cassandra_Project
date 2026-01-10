import backend as db
import services as timeline_service
import time
import os
import uuid
from cassandra.concurrent import execute_concurrent_with_args

# Połącz z bazą danych
db.connect()
timeline_service.initialize_prepared_statements()

# --- FUNKCJE POMOCNICZE ---

def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')

def print_separator():
    print("-" * 50)

def print_header(title):
    print("\n" + "=" * 50)
    print(f" {title.upper()}")
    print("=" * 50)

def format_rows(rows):
    """Wyświetla wyniki z bazy w ładnej formie."""
    if not rows:
        print("  [BRAK WYNIKÓW]")
        return

    for i, row in enumerate(rows, 1):
        print(f"  {i}. {row}")

# --- STRESS TESTY ---

def run_stress_test_push():
    print_header("Stress Test: PUSH")
    
    try:
        followers_count = int(input("Podaj liczbę obserwujących: "))
        posts_count = int(input("Podaj liczbę postów do napisania: "))
        post_content = input("Treść posta testowego: ")
    except ValueError:
        print("Błąd: Wprowadzono niepoprawne liczby.")
        return

    print_separator()
    print("PRZYGOTOWANIE ŚRODOWISKA (Wyczyszczenie i generowanie obserwatorów)...")
    
    try:
        sess = db.get_session()
        # 1. Czyszczenie
        tables = ["kto_mnie_obserwuje", "moja_os_czasu", "kogo_obserwuje", "moje_posty", "user_stats"]
        for table in tables:
            sess.execute(f"TRUNCATE {table}")

        start_time = time.perf_counter()
        celebrity = "CelebrityUser"

        # 2. Generowanie danych w pamięci        
        insert_kto_mnie = timeline_service.get_prepared('INSERT_KTO_MNIE_OBSERWUJE')
        insert_kogo = timeline_service.get_prepared('INSERT_KOGO_OBSERWUJE')
        
        data_kto_mnie = [] # Celebryta <- Fan
        data_kogo = []     # Fan -> Celebryta
        
        for i in range(followers_count):
            follower = f"Follower_{i}"
            data_kto_mnie.append((celebrity, follower))
            data_kogo.append((follower, celebrity))

        # 3. Równoległy zapis do bazy
        execute_concurrent_with_args(sess, insert_kto_mnie, data_kto_mnie, concurrency=100)
        execute_concurrent_with_args(sess, insert_kogo, data_kogo, concurrency=100)
        
        # 4. Aktualizacja licznika
        update_stats = timeline_service.get_prepared('INC_FOLLOWERS_PULL_TEST')
        sess.execute(update_stats, [followers_count, celebrity])
        
        setup_time = time.perf_counter() - start_time
        print(f"-> Setup zajął {setup_time:.2f}s. {celebrity} ma {followers_count} fanów.")
        
        print("\nSTART TESTU PUSH...")
        post_start_time = time.perf_counter()

        # Właściwy test - Celebryta pisze posty
        for i in range(posts_count):
            timeline_service.post(celebrity, f"{post_content} #{i+1}")

        post_end_time = time.perf_counter()
        
        # Obliczenia
        duration_ms = (post_end_time - post_start_time) * 1000
        avg_per_post = duration_ms / posts_count

        print_header("Wyniki testu PUSH")
        print(f"Liczba fanów: {followers_count}")
        print(f"Czas publikacji {posts_count} postów: {duration_ms:.2f} ms")
        print(f"Średnio na post: {avg_per_post:.2f} ms")
        
    except Exception as e:
        print(f"BŁĄD PODCZAS TESTU: {e}")
    
    input("\nNaciśnij ENTER, aby wrócić do menu...")

def run_stress_test_pull():
    print_header("Stress Test: PULL")

    try:
        celebrity_count = int(input("Podaj liczbę celebrytów do obserwowania: "))
        pull_count = int(input("Ile razy pobrać timeline: "))
    except ValueError:
        print("Błąd: Wprowadzono niepoprawne liczby.")
        return

    print_separator()
    print("PRZYGOTOWANIE ŚRODOWISKA...")

    try:
        sess = db.get_session()
        tables = ["kto_mnie_obserwuje", "moja_os_czasu", "kogo_obserwuje", "moje_posty", "user_stats"]
        for table in tables:
            sess.execute(f"TRUNCATE {table}")

        start_time = time.perf_counter()
        follower = "User"

        # 1. Generowanie danych        
        insert_kogo = timeline_service.get_prepared('INSERT_KOGO_OBSERWUJE')
        insert_moje_posty = timeline_service.get_prepared('INSERT_MOJE_POSTY')
        update_stats = timeline_service.get_prepared('INC_FOLLOWERS_PULL_TEST')

        data_obs = []       # User -> Celebryta
        data_posts = []     # Posty celebrytów
        data_stats = []     # Liczniki celebrytów
        
        limit = timeline_service.CELEBRITY_TRESHOLD + 100 # Wartość powyżej progu

        for i in range(celebrity_count):
            cel = f"Celeb_{i}"
            # User obserwuje celebrytę
            data_obs.append((follower, cel))
            # Celebryta ma post (żeby było co pobierać)
            data_posts.append((cel, uuid.uuid1(), "Test post for pull."))
            # Celebryta staje się "celebrytą" (licznik)
            data_stats.append((limit, cel))

        # 2. Równoległy zapis
        execute_concurrent_with_args(sess, insert_kogo, data_obs, concurrency=100)
        execute_concurrent_with_args(sess, insert_moje_posty, data_posts, concurrency=100)
        execute_concurrent_with_args(sess, update_stats, data_stats, concurrency=100)

        setup_time = time.perf_counter() - start_time
        print(f"\n-> Gotowe. Setup zajął {setup_time:.2f}s.")
        print(f"-> {follower} obserwuje {celebrity_count} celebrytów.")
        
        print("\nSTART TESTU PULL (Get Timeline)...")
        pull_start_time = time.perf_counter()

        for i in range(pull_count):
            timeline_service.get_timeline(follower)

        pull_end_time = time.perf_counter()

        # Obliczenia
        duration_ms = (pull_end_time - pull_start_time) * 1000
        avg_per_pull = duration_ms / pull_count

        print_header("WYNIKI TESTU PULL")
        print(f"Liczba obserwowanych celebrytów: {celebrity_count}")
        print(f"Czas {pull_count} pobrań: {duration_ms:.2f} ms")
        print(f"Średnio na pobranie: {avg_per_pull:.2f} ms")

    except Exception as e:
        print(f"BŁĄD PODCZAS TESTU: {e}")
        import traceback
        traceback.print_exc()

    input("\nNaciśnij ENTER, aby wrócić do menu...")

# --- GŁÓWNA PĘTLA APLIKACJI ---

def main_menu():
    db.connect()
    print("Połączono z bazą danych Cassandra.")
    
    current_user = input("\nPodaj nazwę użytkownika, aby się zalogować: ").strip()
    if not current_user:
        print("Nie podano użytkownika. Zamykanie.")
        return

    while True:
        clear_screen()
        print_header(f"Cassandra Timeline CLI | Zalogowany: {current_user}")
        print("1. Moja Oś Czasu (Timeline)")
        print("2. Mój Profil (Moje posty)")
        print("3. Napisz Post")
        print("4. Zaobserwuj Użytkownika")
        print("5. Przestań obserwować")
        print("6. Kogo obserwuję")
        print("7. Kto mnie obserwuje")
        print("-" * 30)
        print("8. Stress Test: PUSH (Celebrity post -> Fans)")
        print("9. Stress Test: PULL (User <- Celebrities)")
        print("-" * 30)
        print("10. Zmień użytkownika")
        print("0. Wyjście")
        print_separator()
        
        choice = input("Wybierz opcję: ")

        try:
            if choice == '1':
                print_header(f"Oś czasu: {current_user}")
                rows = timeline_service.get_timeline(current_user)
                format_rows(rows)
                input("\n[Enter] aby wrócić...")

            elif choice == '2':
                print_header(f"Profil: {current_user}")
                rows = timeline_service.get_profile(current_user)
                format_rows(rows)
                input("\n[Enter] aby wrócić...")

            elif choice == '3':
                print_header("Nowy Post")
                content = input("Treść: ")
                if content:
                    timeline_service.post(current_user, content)
                    print("\n>> Post opublikowany pomyślnie!")
                else:
                    print(">> Anulowano (pusta treść).")
                time.sleep(1.5)

            elif choice == '4':
                print_header("Obserwuj")
                target = input("Kogo chcesz obserwować?: ")
                if target:
                    timeline_service.follow_user(current_user, target)
                    print(f"\n>> Zaobserwowano użytkownika {target}!")
                else:
                    print(">> Anulowano.")
                time.sleep(1.5)

            elif choice == '5':
                print_header("Przestań obserwować")
                target = input("Kogo chcesz przestać obserwować?: ")
                if target:
                    timeline_service.unfollow_user(current_user, target)
                    print(f"\n>> Przestałeś obserwować użytkownika {target}!")
                else:
                    print(">> Anulowano.")
                time.sleep(1.5)

            elif choice == '6':
                print_header(f"Kogo obserwujesz: {current_user}")
                following_list = timeline_service.get_following_list(current_user)
                if following_list:
                    for i, username in enumerate(following_list, 1):
                        print(f"  {i}. {username}")
                else:
                    print("  [NIKOGO NIE OBSERWUJESZ]")
                input("\n[Enter] aby wrócić...")

            elif choice == '7':
                print_header(f"Kto Cię obserwuje: {current_user}")
                followers_list = timeline_service.get_followers_list(current_user)
                if followers_list:
                    for i, username in enumerate(followers_list, 1):
                        print(f"  {i}. {username}")
                else:
                    print("  [NIKT CIĘ NIE OBSERWUJE]")
                input("\n[Enter] aby wrócić...")

            elif choice == '8':
                run_stress_test_push()

            elif choice == '9':
                run_stress_test_pull()

            elif choice == '10':
                new_user = input("Podaj nową nazwę użytkownika: ").strip()
                if new_user:
                    current_user = new_user
            
            elif choice == '0':
                print("Zamykanie...")
                break
            
            else:
                print("Niepoprawna opcja.")
                time.sleep(1)

        except Exception as e:
            print(f"Błąd: {e}")
            input("\nNaciśnij ENTER, aby kontynuować...")

    db.close()
    print("Rozłączono.")

if __name__ == "__main__":
    main_menu()
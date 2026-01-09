import backend as db
import services as timeline_service
import time
import os

# --- NARZĘDZIA POMOCNICZE ---

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
    print("CELEBRYTA pisze do wielu fanów.")
    
    try:
        followers_count = int(input("Podaj liczbę obserwujących: "))
        posts_count = int(input("Podaj liczbę postów do napisania: "))
        post_content = input("Treść posta testowego: ")
    except ValueError:
        print("Błąd: Wprowadzono niepoprawne liczby.")
        return

    print_separator()
    print("PRZYGOTOWANIE ŚRODOWISKA...")
    
    try:
        sess = db.get_session()
        # Czyszczenie tabel
        tables = ["kto_mnie_obserwuje", "moja_os_czasu", "kogo_obserwuje", "moje_posty", "user_stats"]
        for table in tables:
            sess.execute(f"TRUNCATE {table}")

        start_time = time.perf_counter()
        celebrity = "CelebrityUser"

        print(f"-> Generowanie {followers_count} obserwujących dla {celebrity}...")
        for i in range(followers_count):
            follower = f"Follower_{i}"
            timeline_service.follow_user(follower, celebrity)
            if i % 100 == 0:
                print(f"   ...dodano {i} fanów", end='\r')
        
        print(f"\n-> Gotowe. {celebrity} ma {followers_count} fanów.")
        
        print("\nStart testu...")
        post_start_time = time.perf_counter()

        for i in range(posts_count):
            timeline_service.post(celebrity, f"{post_content} #{i+1}")
            print(f"-> Opublikowano post #{i+1}")

        post_end_time = time.perf_counter()
        
        # Obliczenia
        duration_ms = (post_end_time - post_start_time) * 1000
        avg_per_post = duration_ms / posts_count
        total_duration = time.perf_counter() - start_time

        print_header("Wyniki testu")
        print(f"Czas publikacji {posts_count} postów: {duration_ms:.2f} ms")
        print(f"Średnio na post: {avg_per_post:.2f} ms")
        print(f"Całkowity czas testu: {total_duration:.2f} s")
        
    except Exception as e:
        print(f"BŁĄD PODCZAS TESTU: {e}")
    
    input("\nNaciśnij ENTER, aby wrócić do menu...")

def run_stress_test_pull():
    print_header("Stress Test: PULL")
    print("UŻYTKOWNIK pobiera timeline od wielu celebrytów.")

    try:
        celebrity_count = int(input("Podaj liczbę celebrytów: "))
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

        print(f"-> Tworzenie {celebrity_count} celebrytów i obserwacji...")
        for i in range(celebrity_count):
            celebrity = f"Celebrity_{i}"
            timeline_service.follow_user(follower, celebrity)
            timeline_service.post(celebrity, "Example post for pull test.")
           
            sess.execute(timeline_service.PREPARED_INC_FOLLOWERS_pull_test, 
                         [timeline_service.CELEBRITY_TRESHOLD, celebrity])
            if i % 50 == 0:
                print(f"Utworzono {i} celebrytów", end='\r')

        print(f"\n-> Gotowe. {follower} obserwuje {celebrity_count} osób.")
        
        print("\nROZPOCZYNAM POMIAR POBIERANIA TIMELINE...")
        pull_start_time = time.perf_counter()

        for i in range(pull_count):
            timeline_service.get_timeline(follower)
            print(f"-> Pobranie #{i+1} zakończone")

        pull_end_time = time.perf_counter()

        # Obliczenia
        duration_ms = (pull_end_time - pull_start_time) * 1000
        avg_per_pull = duration_ms / pull_count
        total_duration = time.perf_counter() - start_time

        print_header("WYNIKI TESTU PULL")
        print(f"Czas {pull_count} pobrań: {duration_ms:.2f} ms")
        print(f"Średnio na pobranie: {avg_per_pull:.2f} ms")
        print(f"Całkowity czas testu: {total_duration:.2f} s")

    except Exception as e:
        print(f"BŁĄD PODCZAS TESTU: {e}")

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
        print("-" * 30)
        print("5. Stress Test: PUSH (Celebrity post -> Fans)")
        print("6. Stress Test: PULL (User <- Celebrities)")
        print("-" * 30)
        print("7. Zmień użytkownika")
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
                run_stress_test_push()

            elif choice == '6':
                run_stress_test_pull()

            elif choice == '7':
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
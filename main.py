import backend as db
import services as timeline_service
import time

def print_rows(rows):
    count = 0
    for row in rows:
        count += 1
        print(f"  -> {row}")
    if count == 0:
        print("  --- (Brak wyników) ---")

def run_stress_test():
    # Implementacja z MVP 13...
    print("Uruchamiam Stress Test...")
    start_time = time.perf_counter()

    celebrity = "CelebrityUser"
    print("Setup: Tworzenie 5000 obserwujących...")
    for i in range(5000):
        follower = f"Follower_{i}"
        timeline_service.follow_user(follower, celebrity)

    print(f"Setup complete. 5000 followers now observe {celebrity}.")
    print("TEST: Mierzenie czasu publikacji 1 posta (5001 zapisów)...")

    post_start_time = time.perf_counter()
    timeline_service.post(celebrity, "My new stress test post!")
    post_end_time = time.perf_counter()

    duration_ms = (post_end_time - post_start_time) * 1000
    total_duration = (time.perf_counter() - start_time)

    print(f"\n--- WYNIK STRESS TESTU ---")
    print(f"Publikacja 1 posta (fan-out do 5000 osób) zajęła: {duration_ms:.2f} ms")
    print(f"Cały test (z setupem) zajął: {total_duration:.2f} s")
    print("----------------------------\n")

def main_loop():
    # Zamiast logowania, po prostu "ustawiamy" bieżącego użytkownika
    current_user = input("Podaj swoją nazwę użytkownika: ")

    while True:
        print("\n" + "="*20)
        print(f"Aktywny użytkownik: {current_user}")
        print("MENU:")
        print("1. Zmień użytkownika")
        print("2. Pokaż moją oś czasu")
        print("3. Pokaż mój profil")
        print("4. Publikuj post")
        print("5. Obserwuj kogoś")
        print("6. Uruchom Stress Test")
        print("0. Zakończ")
        print("="*20)

        option = input("Wybór: ")

        try:
            if option == '1':
                current_user = input("Podaj nazwę użytkownika: ")

            elif option == '2':
                print(f"--- Oś czasu dla {current_user} ---")
                rows = timeline_service.get_timeline(current_user)
                print_rows(rows)

            elif option == '3':
                print(f"--- Profil {current_user} ---")
                rows = timeline_service.get_profile(current_user)
                print_rows(rows)

            elif option == '4':
                content = input("Treść posta: ")
                timeline_service.post(current_user, content)
                print("Post opublikowany!")

            elif option == '5':
                user_to_follow = input("Kogo chcesz obserwować: ")
                timeline_service.follow_user(current_user, user_to_follow)
                print(f"Obserwujesz teraz {user_to_follow}!")

            elif option == '6':
                run_stress_test() 

            elif option == '0':
                print("Zamykanie...")
                break

            else:
                print("Nieznana opcja.")

        except Exception as e:
            print(f"WYSTĄPIŁ BŁĄD: {e}")

if __name__ == "__main__":
    try:
        db.connect()
        main_loop()
    except Exception as e:
        print(f"KRYTYCZNY BŁĄD POŁĄCZENIA: {e}")
    finally:
        db.close()
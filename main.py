import backend as db
import services as timeline_service
import time
import PySimpleGUI as sg
import os

def print_rows(rows):
    count = 0
    result = ""
    for row in rows:
        count += 1
        result += f"  -> @{row.author_username}: {row.content} ({row.post_id})\n"
    if count == 0:
        result = "  --- (Brak wyników) ---\n"
    return result

def run_stress_test():
    # Okno konfiguracji stress testu
    stress_layout = [
        [sg.Text("Konfiguracja Stress Test")],
        [sg.Text("Liczba obserwujących:"), sg.InputText(default_text="1000", key='followers_count')],
        [sg.Text("Tresc posta:"), sg.InputText(default_text="My stress test post!", key='post_content')],
        [sg.Button('Uruchom Test'), sg.Button('Anuluj')]
    ]
    
    stress_window = sg.Window('Stress Test Config', stress_layout, location=(1920, None))
    
    while True:
        event, values = stress_window.read()
        
        if event in (sg.WIN_CLOSED, 'Anuluj'):
            stress_window.close()
            return "Stress test anulowany.\n"
        
        if event == 'Uruchom Test':
            try:
                followers_count = int(values['followers_count'])
                post_content = values['post_content']
                stress_window.close()
                break
            except ValueError:
                sg.popup_error("Liczba obserwujących musi być liczbą!", location=(1920, None))
                continue
    
    # CZYSZCZENIE PRZED TESTEM - polskie nazwy
    sess = db.get_session()
    sess.execute("TRUNCATE kto_mnie_obserwuje")
    sess.execute("TRUNCATE moja_os_czasu")
    sess.execute("TRUNCATE kogo_obserwuje")
    
    # Wykonanie stress testu z podanymi parametrami
    result = f"Uruchamiam Stress Test z {followers_count} obserwującymi...\n"
    start_time = time.perf_counter()

    celebrity = "CelebrityUser"
    
    # --- NOWY, SZYBKI SETUP ---
    result += f"Setup: Generowanie {followers_count} obserwujących w pamięci...\n"
    followers_list = [f"Follower_{i}" for i in range(followers_count)]
    
    result += f"Setup: Wstawianie {followers_count} obserwujących do bazy (BATCH)...\n"
    
    # Wywołujemy nową funkcję wsadową zamiast wolnej pętli
    try:
        timeline_service.stress_add_followers_batch(celebrity, followers_list)
        result += "Setup: Wstawianie wsadowe zakończone.\n"
    except Exception as e:
        result += f"BŁĄD W TRAKCIE BATCHA: {e}\n"
        return result
    # --- KONIEC NOWEGO SETUPU ---

    result += f"Setup complete. {followers_count} followers now observe {celebrity}.\n"
    result += f"TEST: Mierzenie czasu publikacji posta: '{post_content}'...\n"

    post_start_time = time.perf_counter()
    timeline_service.post(celebrity, post_content)
    post_end_time = time.perf_counter()

    duration_ms = (post_end_time - post_start_time) * 1000
    total_duration = (time.perf_counter() - start_time)

    result += f"\n--- WYNIK STRESS TESTU ---\n"
    result += f"Treść posta: '{post_content}'\n"
    
    result += f"Publikacja posta przez celebrytę (bez fan-out) zajęła: {duration_ms:.2f} ms\n"

    # Zmierzmy czas odczytu (PULL) przez jednego z followerów
    test_follower = "Follower_1"
    pull_start_time = time.perf_counter()
    try:
        timeline_service.get_timeline(test_follower)
        pull_end_time = time.perf_counter()
        pull_duration_ms = (pull_end_time - pull_start_time) * 1000
        result += f"Odczyt osi czasu (PULL) przez '{test_follower}' zajął: {pull_duration_ms:.2f} ms\n"
    except Exception as e:
        result += f"Błąd przy pomiarze PULL: {e}\n"

    result += f"Cały test (z setupem) zajął: {total_duration:.2f} s\n"
    result += "----------------------------\n"
    return result

def main_loop():
    # Okno logowania
    login_layout = [
        [sg.Text("Podaj swoją nazwę użytkownika:")],
        [sg.InputText(key='username')],
        [sg.Button('Zaloguj'), sg.Button('Anuluj')]
    ]
    
    login_window = sg.Window('Login', login_layout, location=(1920,None))
    
    while True:
        event, values = login_window.read()
        
        if event in (sg.WIN_CLOSED, 'Anuluj'):
            login_window.close()
            return
            
        if event == 'Zaloguj' and values['username']:
            current_user = values['username']
            login_window.close()
            break
    
    # Główne menu
    main_layout = [
        [sg.Text(f"Aktywny użytkownik: {current_user}", key='current_user_display')], 
        [sg.Text(f"Liczba obserwujących: 0", key='followers_counter')],
        [sg.Button('Zmień użytkownika')],
        [sg.Button('Pokaż moją oś czasu')],
        [sg.Button('Pokaż mój profil')],
        [sg.Button('Publikuj post')],
        [sg.Button('Obserwuj kogoś')],
        [sg.Button('Uruchom Stress Test')],
        [sg.Button('Zakończ')],
        [sg.Multiline(size=(80, 20), key='output', disabled=True)]
    ]
    
    main_window = sg.Window('Libertyn App', main_layout, location=(1920,None), finalize=True)
    
    # Funkcja do odświeżania liczby obserwujących
    def refresh_followers_count():
        try:
            followers_count = timeline_service.get_followers_count(current_user)
            main_window['followers_counter'].update(f"Liczba obserwujących: {followers_count}")
        except Exception as e:
            print(f"DEBUG: Błąd przy refresh: {e}")
            main_window['followers_counter'].update("Liczba obserwujących: 0")
    
    # Początkowe odświeżenie - DOPIERO PO finalize=True
    refresh_followers_count()
    
    while True:
        event, values = main_window.read()
        
        if event in (sg.WIN_CLOSED, 'Zakończ'):
            break
            
        try:
            if event == 'Zmień użytkownika':
                new_user = sg.popup_get_text('Podaj nową nazwę użytkownika:', location=(1920,None))
                if new_user:
                    current_user = new_user
                    main_window['current_user_display'].update(f"Aktywny użytkownik: {current_user}")
                    refresh_followers_count()

            elif event == 'Pokaż moją oś czasu':
                try:
                    rows = timeline_service.get_timeline(current_user)
                    output = f"--- Oś czasu dla {current_user} ---\n"
                    output += print_rows(rows)
                    main_window['output'].update(output)
                except Exception as e:
                    main_window['output'].update(f"Błąd przy pobieraniu osi czasu: {e}\n")
                
            elif event == 'Pokaż mój profil':
                try:
                    rows = timeline_service.get_profile(current_user)
                    output = f"--- Profil {current_user} ---\n"
                    output += print_rows(rows)
                    main_window['output'].update(output)
                except Exception as e:
                    main_window['output'].update(f"Błąd przy pobieraniu profilu: {e}\n")
                
            elif event == 'Publikuj post':
                content = sg.popup_get_text('Treść posta:', location=(1920,None))
                if content:
                    try:
                        timeline_service.post(current_user, content)
                        main_window['output'].update(f"Post opublikowany: '{content}'\n")
                        refresh_followers_count()
                    except Exception as e:
                        main_window['output'].update(f"Błąd przy publikowaniu: {e}\n")
                    
            elif event == 'Obserwuj kogoś':
                user_to_follow = sg.popup_get_text('Kogo chcesz obserwować:', location=(1920,None))
                if user_to_follow:
                    try:
                        # Nie musimy już sprawdzać count, funkcja follow_user robi to sama
                        timeline_service.follow_user(current_user, user_to_follow)
                        main_window['output'].update(f"Obserwujesz teraz {user_to_follow}!\n")
                    except Exception as e:
                        main_window['output'].update(f"Błąd przy obserwowaniu: {e}\n")
                    
            elif event == 'Uruchom Stress Test':
                try:
                    result = run_stress_test()
                    main_window['output'].update(result)
                    refresh_followers_count() # Odśwież dla "CelebrityUser" jeśli jest aktywny
                except Exception as e:
                    main_window['output'].update(f"Błąd w stress test: {e}\n")
                
        except Exception as e:
            main_window['output'].update(f"NIEOCZEKIWANY BŁĄD: {e}\n")
    
    main_window.close()

if __name__ == "__main__":
    try:
        db.connect()
        main_loop()
    except Exception as e:
        sg.popup_error(f"KRYTYCZNY BŁĄD POŁĄCZENIA: {e}", location=(1920,None))
    finally:
        db.close()
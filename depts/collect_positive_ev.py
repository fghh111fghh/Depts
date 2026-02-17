# collect_positive_ev.py
import os
import glob
import pandas as pd
import re

def collect_positive_ev(folder='positive_ev', output_pickle='calibration_summary.pkl'):
    """
    Собирает все CSV-файлы из указанной папки, добавляет колонки target и last_matches,
    объединяет в один DataFrame и сохраняет в pickle.
    """
    # Находим все CSV-файлы в папке
    csv_files = glob.glob(os.path.join(folder, 'positive_ev_*.csv'))
    if not csv_files:
        print(f"В папке '{folder}' не найдено файлов с шаблоном 'positive_ev_*.csv'")
        return

    print(f"Найдено файлов: {len(csv_files)}")
    data_frames = []

    for fpath in csv_files:
        fname = os.path.basename(fpath)
        # Извлекаем target и last_matches из имени файла
        # ожидаемый формат: positive_ev_over_n5.csv или positive_ev_under_n10.csv
        match = re.match(r'positive_ev_(over|under)_n(\d+)\.csv', fname)
        if not match:
            print(f"⚠️ Пропущен файл с неожиданным именем: {fname}")
            continue

        target, n_str = match.groups()
        n = int(n_str)

        try:
            df = pd.read_csv(fpath, encoding='utf-8')
            # Добавляем колонки с метаданными
            df['target'] = target
            df['last_matches'] = n
            data_frames.append(df)
            print(f"✅ {fname}: {len(df)} строк, target={target}, n={n}")
        except Exception as e:
            print(f"❌ Ошибка чтения {fname}: {e}")

    if not data_frames:
        print("Нет данных для сохранения.")
        return

    # Объединяем все данные
    combined = pd.concat(data_frames, ignore_index=True, sort=False)

    # Сохраняем в pickle
    combined.to_pickle(output_pickle)
    print(f"\nСохранено {len(combined)} записей в {output_pickle}")

    # Краткая статистика
    print("\nСтатистика по файлам:")
    stats = combined.groupby(['target', 'last_matches']).size().reset_index(name='count')
    print(stats.to_string(index=False))

if __name__ == "__main__":
    collect_positive_ev()
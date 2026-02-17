# check_files.py
import os
import pandas as pd
import glob
import csv
import sys
from collections import defaultdict


def detect_separator(file_path, sample_size=4096):
    """Определяет разделитель CSV-файла."""
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        sample = f.read(sample_size)
        # Проверяем популярные разделители
        if ';' in sample:
            lines = sample.splitlines()
            if len(lines) > 1:
                cols_count = len(lines[1].split(';'))
                if cols_count > 1:
                    return ';'
        if ',' in sample:
            lines = sample.splitlines()
            if len(lines) > 1:
                cols_count = len(lines[1].split(','))
                if cols_count > 1:
                    return ','
        # Если не удалось, используем sniffer
        try:
            dialect = csv.Sniffer().sniff(sample)
            return dialect.delimiter
        except:
            # По умолчанию запятая
            return ','


def check_file(file_path, required_cols=None):
    """
    Проверяет один файл: читает, проверяет колонки, возвращает информацию.
    Возвращает словарь с результатом или None при критической ошибке.
    """
    if required_cols is None:
        required_cols = ['Div', 'Date', 'HomeTeam', 'AwayTeam', 'FTHG', 'FTAG']

    file_name = os.path.basename(file_path)
    try:
        sep = detect_separator(file_path)
        # Читаем только первые несколько строк для проверки
        df_sample = pd.read_csv(file_path, sep=sep, nrows=5, encoding='utf-8', on_bad_lines='skip')
        df_sample.columns = [col.strip() for col in df_sample.columns]

        # Проверяем обязательные колонки
        missing = [col for col in required_cols if col not in df_sample.columns]
        if missing:
            return {'file': file_name, 'status': 'ERROR', 'reason': f'Отсутствуют колонки {missing}'}

        # Проверяем наличие хотя бы одной колонки с тоталом
        odds_cols = [c for c in df_sample.columns if ('>2.5' in c or '<2.5' in c)]
        if not odds_cols:
            return {'file': file_name, 'status': 'WARNING', 'reason': 'Нет колонок с тоталом'}

        # Читаем весь файл для получения полной информации
        df = pd.read_csv(file_path, sep=sep, low_memory=False, encoding='utf-8', on_bad_lines='skip')
        df.columns = [col.strip() for col in df.columns]

        # Преобразуем дату
        try:
            df['Date_parsed'] = pd.to_datetime(df['Date'], format='%d/%m/%Y', errors='coerce')
        except:
            df['Date_parsed'] = pd.to_datetime(df['Date'], errors='coerce')

        valid_dates = df['Date_parsed'].notna()
        total_rows = len(df)
        valid_rows = valid_dates.sum()
        invalid_dates = total_rows - valid_rows

        # Уникальные лиги
        leagues = df['Div'].unique().tolist() if 'Div' in df.columns else []

        # Диапазон дат
        if valid_rows > 0:
            min_date = df.loc[valid_dates, 'Date_parsed'].min()
            max_date = df.loc[valid_dates, 'Date_parsed'].max()
        else:
            min_date = max_date = None

        return {
            'file': file_name,
            'status': 'OK',
            'separator': sep,
            'total_rows': total_rows,
            'valid_rows': valid_rows,
            'invalid_dates': invalid_dates,
            'leagues': leagues,
            'min_date': min_date,
            'max_date': max_date,
            'odds_cols_present': odds_cols[:5],  # первые 5 для информации
            'reason': None
        }
    except Exception as e:
        return {'file': file_name, 'status': 'ERROR', 'reason': str(e)}


def main(folder='for_calibration'):
    csv_files = glob.glob(os.path.join(folder, '**', '*.csv'), recursive=True)
    if not csv_files:
        csv_files = glob.glob(os.path.join(folder, '*.csv'))

    print(f"Найдено CSV-файлов: {len(csv_files)}\n")

    results = []
    for fpath in csv_files:
        info = check_file(fpath)
        results.append(info)
        if info['status'] == 'OK':
            print(
                f"✅ {info['file']}: {info['valid_rows']} строк, лиги {info['leagues']}, даты {info['min_date']} - {info['max_date']}")
        elif info['status'] == 'WARNING':
            print(f"⚠️ {info['file']}: {info['reason']}")
        else:
            print(f"❌ {info['file']}: {info['reason']}")

    # Сводка
    total = len(results)
    ok = sum(1 for r in results if r['status'] == 'OK')
    warn = sum(1 for r in results if r['status'] == 'WARNING')
    err = total - ok - warn

    print("\n" + "=" * 50)
    print(f"ИТОГО: OK: {ok}, WARNING: {warn}, ERROR: {err}")

    # Список всех лиг из успешных файлов
    all_leagues = set()
    for r in results:
        if r['status'] == 'OK':
            all_leagues.update(r['leagues'])
    print(f"Уникальные лиги в OK-файлах: {sorted(all_leagues)}")


if __name__ == "__main__":
    # Можно передать путь к папке как аргумент командной строки
    folder = sys.argv[1] if len(sys.argv) > 1 else 'for_calibration'
    main(folder)
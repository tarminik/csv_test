import argparse
from csv_processor import process_csv, AggregationError, FilterError
from tabulate import tabulate

# Основная функция
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CSV Processor")
    parser.add_argument('--file', required=True, help='Путь к CSV-файлу')
    parser.add_argument('--where', help='Фильтрация, например: "rating>4.7" или "brand=apple"')
    parser.add_argument('--aggregate', help='Агрегация, например: "rating=avg" или "rating=min"')
    args = parser.parse_args()

    try:
        result, headers = process_csv(
            file_path=args.file,
            where=args.where,
            aggregate=args.aggregate
        )
        print(tabulate(result, headers=headers, tablefmt="grid"))
    except (AggregationError, FilterError, FileNotFoundError, ValueError) as e:
        print(f"Ошибка: {e}") 
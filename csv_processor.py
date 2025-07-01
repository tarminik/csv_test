from typing import List, Tuple, Optional
import csv
import operator

class AggregationError(Exception):
    pass

class FilterError(Exception):
    pass

def parse_where(where: str) -> Tuple[str, str, str]:
    """
    Разбирает строку where на (column, op, value).
    Поддерживает операторы: =, >, <
    """
    if '>=' in where or '<=' in where:
        raise FilterError('Операторы >= и <= не поддерживаются')
    for op in ['>', '<', '=']:
        if op in where:
            parts = where.split(op)
            if len(parts) != 2:
                raise FilterError('Неверный формат условия фильтрации')
            column = parts[0].strip()
            value = parts[1].strip()
            return column, op, value
    raise FilterError('Поддерживаются только операторы =, >, <')

def parse_aggregate(aggregate: str) -> Tuple[str, str]:
    """
    Разбирает строку aggregate на (column, func_name).
    Пример: 'rating=avg' -> ('rating', 'avg')
    """
    if '=' not in aggregate:
        raise AggregationError('Агрегация должна быть в формате column=func')
    column, func = aggregate.split('=', 1)
    return column.strip(), func.strip().lower()

def process_csv(file_path: str, where: Optional[str], aggregate: Optional[str]) -> Tuple[List[List[str]], List[str]]:
    """
    Обрабатывает CSV-файл: фильтрация и агрегация.
    Возвращает кортеж (данные, заголовки).
    """
    with open(file_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        headers = reader.fieldnames if reader.fieldnames else []

    # Фильтрация (если требуется)
    if where:
        column, op, value = parse_where(where)
        if column not in headers:
            raise FilterError(f'Колонка "{column}" не найдена в файле')
        op_map = {'=': operator.eq, '>': operator.gt, '<': operator.lt}
        cmp_func = op_map[op]
        def try_cast(val):
            try:
                return float(val)
            except ValueError:
                return val
        value_casted = try_cast(value)
        filtered_rows = []
        for row in rows:
            cell = row[column]
            cell_casted = try_cast(cell)
            try:
                if cmp_func(cell_casted, value_casted):
                    filtered_rows.append(row)
            except Exception:
                continue
        rows = filtered_rows

    # Агрегация (если требуется)
    if aggregate:
        agg_column, agg_func = parse_aggregate(aggregate)
        if agg_column not in headers:
            raise AggregationError(f'Колонка "{agg_column}" не найдена в файле')
        # Собираем значения колонки, приводим к float
        try:
            values = [float(row[agg_column]) for row in rows]
        except ValueError:
            raise AggregationError(f'Колонка "{agg_column}" должна содержать только числа для агрегации')
        if not values:
            raise AggregationError('Нет данных для агрегации')
        # Реестр функций агрегации
        def avg(lst):
            return sum(lst) / len(lst)
        agg_funcs = {
            'avg': avg,
            'min': min,
            'max': max
        }
        if agg_func not in agg_funcs:
            raise AggregationError(f'Агрегация "{agg_func}" не поддерживается')
        result_value = agg_funcs[agg_func](values)
        # Формируем таблицу из одного значения
        return [[round(result_value, 2)]], [agg_func]

    # Если нет агрегации, возвращаем таблицу
    return [[row[h] for h in headers] for row in rows], headers 
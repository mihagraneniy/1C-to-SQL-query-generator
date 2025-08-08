import pandas as pd
import re
from typing import List, Dict, Union, Optional, Tuple, Set, Any

class SQLQueryGenerator:
    def __init__(self, mapping_file: str):
        """Инициализация с загрузкой файла маппинга"""
        self.df = pd.read_excel(mapping_file)
        self.validate_structure()
        self._queries: Dict[str, str] = {}
        self._aliases: Dict[str, Dict[str, str]] = {}  # {table_name: {alias: field}}
        self._join_info: Dict[str, Dict[str, str]] = {}  # {table_name: {alias: join_condition}}
    
    def validate_structure(self) -> None:
        """Проверка структуры файла маппинга"""
        required_columns = [
            'Имя таблицы 1С', 'Имя поля 1С', 'ТипПоля1С',
            'SQL таблица', 'SQL Имя поля',
            'SQLВнешняяТаблица', 'Связи'
        ]
        missing_cols = [col for col in required_columns if col not in self.df.columns]
        if missing_cols:
            raise ValueError(f"Отсутствуют обязательные столбцы: {missing_cols}")

    def get_available_tables(self) -> List[str]:
        """Возвращает список доступных таблиц 1С"""
        return list(self.df['Имя таблицы 1С'].unique())

    def _get_field_type_info(self, field_type: str) -> Tuple[str, str]:
        """Определяет тип поля и соответствующее поле для описания"""
        if pd.isna(field_type):
            return ("regular", "")
        
        type_category = field_type.split('.')[0]
        if type_category == 'Справочник':
            return ("reference", '_Description')
        elif type_category == 'Документ':
            return ("document", '_Number')
        elif type_category == 'Перечисление':
            return ("enum", '_EnumOrder')
        else:
            return ("reference", '_Description')

    def _process_regular_field(self, row: pd.Series, table_1c_name: str, join_aliases: Dict[str, str]) -> str:
        """Обработка обычного поля (не ссылочного)"""
        field_name = row['Имя поля 1С']
        gk_table = row['SQL таблица']
        gk_field = row['SQL Имя поля']
        
        select_part = f"{join_aliases[gk_table]}.{gk_field} AS \"{field_name}\""
        self._aliases[table_1c_name][field_name] = f"{join_aliases[gk_table]}.{gk_field}"
        return select_part

    def _process_reference_field(self, row: pd.Series, table_1c_name: str, 
                               join_aliases: Dict[str, str], alias_counter: int,
                               main_table: str, join_tracker: Set[str]) -> List[str]:
        """Обработка ссылочного поля (справочник, документ и т.д.)"""
        field_name = row['Имя поля 1С']
        gk_table = row['SQL таблица']
        gk_field = row['SQL Имя поля']
        external_table = row['SQLВнешняяТаблица']
        relation = row['Связи']
        
        _, external_field = self._get_field_type_info(row['ТипПоля1С'])
        select_parts = []
        
        # Добавляем оригинальное поле с _IDRRef
        select_part_id = f"{join_aliases[gk_table]}.{gk_field} AS \"{field_name}_ID\""
        select_parts.append(select_part_id)
        self._aliases[table_1c_name][f"{field_name}_ID"] = f"{join_aliases[gk_table]}.{gk_field}"
        
        # Обработка рекурсивных связей
        if external_table == main_table:
            alias = f"parent_{gk_field}"
            join_key = f"{external_table}_{alias}"
            
            if join_key not in join_tracker:
                join_condition = f"main.[{gk_field}] = {alias}.[_IDRRef]"
                self._add_join_info(table_1c_name, alias, f"LEFT JOIN {external_table} {alias} ON {join_condition}")
                join_tracker.add(join_key)
                join_aliases[external_table + gk_field] = alias
            
            select_part_desc = f"{alias}.{external_field} AS \"{field_name}\""
            self._aliases[table_1c_name][f"{field_name}"] = f"{alias}.{external_field}"
        else:
            if external_table not in join_aliases:
                alias = f"ext_{alias_counter}"
                alias_counter += 1
                join_aliases[external_table] = alias
                join_key = f"{external_table}_{alias}"
                
                if pd.notna(relation) and join_key not in join_tracker:
                    join_condition = relation.replace(f"[{external_table}]", alias)
                    join_condition = join_condition.replace(f"[{main_table}]", "main")
                    self._add_join_info(table_1c_name, alias, f"LEFT JOIN {external_table} {alias} ON {join_condition}")
                    join_tracker.add(join_key)
            
            select_part_desc = f"{join_aliases[external_table]}.{external_field} AS \"{field_name}\""
            self._aliases[table_1c_name][f"{field_name}"] = f"{join_aliases[external_table]}.{external_field}"
        
        select_parts.append(select_part_desc)
        return select_parts

    def _add_join_info(self, table_1c_name: str, alias: str, join_condition: str) -> None:
        """Добавляет информацию о JOIN в хранилище"""
        if table_1c_name not in self._join_info:
            self._join_info[table_1c_name] = {}
        self._join_info[table_1c_name][alias] = join_condition

    def generate_query(self, table_1c_name: str, include_aliases: Optional[List[str]] = None) -> Optional[str]:
        """
        Генерация SQL запроса для указанной таблицы 1С.
        
        Args:
            table_1c_name: Имя таблицы 1С
            include_aliases: Список алиасов для включения (None - все алиасы)
            
        Returns:
            Строка SQL запроса или None, если таблица не найдена
        """
        if table_1c_name in self._queries and include_aliases is None:
            return self._queries[table_1c_name]
            
        table_data = self.df[self.df['Имя таблицы 1С'] == table_1c_name]
        
        if table_data.empty:
            available_tables = self.get_available_tables()
            print(f"Таблица '{table_1c_name}' не найдена. Доступные таблицы:")
            print("\n".join(available_tables))
            return None
        
        main_table = table_data['SQL таблица'].iloc[0]
        join_aliases = {main_table: "main"}
        alias_counter = 0
        join_tracker: Set[str] = set()
        
        # Инициализация структур данных для хранения информации
        if table_1c_name not in self._aliases:
            self._aliases[table_1c_name] = {}
        if table_1c_name not in self._join_info:
            self._join_info[table_1c_name] = {}
        
        select_parts = []
        
        for _, row in table_data.iterrows():
            field_name = row['Имя поля 1С']
            
            # Пропускаем поля, которые не входят в запрошенные алиасы
            if include_aliases is not None and field_name not in include_aliases:
                continue
            
            field_type, _ = self._get_field_type_info(row['ТипПоля1С'])
            
            if field_type == "regular":
                select_part = self._process_regular_field(row, table_1c_name, join_aliases)
                select_parts.append(select_part)
            else:
                ref_parts = self._process_reference_field(
                    row, table_1c_name, join_aliases, alias_counter, main_table, join_tracker
                )
                select_parts.extend(ref_parts)
                alias_counter += 1  # Увеличиваем счетчик только для ссылочных полей
        
        # Собираем SQL запрос
        sql_query = "SELECT\n    " + ",\n    ".join(select_parts)
        sql_query += f"\nFROM {main_table} main"
        
        # Добавляем JOIN-ы
        if table_1c_name in self._join_info:
            for join_condition in self._join_info[table_1c_name].values():
                sql_query += "\n" + join_condition
        
        # Кэшируем запрос только если он полный (не фильтрованный по алиасам)
        if include_aliases is None:
            self._queries[table_1c_name] = sql_query
        
        return sql_query

    def get_query_aliases(self, table_1c_name: str) -> Dict[str, str]:
        """
        Возвращает словарь алиасов запроса для указанной таблицы 1С.
        
        Args:
            table_1c_name: Имя таблицы 1С
            
        Returns:
            Словарь вида {"алиас": "полное_имя_поля"}
            
        Raises:
            ValueError: Если таблица не найдена
        """
        if table_1c_name not in self._aliases:
            self.generate_query(table_1c_name)
            
        if table_1c_name not in self._aliases:
            raise ValueError(f"Таблица '{table_1c_name}' не найдена")
            
        return self._aliases[table_1c_name]

    def get_table_join_info(self, table_1c_name: str) -> Dict[str, str]:
        """
        Возвращает информацию о JOIN-ах таблицы.
        
        Args:
            table_1c_name: Имя таблицы 1С
            
        Returns:
            Словарь {алиас_таблицы: строка_JOIN}
        """
        if table_1c_name not in self._join_info:
            self.generate_query(table_1c_name)
            
        return self._join_info.get(table_1c_name, {})

 
    def rename_aliases(self, query: str, rename_dict: Dict[str, str]) -> str:
        """
        Корректно переименовывает алиасы в SQL-запросе, сохраняя запятые и кавычки.
        
        Args:
            query: Исходный SQL-запрос
            rename_dict: Словарь {старый_алиас: новый_алиас}
            
        Returns:
            Запрос с корректно переименованными алиасами
        """
        if not query or not rename_dict:
            return query
        
        # Нормализуем rename_dict (удаляем кавычки из ключей)
        normalized_rename = {k.strip('"\''): v for k, v in rename_dict.items()}
        
        lines = query.split('\n')
        new_lines = []
        in_select = False
        
        for line in lines:
            stripped_line = line.strip()
            
            # Определяем, находимся ли мы в секции SELECT
            if stripped_line.upper().startswith('SELECT'):
                in_select = True
                new_lines.append(line)
                continue
            elif stripped_line.upper().startswith(('FROM', 'WHERE', 'GROUP', 'HAVING', 'ORDER', 'JOIN')):
                in_select = False
                new_lines.append(line)
                continue
                
            # Если не в SELECT секции, пропускаем
            if not in_select:
                new_lines.append(line)
                continue
                
            # Проверяем, есть ли запятая в конце строки
            has_comma = stripped_line.endswith(',')
            clean_line = stripped_line.rstrip(', ')
            
            # Обрабатываем поле
            if ' AS ' in clean_line.upper():
                # Разделяем на выражение и алиас
                parts = clean_line.split(' AS ', 1)
                expr = parts[0].strip()
                alias_part = parts[1].strip()
                
                # Извлекаем алиас (удаляем кавычки если есть)
                if alias_part.startswith('"') and alias_part.endswith('"'):
                    alias = alias_part[1:-1]
                elif alias_part.startswith("'") and alias_part.endswith("'"):
                    alias = alias_part[1:-1]
                else:
                    alias = alias_part
                
                # Применяем переименование
                new_alias = normalized_rename.get(alias, alias)
                
                # Форматируем поле с сохранением отступа
                indent = ' ' * (len(line) - len(stripped_line))
                # Добавляем запятую ВНЕ кавычек
                comma = ',' if has_comma else ''
                new_field = f"{indent}{expr} AS \"{new_alias}\"{comma}"
                new_lines.append(new_field)
            else:
                # Если нет AS, оставляем как есть (но сохраняем запятую)
                if has_comma:
                    new_lines.append(line.rstrip(', ') + ',')
                else:
                    new_lines.append(line)
        
        return '\n'.join(new_lines)
    
    
    def generate_cte(self, metadata: Dict[str, List[str]]) -> str:
        """
        Генерирует CTE запрос, используя generate_query для каждой таблицы
        
        Параметры:
            metadata: {
                "Таблица1С": ["поле1", "поле2", ...],
                ...
            }
            
        Возвращает:
            SQL запрос с CTE для всех таблиц и SELECT со всеми полями
        """
        cte_parts = []
        select_fields = []
        first_cte = None
        
        for table_1c_name, fields in metadata.items():
            # Генерируем запрос для таблицы с указанными полями
            query = self.generate_query(table_1c_name, include_aliases=fields)
            if not query:
                continue
                
            # Формируем имя CTE (заменяем точки на подчеркивания)
            cte_name = table_1c_name.replace(".", "_")
            
            # Запоминаем первую CTE для FROM
            if first_cte is None:
                first_cte = cte_name
                
            # Добавляем CTE
            cte_parts.append(f"{cte_name} AS (\n{query}\n)")
            
            # Добавляем поля в SELECT
            for field in fields:
                select_fields.append(f"{cte_name}.\"{field}\"")
        
        if not cte_parts:
            return "Не удалось сгенерировать CTE для указанных таблиц"
        
        # Собираем финальный запрос
        sql = "WITH\n    " + ",\n    ".join(cte_parts)
        sql += "\nSELECT\n    " + ",\n    ".join(select_fields)
        sql += f"\nFROM {first_cte}"
        
        return sql

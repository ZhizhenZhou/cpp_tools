CREATE OR REPLACE FUNCTION create_table_function(tablename TEXT)
RETURNS VOID AS $$
BEGIN
    -- EXECUTE 'CREATE TABLE ' || tablename || ' (id SERIAL PRIMARY KEY, cellname TEXT DEFAULT NULL, filepath TEXT DEFAULT NULL, count INT DEFAULT 0)';
    EXECUTE 'CREATE TABLE ' || tablename || ' (id SERIAL PRIMARY KEY, cellname TEXT DEFAULT NULL, filepath TEXT DEFAULT NULL)';

    RAISE NOTICE 'Table created: %', tablename;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION add_missing_columns(tablename TEXT, column_list TEXT[])
RETURNS VOID AS $$
BEGIN
    EXECUTE format('ALTER TABLE %I %s', tablename, array_to_string(array(
        SELECT format('ADD COLUMN IF NOT EXISTS %I BOOLEAN DEFAULT false', column_name)
        FROM unnest(column_list) AS column_name
    ), ', '));
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION set_columns_to_true(tablename TEXT, column_list TEXT[], cellname TEXT, filepath TEXT)
RETURNS VOID AS $$
BEGIN
    -- EXECUTE format('UPDATE %I SET %s, count = count + (SELECT count(*) FROM unnest(%L::text[]) AS column_name) WHERE cellname = %L AND filepath = %L;', tablename, array_to_string(array(
    --     SELECT format('%I = true', column_name)
    --     FROM unnest(column_list) AS column_name
    -- ), ', '), column_list, cellname, filepath);

    EXECUTE format('UPDATE %I SET %s WHERE cellname = %L AND filepath = %L;', tablename, array_to_string(array(
        SELECT format('%I = true', column_name)
        FROM unnest(column_list) AS column_name
    ), ', '), cellname, filepath);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_table_columns(tablename TEXT, column_list TEXT[], cellname TEXT, filepath TEXT)
RETURNS VOID AS $$
DECLARE
    table_exists BOOLEAN;
    row_exists BOOLEAN;
BEGIN
    -- 检查表是否存在
    SELECT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_name = tablename
    ) INTO table_exists;

    IF NOT table_exists THEN
        -- 表不存在，调用创建表的函数
        PERFORM create_table_function(tablename);
    END IF;

    -- 检查行是否存在
    EXECUTE format('
        SELECT EXISTS (
            SELECT 1
            FROM %I
            WHERE cellname = %L AND filepath = %L
        )', tablename, cellname, filepath)
    INTO row_exists;

    -- 更新表项的 name 属性
    IF NOT row_exists THEN
        EXECUTE 'INSERT INTO ' || tablename || ' (cellname, filepath) VALUES (' || quote_literal(cellname) || ', ' || quote_literal(filepath) || ')';
    END IF;

    -- 检查并添加缺失的列
    PERFORM add_missing_columns(tablename, column_list);

    -- 将所有列的值置为 true
    PERFORM set_columns_to_true(tablename, column_list, cellname, filepath);
END;
$$ LANGUAGE plpgsql;

-- 使用示例
-- SELECT update_table_columns('your_table', ARRAY['column1', 'column2', 'column3'], 'ASDASD-8', '/mnt/c/file.csv');


CREATE OR REPLACE FUNCTION count_true_values(tablename text, columnlist text[])
RETURNS TABLE (cellname TEXT, filepath TEXT, count INTEGER) AS
$$
DECLARE
    querytext TEXT;
    i INTEGER;
    valid_columns text[];
BEGIN
    -- 从给定的列名列表和表中的列名取交集
    SELECT array_agg(column_name)
    INTO valid_columns
    FROM information_schema.columns
    WHERE table_name = tablename
        AND column_name = ANY(columnlist);

    RAISE NOTICE 'Valid columns: %', valid_columns;
    -- 检查是否存在有效列名，如果不存在则直接返回空结果
    IF valid_columns IS NULL THEN
        RETURN;
    END IF;

    -- 构建查询语句
    querytext := 'SELECT cellname, filepath, ';
    FOR i IN 1..array_length(valid_columns, 1) LOOP
        querytext := querytext || '(CASE WHEN ' || valid_columns[i] || ' = true THEN 1 ELSE 0 END)';
        IF i < array_length(valid_columns, 1) THEN
            querytext := querytext || ' + ';
        END IF;
    END LOOP;
    querytext := querytext || ' AS count FROM ' || tablename;

    -- 执行查询并返回结果
    RETURN QUERY EXECUTE querytext;
END;
$$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION generate_new_table(table_list text[])
RETURNS TABLE (cellname TEXT, filepath TEXT, total_count NUMERIC) AS $$
DECLARE
    query text;
BEGIN
    query := '
        SELECT cellname, filepath, SUM(count) AS total_count
        FROM (
    ';

    FOR i IN 1..array_length(table_list, 1) LOOP
        query := query || format('
            SELECT cellname::TEXT, filepath::TEXT, count::NUMERIC
            FROM %I', table_list[i]);
        
        IF i < array_length(table_list, 1) THEN
            query := query || '
            UNION ALL ';
        END IF;
    END LOOP;

    query := query || '
        ) AS combined_tables
        GROUP BY cellname, filepath';
    
    RETURN QUERY EXECUTE query;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION process_json_data(json_data text)
RETURNS TABLE (cellname TEXT, filepath TEXT, total_count NUMERIC) AS
$$
DECLARE
    table_name text;
    column_list text[];
    temp_table_name text;
    temp_table_list text[] := '{}';
    -- result_table RECORD;
    json_object jsonb;
    json_key text;
    json_value jsonb;

    -- alpha NUMERIC;
BEGIN
    -- alpha := 0.5;
    -- 构建 JSON 对象
    json_object := jsonb_build_object('data', json_data::jsonb);

    -- 使用 jsonb_each_text 遍历 JSON 对象的键值对
    FOR json_key, json_value IN SELECT * FROM jsonb_each(json_object->'data')
    LOOP
        -- 从 JSON 键值对中获取表名和列名列表
        table_name := json_key;
        column_list := ARRAY(SELECT jsonb_array_elements_text(json_value::jsonb));

        -- 替换表名中的下划线为破折号
        temp_table_name := 'tmp_' || table_name;
        RAISE NOTICE 'temp_tablename: %', temp_table_name;

        -- 调用第一个函数，并将结果存储到临时表中
        -- IF table_name = 'indexcode_00000000' THEN
        --     EXECUTE format('
        --         CREATE TEMPORARY TABLE %I AS
        --         SELECT cellname, filepath, count * (1 - %s) AS count
        --         FROM count_true_values(%L, %L)
        --     ', temp_table_name, alpha, table_name, column_list);
        -- ELSE
        --     EXECUTE format('
        --         CREATE TEMPORARY TABLE %I AS
        --         SELECT cellname, filepath, count * %s AS count
        --         FROM count_true_values(%L, %L)
        --     ', temp_table_name, alpha, table_name, column_list);
        -- END IF;

        EXECUTE format('
            CREATE TEMPORARY TABLE %I ON COMMIT DROP AS
            SELECT *
            FROM count_true_values(%L, %L)
        ', temp_table_name, table_name, column_list);

        -- 将临时表名添加到临时表列表中
        temp_table_list := array_append(temp_table_list, temp_table_name);
    END LOOP;

    -- 返回结果表
    RETURN QUERY
    SELECT * FROM generate_new_table(temp_table_list)
    ORDER BY total_count DESC;

END;
$$
LANGUAGE plpgsql;                                                                               
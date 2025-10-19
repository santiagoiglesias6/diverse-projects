-- =====================================================================
-- ANONIMIZED STORED PROCEDURE
-- WARNING: This version has replaced table, column, and function names
-- with generic labels. It contains no real data or references
-- to internal providers/functions.
-- =====================================================================

REPLACE PROCEDURE sp_upload_chatbot_resume_anonymized
(
    IN p_start_date DATE,
    IN p_end_date   DATE
)
SQL SECURITY INVOKER
BEGIN
    DECLARE v_process_name       VARCHAR(255) DEFAULT 'upload_chatbot_resume_anonymized';
    DECLARE v_start_ts           TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

    -- util: small logging calls replaced by a generic helper name
    CALL util_log_step(v_process_name, v_start_ts, 'INF', 'Start stored procedure');

    -- 1) Universe of conversations
    CALL util_drop_table_if_exists('tmp_chatbot_universe');
    CREATE VOLATILE TABLE tmp_chatbot_universe AS
    (
        SELECT
            EXTRACT(YEAR FROM message_ts) AS year_num,
            EXTRACT(MONTH FROM message_ts) AS month_num,
            (EXTRACT(YEAR FROM message_ts) * 100 + EXTRACT(MONTH FROM message_ts)) AS period_num,
            CAST(message_ts AS DATE) AS start_date,
            message_ts AS start_ts,
            conversation_id,
            channel_name
        FROM source_chatbot_messages
        WHERE conversation_id IS NOT NULL
          AND CAST(message_ts AS DATE) >= p_start_date
          AND CAST(message_ts AS DATE) <  p_end_date
        QUALIFY ROW_NUMBER() OVER (PARTITION BY conversation_id ORDER BY message_ts ASC) = 1
    ) WITH DATA
    PRIMARY INDEX (conversation_id)
    ON COMMIT PRESERVE ROWS;

    CALL util_log_step(v_process_name, v_start_ts, 'INF', 'Universe created');

    -- 2) Internal flags derived from messages (examples)
    CALL util_drop_table_if_exists('tmp_chatbot_flag_handover');
    CREATE VOLATILE TABLE tmp_chatbot_flag_handover AS
    (
        SELECT a.conversation_id
        FROM tmp_chatbot_universe a
        JOIN source_chatbot_messages b
          ON a.conversation_id = b.conversation_id
        WHERE b.partition_tag >= 'YYYY-MM'
          AND LOWER(TRIM(b.action_text)) = 'handover'
        GROUP BY 1
    ) WITH DATA
    PRIMARY INDEX (conversation_id)
    ON COMMIT PRESERVE ROWS;

    -- 3) Recover user identifiers from different sources
    -- 3.1: direct mapping (if exists in origin)
    CALL util_drop_table_if_exists('tmp_chatbot_user_origin');
    CREATE VOLATILE TABLE tmp_chatbot_user_origin AS
    (
        SELECT
            a.conversation_id,
            MAX(TO_NUMBER(b.user_id)) AS user_id
        FROM tmp_chatbot_universe a
        JOIN source_chatbot_messages b
          ON a.conversation_id = b.conversation_id
        WHERE b.partition_tag >= 'YYYY-MM'
        GROUP BY 1
    ) WITH DATA
    PRIMARY INDEX (conversation_id)
    ON COMMIT PRESERVE ROWS;

    DELETE FROM tmp_chatbot_user_origin WHERE user_id IS NULL;

    -- 3.2: recover document number (doc_number) from messages or routing info
    CALL util_drop_table_if_exists('tmp_chatbot_doc_number');
    CREATE VOLATILE TABLE tmp_chatbot_doc_number AS
    (
        SELECT
            a.conversation_id AS conversation_id,
            MAX(TRIM(b.doc_number_text)) AS doc_number
        FROM tmp_chatbot_universe a
        JOIN source_chatbot_messages b
          ON a.conversation_id = b.conversation_id
        WHERE b.partition_tag >= 'YYYY-MM'
        GROUP BY 1
    ) WITH DATA
    PRIMARY INDEX (conversation_id)
    ON COMMIT PRESERVE ROWS;

    DELETE FROM tmp_chatbot_doc_number WHERE doc_number = '';

    -- 3.3: recover phone number mapping
    CALL util_drop_table_if_exists('tmp_chatbot_phone_number');
    CREATE VOLATILE TABLE tmp_chatbot_phone_number AS
    (
        SELECT
            a.conversation_id AS conversation_id,
            MAX(TRIM(b.phone_number_text)) AS phone_number
        FROM tmp_chatbot_universe a
        JOIN source_chatbot_messages b
          ON a.conversation_id = b.conversation_id
        WHERE b.partition_tag >= 'YYYY-MM'
        GROUP BY 1
    ) WITH DATA
    PRIMARY INDEX (conversation_id)
    ON COMMIT PRESERVE ROWS;

    DELETE FROM tmp_chatbot_phone_number WHERE phone_number = '';

    -- 3.4: consolidate user id from origin, doc_number and phone_number
    CALL util_drop_table_if_exists('tmp_chatbot_user');
    CREATE VOLATILE TABLE tmp_chatbot_user AS
    (
        SELECT conversation_id, user_id, 'ORIGIN' AS user_origin FROM tmp_chatbot_user_origin
        UNION ALL
        SELECT conversation_id, CAST(NULL AS BIGINT) AS user_id, 'DOC' AS user_origin FROM tmp_chatbot_doc_number
        UNION ALL
        SELECT conversation_id, CAST(NULL AS BIGINT) AS user_id, 'PHONE' AS user_origin FROM tmp_chatbot_phone_number
    ) WITH DATA
    PRIMARY INDEX (conversation_id)
    ON COMMIT PRESERVE ROWS;

    -- 4) Message processing: clean and classify text (example cleaning steps)
    CALL util_drop_table_if_exists('tmp_messages_raw');
    CREATE VOLATILE TABLE tmp_messages_raw AS
    (
        SELECT
            b.interaction_num,
            b.conversation_id,
            b.message_text,
            CAST(NULL AS VARCHAR(1000)) AS message_clean_text,
            CAST(NULL AS VARCHAR(100)) AS message_type_predicted
        FROM tmp_chatbot_universe a
        JOIN source_chatbot_messages b
          ON a.conversation_id = b.conversation_id
        WHERE b.actor IN ('USER', 'USUARIO')
    ) WITH DATA
    PRIMARY INDEX (conversation_id)
    ON COMMIT PRESERVE ROWS;

    -- Example: basic cleaning (non-language-specific regex calls replaced by comments)
    -- UPDATE tmp_messages_raw
    -- SET message_clean_text = REGEXP_REPLACE(message_text, '[^[:alnum:][:space:]]', ' ');

    -- 5) Create final summarized table for publishing
    CALL util_drop_table_if_exists('tmp_chatbot_conversations_summary');
    CREATE VOLATILE TABLE tmp_chatbot_conversations_summary
    (
        year_num              INTEGER,
        month_num             INTEGER,
        period_num            INTEGER,
        period_text           VARCHAR(10),
        start_date            DATE,
        start_ts              TIMESTAMP,
        conversation_id       VARCHAR(1000),
        case_id               BIGINT,
        user_id               DECIMAL(18,0),
        user_origin           VARCHAR(100),
        user_validated_flag   BYTEINT,
        channel_name          VARCHAR(100),
        destination_queue     VARCHAR(100),
        message_type_group    VARCHAR(100),
        message_type          VARCHAR(100),
        message_sample        VARCHAR(1000),
        message_flag          BYTEINT,
        handover_flag         BYTEINT,
        resolution_flag       BYTEINT,
        derivation_flag      BYTEINT,
        label_1               VARCHAR(100),
        label_2               VARCHAR(100),
        label_3               VARCHAR(100)
    ) WITH DATA
    PRIMARY INDEX (conversation_id)
    ON COMMIT PRESERVE ROWS;

    -- Example insertion into final table (aggregated / anonymized)
    INSERT INTO tmp_chatbot_conversations_summary
    (
        year_num, month_num, period_num, period_text, start_date, start_ts,
        conversation_id, user_id, user_origin, channel_name
    )
    SELECT
        year_num, month_num, period_num, CAST(period_num AS VARCHAR(10)), start_date, start_ts,
        conversation_id, NULL AS user_id, 'ANON' AS user_origin, channel_name
    FROM tmp_chatbot_universe;

    CALL util_log_step(v_process_name, v_start_ts, 'INF', 'Inserted summary (anonymized)');

    -- Finalize
    CALL util_log_step(v_process_name, v_start_ts, 'INF', 'End stored procedure');
END;

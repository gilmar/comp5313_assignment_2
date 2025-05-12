COPY (
    WITH issue_pr_data AS (
        SELECT
              JSON_EXTRACT(repository,'$.name') AS repository,
              type AS event_type,
              actor AS actor_id,
              created_at AS event_date,
              JSON_EXTRACT(payload, '$.action') AS action,
              CAST(JSON_EXTRACT(payload, '$.pull_request.number') AS VARCHAR) AS pr_id,
              JSON_EXTRACT(payload, '$.issue') AS issue_id
        FROM
            read_json_auto('{{INPUT_FILE}}', format='newline_delimited', maximum_object_size=268435456, ignore_errors=TRUE)
        WHERE
            JSON_EXTRACT(repository,'$.name') IN (
                '"kubernetes"',
                '"prometheus"',
                '"envoy"',
                '"harbor"',
                '"home-assistant"',
                '"godot"'
            )
    ),

    activity AS (
        SELECT
            repository,
            event_date,
            action,
            event_type,
            actor_id,
            CASE
                WHEN pr_id IS NOT NULL THEN CONCAT('pr_', pr_id)
                WHEN issue_id IS NOT NULL THEN CONCAT('issue_', issue_id)
                ELSE NULL
            END AS object_id
        FROM issue_pr_data
    )

    SELECT *
    FROM activity
    WHERE object_id IS NOT NULL
    ORDER BY event_date
)
TO '{{OUTPUT_FILE}}' WITH (HEADER FALSE, DELIMITER ',');

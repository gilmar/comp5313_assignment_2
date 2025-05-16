COPY (
    WITH issue_pr_data AS (
        SELECT
            repo.name AS repository,
            type AS event_type,
            actor.id AS actor_id,
            created_at AS event_date,
            JSON_EXTRACT(payload, '$.action') AS action,
            JSON_EXTRACT(payload, '$.pull_request.number') AS pr_id,
            JSON_EXTRACT(payload, '$.issue.id') AS issue_id
        FROM
            read_json_auto('{{INPUT_FILE}}', format='newline_delimited', maximum_object_size=268435456)
        WHERE
            repo.name IN (
                'kubernetes/kubernetes',
                'prometheus/prometheus',
                'envoyproxy/envoy',
                'goharbor/harbor',
                'home-assistant/core',
                'godotengine/godot'
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

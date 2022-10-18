{{ config(
    materialized='incremental',
    format='ICEBERG',
    tags=['dbt_test_athena', 'iceberg'],
    incremental_strategy='merge',
    unique_key='primary_key'
) }}

WITH stage_data as (
    SELECT
        'key_001' as primary_key,
        '{{ var("name") }}' as name,
        '{{ var("email") }}' as email

    {% if var('include_dave') == 'true' %}
    UNION ALL

    SELECT
        'key_002' as primary_key,
        'Dave Patterson' as name,
        'david.patterson@example.com' as email
    {% endif %}
)
SELECT
    stage_data.primary_key,
    stage_data.name,
    stage_data.email
FROM stage_data

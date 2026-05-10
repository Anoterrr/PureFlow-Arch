{% macro create_external_sources() %}
    {% do log("DEBUG: MACRO STARTING", info=True) %}
    {% if execute %}
        {% do log("DEBUG: SOURCES FOUND: " ~ graph.sources.values() | length, info=True) %}
        {# 1. Ensure S3 credentials are set in this session #}
        {% set s3_endpoint = env_var('S3_ENDPOINT', 'http://minio:9000').replace('http://', '').replace('https://', '') %}
        {% set s3_user = env_var('STORAGE_USER', 'admin') %}
        {% set s3_pass = env_var('STORAGE_PASSWORD', 'strongpassword123') %}

        {% set setup_sql %}
            INSTALL httpfs; LOAD httpfs;
            INSTALL json; LOAD json;
            INSTALL delta; LOAD delta;
            SET s3_endpoint = '{{ s3_endpoint }}';
            SET s3_access_key_id = '{{ s3_user }}';
            SET s3_secret_access_key = '{{ s3_pass }}';
            SET s3_use_ssl = false;
            SET s3_url_style = 'path';
        {% endset %}
        {% do run_query(setup_sql) %}

        {# 2. Register Sources as Views #}
        {% for source in graph.sources.values() %}
            {% if source.external_location %}
                {% set schema_name = source.schema %}
                {% set table_name = source.name %}
                {% do log("DEBUG: PROCESSING SOURCE " ~ schema_name ~ "." ~ table_name, info=True) %}
                
                {# Render the location to resolve variables #}
                {% set loc = render(source.external_location) %}
                
                {% set is_delta = 'dt=' in loc or 'delta' in loc %}
                {% set fmt = 'delta' if is_delta else ('csv' if 'csv' in loc else ('json' if 'json' in loc else 'parquet')) %}
                {% set read_func = "delta_scan" if fmt == 'delta' else ("read_csv_auto" if fmt == 'csv' else ("read_json_auto" if fmt == 'json' else "read_parquet")) %}

                {% do log("DEBUG: REGISTERING " ~ table_name ~ " AS " ~ fmt, info=True) %}

                {% set sql %}
                    CREATE SCHEMA IF NOT EXISTS {{ schema_name }};
                    CREATE OR REPLACE VIEW {{ schema_name }}.{{ table_name }} AS 
                    SELECT * FROM {{ read_func }}('{{ loc }}');
                {% endset %}
                
                {% do log("REGISTERING SOURCE: " ~ schema_name ~ "." ~ table_name ~ " FROM " ~ loc, info=True) %}
                {% do run_query(sql) %}
            {% endif %}
        {% endfor %}
    {% endif %}
{% endmacro %}

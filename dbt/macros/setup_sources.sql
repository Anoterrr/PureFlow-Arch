{% macro create_external_sources() %}
    {% if execute %}
        {% do log("🚀 [dbt] Registering external sources...", info=True) %}
        
        {# 1. Ensure S3 credentials and extensions are set GLOBALly in this session #}
        {% set s3_endpoint = env_var('S3_ENDPOINT', 'minio:9000').replace('http://', '').replace('https://', '') %}
        {% set s3_user = env_var('STORAGE_USER', 'admin') %}
        {% set s3_pass = env_var('STORAGE_PASSWORD', 'strongpassword123') %}

        {% set setup_sql %}
            INSTALL httpfs; LOAD httpfs;
            INSTALL json; LOAD json;
            INSTALL delta; LOAD delta;
            
            -- Apply GLOBALly to ensure all connection pool members see these settings
            SET GLOBAL s3_endpoint = '{{ s3_endpoint }}';
            {% if s3_user %}
            SET GLOBAL s3_access_key_id = '{{ s3_user }}';
            SET GLOBAL s3_secret_access_key = '{{ s3_pass }}';
            {% endif %}
            SET GLOBAL s3_use_ssl = false;
            SET GLOBAL s3_url_style = 'path';
        {% endset %}
        
        {% do run_query(setup_sql) %}

        {# 2. Register Sources as Views #}
        {% for source in graph.sources.values() %}
            {% set ext_loc = None %}
            {% if source.meta and source.meta.get('external_location') %}
                {% set ext_loc = source.meta.get('external_location') %}
            {% endif %}
            
            {% if ext_loc %}
                {% set schema_name = source.schema %}
                {% set table_name = source.name %}
                
                {# Simple view registration #}
                {% set loc = ext_loc | replace("{{ var('execution_date') }}", var('execution_date')) %}
                {% set is_delta = 'dt=' in loc or 'delta' in loc %}
                {% set read_func = "delta_scan" if is_delta else "read_parquet" %}

                {% set registration_sql %}
                    CREATE SCHEMA IF NOT EXISTS {{ schema_name }};
                    CREATE OR REPLACE VIEW {{ schema_name }}.{{ table_name }} AS 
                    SELECT * FROM {{ read_func }}('{{ loc }}');
                {% endset %}
                
                {% do log("📦 [dbt] Registering source: " ~ schema_name ~ "." ~ table_name, info=True) %}
                {% do run_query(registration_sql) %}
            {% endif %}
        {% endfor %}
    {% endif %}
{% endmacro %}

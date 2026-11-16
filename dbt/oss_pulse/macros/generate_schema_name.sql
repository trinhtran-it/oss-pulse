{#
  dev / prod: write to the Terraform-managed dataset named by the model's +schema
              (oss_pulse_staging, oss_pulse_marts), unprefixed.
  any other target (e.g. ci): write everything to the target's own dataset so
              CI runs can never touch dev/prod tables.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if target.name in ['dev', 'prod'] and custom_schema_name is not none -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ target.schema }}
    {%- endif -%}
{%- endmacro %}

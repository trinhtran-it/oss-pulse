{#
  Where each model is built:
    prod       -> the dataset named by the model's +schema (oss_pulse_staging, oss_pulse_marts),
                  used as-is (no "<target>_" prefix). Those datasets are created by Terraform.
    dev / ci   -> everything goes to the target's own dataset (e.g. dbt_<yourname>, oss_pulse_ci),
                  so development and CI runs can never overwrite production tables.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if target.name == 'prod' and custom_schema_name is not none -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ target.schema }}
    {%- endif -%}
{%- endmacro %}

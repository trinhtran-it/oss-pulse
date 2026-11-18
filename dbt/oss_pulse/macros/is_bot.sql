{#
  SQL boolean expression: TRUE when the login belongs to a bot.
    - GitHub Apps always end in "[bot]" (dependabot[bot], github-actions[bot], ...).
    - Service accounts that look like users are listed in seeds/known_bots.csv (e.g. copilot).
  Case-insensitive; NULL logins count as not-bot. Deliberately no "ends with bot" pattern:
  it would also flag real users (e.g. "abbot").

  Usage: select {{ is_bot('actor_login') }} as is_bot
#}
{% macro is_bot(login_column) -%}
    coalesce(
        ends_with(lower({{ login_column }}), '[bot]')
        or lower({{ login_column }}) in (select lower(login) from {{ ref('known_bots') }}),
        false
    )
{%- endmacro %}

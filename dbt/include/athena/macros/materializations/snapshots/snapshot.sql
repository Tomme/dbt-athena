{% materialization snapshot, adapter='athena' -%}
  {{ exceptions.raise_not_implemented(
    'snapshot materialization not implemented for '+adapter.type())
  }}
{% endmaterialization %}

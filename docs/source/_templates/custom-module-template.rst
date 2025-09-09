{{ fullname | escape | underline}}

.. automodule:: {{ fullname }}

   {% block attributes %}
   {% if attributes %}
   .. rubric:: Module attributes

   .. autosummary::
      :toctree:
   {% for item in attributes %}
      {{ item }}
   {%- endfor %}
   {% endif %}
   {% endblock %}

   {% block functions %}
   {% if functions %}
   .. rubric:: {{ _('Functions') }}

   .. autosummary::
      :toctree:
      :nosignatures:
   {% for item in functions %}
      {{ item }}
   {%- endfor %}
   {% endif %}
   {% endblock %}

   {% block classes %}
   {% if classes %}
   .. rubric:: {{ _('Classes') }}

   .. autosummary::
      :toctree:
      :template: custom-class-template.rst
      :nosignatures:
   {% for item in classes %}
      {{ item }}
   {%- endfor %}
   {% endif %}
   {% endblock %}

   {# Hacky solution to try to filter everything that isn't a pydantic model #}
   {# Necessary because of Sphinx issue #6364, but will occasionally catch false positives #}
   {# Will only catch capitalised classes ! #}
   {# See the warning at https://autodoc-pydantic.readthedocs.io/en/stable/users/usage.html#autosummary #}

   {% set pydantic_models = members | reject('in', classes) | reject('in', attributes) | reject('in', exceptions) | reject('in', functions) | reject('eq', "TYPE_CHECKING") | select('lt', "_") | list -%}
   {% block pydantic %}
   {% if pydantic_models %}
   .. rubric:: {{ _('Pydantic models') }}

   .. autosummary::
      :toctree:
      :template: custom-pydantic-template.rst
      :nosignatures:
   {% for item in pydantic_models %}
      {{ item }}
   {%- endfor %}
   {% endif %}
   {% endblock %}

   {% block exceptions %}
   {% if exceptions %}
   .. rubric:: {{ _('Exceptions') }}

   .. autosummary::
      :toctree:
   {% for item in exceptions %}
      {{ item }}
   {%- endfor %}
   {% endif %}
   {% endblock %}

{% block modules %}
{% if modules %}
.. rubric:: {{ _('Modules') }}

.. autosummary::
   :toctree:
   :template: custom-module-template.rst
   :recursive:
{% for item in modules %}
   {{ item }}
{%- endfor %}
{% endif %}
{% endblock %}
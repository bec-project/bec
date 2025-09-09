{{ fullname | escape | underline}}

.. currentmodule:: {{ module }}
   

{% if objtype == "pydantic_model" %}
.. autopydantic_model:: {{ objname }}
   :model-show-validator-summary:

{% endif %}

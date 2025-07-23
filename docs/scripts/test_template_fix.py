#!/usr/bin/env python3
"""
Test Jinja2 template formatting fix directly
"""
from jinja2 import Template

# Test the old problematic template pattern
old_template = '''
            # Read source data
            {%- for source in sources %}
            {{ source.name }}_df = self._read_{{ source.name }}()
            {%- endfor %}
            
            # Apply transformations
            current_df = {{ sources[0].name }}_df
            {%- for transformation in transformations %}
            current_df = self._apply_{{ transformation.name }}(current_df)
            {%- endfor %}
'''

# Test the new fixed template pattern  
new_template = '''
            # Read source data
            {% for source in sources -%}
            {{ source.name }}_df = self._read_{{ source.name }}()
            {% endfor %}
            
            # Apply transformations
            current_df = {{ sources[0].name }}_df
            {% for transformation in transformations -%}
            current_df = self._apply_{{ transformation.name }}(current_df)
            {% endfor %}
'''

# Test data
sources = [{'name': 'src_customer_file'}, {'name': 'src_orders'}]
transformations = [{'name': 'exp_standardize_names'}, {'name': 'lkp_customer_lookup'}]

print("=== OLD TEMPLATE OUTPUT (with alignment issues) ===")
old_result = Template(old_template).render(sources=sources, transformations=transformations)
print(repr(old_result))
print("\nFormatted:\n" + old_result)

print("\n=== NEW TEMPLATE OUTPUT (fixed alignment) ===")
new_result = Template(new_template).render(sources=sources, transformations=transformations)
print(repr(new_result))
print("\nFormatted:\n" + new_result)

print("\n=== COMPARISON ===")
print("Old template issues:")
lines = old_result.split('\n')
for i, line in enumerate(lines, 1):
    if line.strip() and len(line) > 80:
        print(f"Line {i}: {repr(line)} - TOO LONG/CRAMPED")
        
print("\nNew template validation:")
lines = new_result.split('\n')
issues = 0
for i, line in enumerate(lines, 1):
    if line.strip() and len(line) > 80:
        print(f"Line {i}: {repr(line)} - TOO LONG/CRAMPED")
        issues += 1
        
if issues == 0:
    print("✅ NEW TEMPLATE: No formatting issues detected!")
else:
    print(f"❌ NEW TEMPLATE: {issues} issues found")
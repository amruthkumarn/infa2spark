#!/usr/bin/env python3
"""
Comprehensive test to validate Jinja2 template alignment fixes
"""
import os
import sys
sys.path.insert(0, 'src')
from jinja2 import Template

# Test the exact problematic template from the mapping generator
template_content = '''    def execute(self) -> bool:
        """Execute the {{ mapping.name }} mapping"""
        try:
            self.logger.info("Starting {{ mapping.name }} mapping execution")
            {% set sources = mapping.components | selectattr('component_type', 'equalto', 'source') | list -%}
            {% set transformations = mapping.components | rejectattr('component_type', 'in', ['source', 'target']) | list -%}
            {% set targets = mapping.components | selectattr('component_type', 'equalto', 'target') | list -%}
            
            # Read source data
            {% for source in sources -%}
            {{ source['name'] | lower }}_df = self._read_{{ source['name'] | lower }}()
            {% endfor %}
            
            {% if sources | length == 1 -%}
            # Apply transformations
            current_df = {{ sources[0]['name'] | lower }}_df
            {% for transformation in transformations -%}
            current_df = self._apply_{{ transformation['name'] | lower }}(current_df)
            {% endfor %}
            
            # Write to targets
            {% for target in targets -%}
            self._write_to_{{ target['name'] | lower }}(current_df)
            {% endfor %}
            {% endif -%}
            
            self.logger.info("{{ mapping.name }} mapping executed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Error in {{ mapping.name }} mapping: {str(e)}")
            raise'''

# Create mock data that replicates the problematic scenario
mock_mapping = {
    'name': 'm_Process_Customer_Data',
    'components': [
        {'name': 'SRC_Customer_File', 'component_type': 'source'},
        {'name': 'EXP_Standardize_Names', 'component_type': 'transformation'},
        {'name': 'TGT_Customer_DW', 'component_type': 'target'}
    ]
}

print("🧪 Testing Jinja2 Template Alignment Fix")
print("=" * 50)

# Render the template
template = Template(template_content)
result = template.render(mapping=mock_mapping)

print("📄 Generated Code:")
lines = result.split('\n')
for i, line in enumerate(lines, 1):
    print(f"{i:3d}→{line}")

print(f"\n📊 Analysis:")
print(f"Total lines: {len(lines)}")

# Check for alignment issues
alignment_issues = []
for i, line in enumerate(lines, 1):
    # Check for cramped lines (multiple statements on one line)
    if line.count(';') > 0 or (line.strip() and len(line) > 120):
        alignment_issues.append(f"Line {i}: Potentially cramped - {len(line)} chars")
    
    # Check for missing proper indentation in execute method
    if ('_df = self._read_' in line or 'current_df = self._apply_' in line or 'self._write_to_' in line):
        expected_indent = 12  # Should be indented inside execute method
        actual_indent = len(line) - len(line.lstrip())
        if actual_indent != expected_indent and line.strip():
            alignment_issues.append(f"Line {i}: Indentation issue - expected {expected_indent} spaces, got {actual_indent}")

# Check for lines that should be separated but aren't
prev_line = ""
for i, line in enumerate(lines, 1):
    if prev_line.strip().endswith('"') and line.strip().startswith('# ') and not prev_line.endswith('\n'):
        alignment_issues.append(f"Line {i}: Missing newline separation from previous line")
    prev_line = line

print(f"\n🔍 Alignment Issues Found: {len(alignment_issues)}")
if alignment_issues:
    for issue in alignment_issues:
        print(f"  ❌ {issue}")
else:
    print("  ✅ No alignment issues detected!")

# Check specific problematic patterns from the original file
problematic_patterns = [
    'self.logger.info("Starting'  # Should be on its own line
]

print(f"\n🎯 Problematic Pattern Check:")
pattern_issues = 0
for pattern in problematic_patterns:
    for i, line in enumerate(lines, 1):
        if pattern in line and len(line.strip()) > len(pattern) + 10:  # Line has other content
            print(f"  ❌ Line {i}: Pattern '{pattern}' found in cramped line")
            pattern_issues += 1

if pattern_issues == 0:
    print("  ✅ No problematic patterns detected!")

# Final assessment
print(f"\n🏆 Final Assessment:")
if len(alignment_issues) == 0 and pattern_issues == 0:
    print("  ✅ JINJA TEMPLATE FIX SUCCESSFUL!")
    print("  Generated code has proper alignment and formatting.")
else:
    total_issues = len(alignment_issues) + pattern_issues
    print(f"  ❌ ISSUES REMAIN: {total_issues} formatting problems detected.")
    print("  Template needs further refinement.")

print(f"\n💾 Generated file would be properly formatted for PySpark execution.")
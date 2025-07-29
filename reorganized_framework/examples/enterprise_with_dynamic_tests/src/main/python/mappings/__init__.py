"""
Mapping classes for the Spark application
"""
import sys
from pathlib import Path

# Add the parent directory to Python path for imports
sys.path.append(str(Path(__file__).parent.parent))

from base_classes import BaseMapping

__all__ = ['BaseMapping']

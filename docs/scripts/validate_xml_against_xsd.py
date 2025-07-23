import sys
import os
from lxml import etree

class LocalXSDResolver(etree.Resolver):
    """Custom resolver to find XSDs in the local directory."""
    def __init__(self, base_path):
        self.base_path = base_path

    def resolve(self, system_url, public_id, context):
        # The system_url is often the filename of the imported schema
        xsd_file_path = os.path.join(self.base_path, os.path.basename(system_url))
        if os.path.exists(xsd_file_path):
            print(f"    --> Resolver found schema: {xsd_file_path}")
            return self.resolve_filename(xsd_file_path, context)
        else:
            print(f"    --> Resolver could NOT find schema: {system_url}")
            return None

def validate_xml_with_resolver(xml_path, xsd_path, xsd_dir):
    """
    Validates an XML file against an XSD schema using a local resolver.
    """
    try:
        # Set up a parser with the custom resolver
        parser = etree.XMLParser(resolve_entities=True)
        parser.resolvers.add(LocalXSDResolver(xsd_dir))
        
        # Load the XSD schema with the custom parser
        print(f"🔍 Loading XSD schema from: {xsd_path} with local resolver...")
        with open(xsd_path, 'rb') as f:
            schema_root = etree.fromstring(f.read(), parser=parser)
        schema = etree.XMLSchema(schema_root)
        print(f"✅ Successfully loaded and parsed XSD schema.")

        # Load the XML file
        with open(xml_path, 'rb') as f:
            xml_doc = etree.fromstring(f.read())
        print(f"✅ Successfully loaded XML file from: {xml_path}")
        
        # Validate the XML against the schema
        print("\n🔍 Attempting validation...")
        schema.assertValid(xml_doc)
        
        print("\n🎉 VALIDATION SUCCESSFUL: The XML file conforms to the XSD schema.")
        return True

    except etree.XMLSchemaParseError as e:
        print(f"❌ XSD PARSE ERROR: The XSD schema itself is invalid or has unresolved imports.")
        print(f"   Error: {e}")
        return False
    except etree.DocumentInvalid as e:
        print(f"❌ VALIDATION FAILED: The XML file does not conform to the XSD schema.")
        print(f"   Validation Errors:")
        for error in e.error_log:
            print(f"     - Line {error.line}, Column {error.column}: {error.message}")
        return False
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: An unexpected error occurred during validation.")
        print(f"   Error: {e}")
        return False

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python validate_xml_against_xsd.py <path_to_xml> <path_to_xsd_dir>")
        sys.exit(1)

    xml_file = sys.argv[1]
    xsd_directory = sys.argv[2]
    # We still start with IMX.xsd, but now the resolver can find its imports
    root_xsd_file = os.path.join(xsd_directory, "IMX.xsd")
    
    if not os.path.exists(root_xsd_file):
        print(f"Error: Root XSD file not found at {root_xsd_file}")
        sys.exit(1)

    validate_xml_with_resolver(xml_file, root_xsd_file, xsd_directory) 
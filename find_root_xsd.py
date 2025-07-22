import sys
import os
from lxml import etree

def find_validating_schema(xml_path, xsd_dir):
    """
    Iterates through all XSD files in a directory and attempts to validate an XML file.
    """
    xml_files_found = []
    
    print(f"🔍 Searching for a validating schema for '{xml_path}' in directory '{xsd_dir}'...")
    
    # Iterate through all files in the directory
    for filename in os.listdir(xsd_dir):
        if filename.endswith(".xsd"):
            xsd_path = os.path.join(xsd_dir, filename)
            print(f"\n--- Attempting validation with schema: {filename} ---")
            
            try:
                # Use a resolver to handle imports
                parser = etree.XMLParser(resolve_entities=True)
                parser.resolvers.add(LocalXSDResolver(xsd_dir))
                
                # Load the XSD schema
                with open(xsd_path, 'rb') as f:
                    schema_root = etree.fromstring(f.read(), parser=parser)
                schema = etree.XMLSchema(schema_root)

                # Load the XML file
                with open(xml_path, 'rb') as f:
                    xml_doc = etree.fromstring(f.read())
                
                # Attempt to validate
                schema.assertValid(xml_doc)
                
                # If we get here, it's a success!
                print(f"\n🎉🎉🎉 SUCCESS! Found a validating schema: {filename}")
                xml_files_found.append(filename)

            except etree.DocumentInvalid as e:
                # This is an expected failure for most schemas
                print(f"   -> FAILED (as expected): XML is not valid against this schema.")
                # Log the first error for context
                if e.error_log:
                    print(f"      Reason: {e.error_log[0].message}")
            except Exception as e:
                # This indicates a problem with the schema file itself
                print(f"   -> ERROR: Could not process schema {filename}: {e}")

    if xml_files_found:
        print(f"\n✅ Found {len(xml_files_found)} validating schema(s): {', '.join(xml_files_found)}")
        return xml_files_found
    else:
        print(f"\n❌ No single schema file in the directory could validate the XML.")
        return None

class LocalXSDResolver(etree.Resolver):
    """Custom resolver to find XSDs in the local directory."""
    def __init__(self, base_path):
        self.base_path = base_path

    def resolve(self, system_url, public_id, context):
        xsd_file_path = os.path.join(self.base_path, os.path.basename(system_url))
        if os.path.exists(xsd_file_path):
            return self.resolve_filename(xsd_file_path, context)
        return None

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python find_root_xsd.py <path_to_xml> <path_to_xsd_dir>")
        sys.exit(1)

    xml_file = sys.argv[1]
    xsd_directory = sys.argv[2]
    
    find_validating_schema(xml_file, xsd_directory) 
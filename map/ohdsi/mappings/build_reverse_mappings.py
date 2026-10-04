import json

"""
This script creates a reverse mapping from the OID to the vocabulary id JSON file 'oid_to_vocabulary_id.json'
and maps it to a new JSON file 'vocabulary_to_oid.json'
"""

def build_reverse_mappings(input_json_file_name, output_json_file_name):

    with open(input_json_file_name, "r") as f:
        mapping_dict = json.load(f)

    reverse_mapping_dict = {mapping_dict[k]: k for k in mapping_dict}

    with open(output_json_file_name, "w") as fw:
        json.dump(reverse_mapping_dict, fw, indent=2)


if __name__ == "__main__":
    build_reverse_mappings("oid_to_vocabulary_id.json", "vocabulary_to_oid.json")
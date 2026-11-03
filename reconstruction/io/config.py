"""YAML config files."""
import os
import yaml


def load_yaml(filepath):
    with open(filepath, 'r') as file:
        return yaml.safe_load(file)


def save_to_yaml(data):
    filename = os.path.join(data['output_dir'], 'config.yaml')
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, 'w') as yaml_file:
        yaml.dump(data, yaml_file, default_flow_style=False)

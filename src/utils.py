import json
from typing import Any

def _write_files(path: str, filename: str, obj: Any):
    with open(f'{path}/{filename}', 'w') as f:
        f.write(json.dumps(obj, indent=4, ensure_ascii=False))

def _update_path_url_json(path_data: str, name_property_url: str, i: int, path_url: str):
    with open(path_data, 'r') as f:
        data = json.load(f)

    data[i].update({
        name_property_url: path_url
        })

    with open(path_data, 'w') as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

def format_duration(duration):
    hours, remainder = divmod(duration.seconds, 3600)
    minutes, _ = divmod(remainder, 60)
    return f'{hours} horas con {minutes} minutos' if hours > 0 else f'{minutes} minutos'
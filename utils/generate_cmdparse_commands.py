#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
import json
import os
import sys


def load_commands(commands_dir):
    commands = []
    for filename in sorted(os.listdir(commands_dir)):
        if not filename.endswith('.json'):
            continue
        filepath = os.path.join(commands_dir, filename)
        # Python 2/3 compatibility for file encoding
        if sys.version_info[0] >= 3:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
        else:
            with open(filepath, 'r') as f:
                data = json.load(f)
        commands.append(data)
    return commands


def generate_command_def(output_path, commands):
    lines = [
        "/* ================================================================",
        " * utils/generate_cmdparse_commands.py auto build , don't modify! ",
        " * ================================================================ */",
        "",
        "static cmdParseCommandDef cmd_parse_commands[] = {",
    ]

    for cmd in commands:
        # Use string formatting instead of f-string for Python 2 compatibility
        lines.append('    {{ "{0}", {1} }},'.format(cmd["name"], cmd["parse"]))

    lines.extend([
        "    {NULL, NULL}",
        "};",
        "",
    ])

    content = "\n".join(lines) + "\n"

    # Python 2/3 compatibility for file encoding
    if sys.version_info[0] >= 3:
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(content)
    else:
        with open(output_path, "w") as f:
            f.write(content)

    print("Generated {0} with {1} commands.".format(output_path, len(commands)))


if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    commands_dir = os.path.join(script_dir, "..", "xredis", "commands")
    output = os.path.join(script_dir, "..", "xredis", "xredis_commands.def")

    commands = load_commands(commands_dir)
    generate_command_def(output, commands)

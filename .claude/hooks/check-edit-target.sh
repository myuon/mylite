#!/bin/bash
# PreToolUse hook: block Edit/Write on protected files
# stdin: JSON with tool_input.file_path

file_path=$(jq -r '.tool_input.file_path // .tool_input.filePath // ""')

if echo "$file_path" | grep -q 'skiplist.json'; then
  echo '{"decision":"block","reason":"skiplist.jsonを直接編集しないでください。python3 scripts/skiplist.py を使用してください。"}'
elif echo "$file_path" | grep -q 'testdata/dolt-mysql-tests'; then
  echo '{"decision":"block","reason":"testdata/dolt-mysql-tests はサブモジュールです。直接編集しないでください。"}'
else
  echo '{}'
fi

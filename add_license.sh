#!/bin/bash

for i in $(find ./ \( -name '*.py' -o -name '*.rs' \) \
  -not -path './.venv/*' \
  -not -path '*/site-packages/*' \
  -not -path '*/.venv/*' \
  -not -path './env/*' \
  -not -path '*/target/*' \
  -not -path '*/.cargo/*'); do
  if ! grep -qi 'GNU AFFERO' "$i"; then
    case "$i" in
      *.rs) cat LICENSE_HEADER_RS "$i" > "$i.new" && mv "$i.new" "$i" ;;
      *)    cat LICENSE_HEADER "$i" > "$i.new" && mv "$i.new" "$i" ;;
    esac
  fi
done

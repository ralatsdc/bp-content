rsync -rpt --include '*/' --include '*.json' --exclude '*' --prune-empty-dirs \
      author/ bp-static:~/nginx_content/crisis-countries/json/source

rsync -rpt --include '*/' --include '*.json' --exclude '*' --prune-empty-dirs \
      collection/crisis/ bp-static:~/nginx_content/crisis-countries/json/collection

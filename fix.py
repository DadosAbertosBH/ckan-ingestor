p = "ckan_ingestor/ckan_dataset_fetcher.py"
c = open(p).read()
old = "nulls (empty list had no real data)"
idx = c.find(old)
if idx < 0:
    print("NOT FOUND")
    # print context
    for line in c.split("\n"):
        if "nulls" in line:
            print(repr(line))
else:
    print("FOUND at", idx)

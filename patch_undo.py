path = "ingestor_orchestrator/backend/ingestor_orchestrator/services/metadata_sync.py"
with open(path) as f:
    content = f.read()

old = """                )
                # Mark resource as synced to avoid re-enqueueing
                conn.execute(
                    "INSERT OR REPLACE INTO ckan_resource_last_update "
                    "(ckan_resource_id, last_modified) VALUES (?, CURRENT_TIMESTAMP)",
                    (resource_id,),
                )
                enqueued += 1"""

new = """                )
                enqueued += 1"""

content = content.replace(old, new)
with open(path, "w") as f:
    f.write(content)
print("Done")

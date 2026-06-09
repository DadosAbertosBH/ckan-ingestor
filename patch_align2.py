path = "ckan_ingestor/ckan_dataset_fetcher.py"
with open(path) as f:
    c = f.read()

old = """            elif table.schema.field(field.name).type != field.type:
                # Cast mismatched types to string to allow concatenation
                idx = table.schema.get_field_index(field.name)
                try:
                    table = table.set_column(
                        idx, field.name, table.column(field.name).cast(field.type)
                    )
                except (
                    pyarrow.ArrowInvalid,
                    pyarrow.ArrowTypeError,
                    pyarrow.ArrowNotImplementedError,
                ):
                    table = table.set_column(
                        idx, field.name,
                        pyarrow.nulls(table.num_rows, field.type),
                    )"""

new = """            elif table.schema.field(field.name).type != field.type:
                idx = table.schema.get_field_index(field.name)
                col = table.column(field.name)
                try:
                    table = table.set_column(idx, field.name, col.cast(field.type))
                except (
                    pyarrow.ArrowInvalid,
                    pyarrow.ArrowTypeError,
                    pyarrow.ArrowNotImplementedError,
                ):
                    try:
                        table = table.set_column(idx, field.name, col.cast(pyarrow.string()))
                    except (
                        pyarrow.ArrowInvalid,
                        pyarrow.ArrowTypeError,
                        pyarrow.ArrowNotImplementedError,
                    ):
                        table = table.set_column(
                            idx, field.name,
                            pyarrow.nulls(table.num_rows, field.type),
                        )"""

c = c.replace(old, new)
with open(path, "w") as f:
    f.write(c)
print("Done")

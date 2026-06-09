path = "ckan_ingestor/ckan_dataset_fetcher.py"
with open(path) as f:
    c = f.read()

old = """            elif table.schema.field(field.name).type != field.type:
                idx = table.schema.get_field_index(field.name)
                col = table.column(field.name)
                # 1) Try direct cast to base type
                try:
                    table = table.set_column(idx, field.name, col.cast(field.type))
                except (
                    pyarrow.ArrowInvalid,
                    pyarrow.ArrowTypeError,
                    pyarrow.ArrowNotImplementedError,
                ):
                    # 2) Scalar fallback: cast to string (preserves data)
                    if not pyarrow.types.is_list(field.type):
                        try:
                            table = table.set_column(
                                idx, field.name, col.cast(pyarrow.string())
                            )
                            continue
                        except (
                            pyarrow.ArrowInvalid,
                            pyarrow.ArrowTypeError,
                            pyarrow.ArrowNotImplementedError,
                        ):
                            pass
                    # 3) Last resort: nulls of base type
                    table = table.set_column(
                        idx, field.name,
                        pyarrow.nulls(table.num_rows, field.type),
                    )"""

new = """            elif table.schema.field(field.name).type != field.type:
                idx = table.schema.get_field_index(field.name)
                col = table.column(field.name)
                handled = False
                # 1) Try direct cast to base type
                try:
                    table = table.set_column(idx, field.name, col.cast(field.type))
                    handled = True
                except (
                    pyarrow.ArrowInvalid,
                    pyarrow.ArrowTypeError,
                    pyarrow.ArrowNotImplementedError,
                ):
                    pass
                if handled:
                    continue
                # 2) Scalar fallback: cast to string (preserves data)
                if not pyarrow.types.is_list(field.type):
                    try:
                        table = table.set_column(
                            idx, field.name, col.cast(pyarrow.string())
                        )
                        handled = True
                    except (
                        pyarrow.ArrowInvalid,
                        pyarrow.ArrowTypeError,
                        pyarrow.ArrowNotImplementedError,
                    ):
                        pass
                if handled:
                    continue
                # 3) Last resort: nulls of base type (list/struct mismatches)
                table = table.set_column(
                    idx, field.name,
                    pyarrow.nulls(table.num_rows, field.type),
                )"""

c = c.replace(old, new)
with open(path, "w") as f:
    f.write(c)
print("Done")
